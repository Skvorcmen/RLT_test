"""
Скрипт для загрузки данных из JSON файла в базу данных PostgreSQL.
Скачивает JSON файл с Google Drive и загружает данные в таблицы videos и video_snapshots.
"""
import asyncio
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import asyncpg
import gdown
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# URL для скачивания JSON файла с Google Drive
JSON_URL = "https://drive.google.com/uc?id=1BZOYxhDmMGJrSbPdcQgQjh0HRzN1YZt5"
JSON_FILE = "data.json"


async def download_json_file(url: str, output_file: str) -> str:
    """Скачивает JSON файл с Google Drive."""
    logger.info(f"Скачивание JSON файла из {url}...")
    output_path = Path(output_file)
    
    if output_path.exists():
        logger.info(f"Файл {output_file} уже существует, пропускаем скачивание")
        return str(output_path)
    
    gdown.download(url, output_file, quiet=False)
    logger.info(f"Файл успешно скачан: {output_file}")
    return str(output_path)


def parse_datetime(date_str: str) -> datetime:
    """Парсит строку даты в datetime объект."""
    try:
        # Пробуем разные форматы
        formats = [
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%fZ",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        # Если ничего не подошло, возвращаем текущее время
        logger.warning(f"Не удалось распарсить дату: {date_str}, используем текущее время")
        return datetime.now()
    except Exception as e:
        logger.error(f"Ошибка при парсинге даты {date_str}: {e}")
        return datetime.now()


async def load_data_to_db(conn: asyncpg.Connection, json_file: str):
    """Загружает данные из JSON файла в базу данных."""
    logger.info(f"Чтение JSON файла: {json_file}")
    
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if not isinstance(data, list):
        raise ValueError("JSON файл должен содержать массив объектов videos")
    
    logger.info(f"Найдено {len(data)} видео для загрузки")
    
    # Загружаем видео
    videos_inserted = 0
    snapshots_inserted = 0
    
    for video_data in data:
        try:
            # Вставляем видео
            video_id = await conn.fetchval(
                """
                INSERT INTO videos (
                    id, creator_id, video_created_at, views_count, 
                    likes_count, comments_count, reports_count, 
                    created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (id) DO UPDATE SET
                    views_count = EXCLUDED.views_count,
                    likes_count = EXCLUDED.likes_count,
                    comments_count = EXCLUDED.comments_count,
                    reports_count = EXCLUDED.reports_count,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id
                """,
                video_data['id'],
                video_data['creator_id'],
                parse_datetime(video_data['video_created_at']),
                video_data.get('views_count', 0),
                video_data.get('likes_count', 0),
                video_data.get('comments_count', 0),
                video_data.get('reports_count', 0),
                parse_datetime(video_data.get('created_at', video_data['video_created_at'])),
                parse_datetime(video_data.get('updated_at', video_data['video_created_at']))
            )
            
            videos_inserted += 1
            
            # Вставляем снапшоты
            snapshots = video_data.get('snapshots', [])
            if snapshots:
                snapshot_values = []
                for snapshot in snapshots:
                    snapshot_values.append((
                        snapshot.get('id'),
                        video_id,
                        snapshot.get('views_count', 0),
                        snapshot.get('likes_count', 0),
                        snapshot.get('comments_count', 0),
                        snapshot.get('reports_count', 0),
                        snapshot.get('delta_views_count', 0),
                        snapshot.get('delta_likes_count', 0),
                        snapshot.get('delta_comments_count', 0),
                        snapshot.get('delta_reports_count', 0),
                        parse_datetime(snapshot.get('created_at', video_data['video_created_at'])),
                        parse_datetime(snapshot.get('updated_at', snapshot.get('created_at', video_data['video_created_at'])))
                    ))
                
                # Batch insert для снапшотов
                await conn.executemany(
                    """
                    INSERT INTO video_snapshots (
                        id, video_id, views_count, likes_count, comments_count, reports_count,
                        delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count,
                        created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (id) DO UPDATE SET
                        views_count = EXCLUDED.views_count,
                        likes_count = EXCLUDED.likes_count,
                        comments_count = EXCLUDED.comments_count,
                        reports_count = EXCLUDED.reports_count,
                        delta_views_count = EXCLUDED.delta_views_count,
                        delta_likes_count = EXCLUDED.delta_likes_count,
                        delta_comments_count = EXCLUDED.delta_comments_count,
                        delta_reports_count = EXCLUDED.delta_reports_count,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    snapshot_values
                )
                snapshots_inserted += len(snapshots)
            
            if videos_inserted % 100 == 0:
                logger.info(f"Обработано {videos_inserted} видео, {snapshots_inserted} снапшотов...")
        
        except Exception as e:
            logger.error(f"Ошибка при загрузке видео {video_data.get('id', 'unknown')}: {e}")
            continue
    
    logger.info(f"Загрузка завершена: {videos_inserted} видео, {snapshots_inserted} снапшотов")


async def main():
    """Основная функция."""
    # Скачиваем JSON файл
    json_file = await asyncio.to_thread(download_json_file, JSON_URL, JSON_FILE)
    
    # Подключаемся к базе данных
    database_url = os.getenv(
        'DATABASE_URL',
        'postgresql://postgres:postgres@localhost:5432/video_analytics'
    )
    
    logger.info("Подключение к базе данных...")
    conn = await asyncpg.connect(database_url)
    
    try:
        await load_data_to_db(conn, json_file)
        logger.info("Данные успешно загружены в базу данных")
    finally:
        await conn.close()
        logger.info("Соединение с базой данных закрыто")


if __name__ == '__main__':
    asyncio.run(main())

