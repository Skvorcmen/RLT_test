"""
Модуль для работы с базой данных PostgreSQL.
"""
import logging
import os
from typing import Optional

import asyncpg
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class Database:
    """Класс для управления подключением к базе данных."""
    
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
    
    async def connect(self):
        """Создает пул подключений к базе данных."""
        database_url = os.getenv(
            'DATABASE_URL',
            'postgresql://postgres:postgres@localhost:5432/video_analytics'
        )
        
        logger.info("Подключение к базе данных...")
        self.pool = await asyncpg.create_pool(
            database_url,
            min_size=1,
            max_size=10,
            command_timeout=60
        )
        logger.info("Подключение к базе данных установлено")
    
    async def disconnect(self):
        """Закрывает пул подключений."""
        if self.pool:
            await self.pool.close()
            logger.info("Соединение с базой данных закрыто")
    
    async def execute_query(self, sql: str) -> Optional[float]:
        """
        Выполняет SQL запрос и возвращает первое число из результата.
        
        Args:
            sql: SQL запрос для выполнения
            
        Returns:
            Первое число из результата запроса или None
        """
        if not self.pool:
            raise RuntimeError("База данных не подключена. Вызовите connect() сначала.")
        
        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetch(sql)
                
                if not result:
                    return 0
                
                # Извлекаем первое значение из первой строки
                first_row = result[0]
                first_value = first_row[0]
                
                # Преобразуем в число
                if isinstance(first_value, (int, float)):
                    return float(first_value)
                elif first_value is None:
                    return 0
                else:
                    # Пробуем преобразовать строку в число
                    try:
                        return float(first_value)
                    except (ValueError, TypeError):
                        logger.warning(f"Не удалось преобразовать значение {first_value} в число")
                        return 0
        
        except Exception as e:
            logger.error(f"Ошибка при выполнении SQL запроса: {e}")
            raise

