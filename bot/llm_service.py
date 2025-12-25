"""
Сервис для преобразования естественного языка в SQL запросы с использованием OpenAI API.
"""
import logging
import os
import re
from typing import Optional

from openai import AsyncOpenAI
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Промпт с описанием схемы БД и примерами
SCHEMA_PROMPT = """
Ты - эксперт по SQL и базе данных PostgreSQL. Твоя задача - преобразовать запрос на русском языке в SQL запрос.

## Схема базы данных:

### Таблица: videos
Итоговая статистика по каждому видео.

Поля:
- id (BIGINT, PRIMARY KEY) - идентификатор видео
- creator_id (BIGINT, NOT NULL) - идентификатор креатора
- video_created_at (TIMESTAMP, NOT NULL) - дата и время публикации видео
- views_count (BIGINT, NOT NULL, DEFAULT 0) - финальное количество просмотров
- likes_count (BIGINT, NOT NULL, DEFAULT 0) - финальное количество лайков
- comments_count (BIGINT, NOT NULL, DEFAULT 0) - финальное количество комментариев
- reports_count (BIGINT, NOT NULL, DEFAULT 0) - финальное количество жалоб
- created_at (TIMESTAMP, NOT NULL) - служебное поле
- updated_at (TIMESTAMP, NOT NULL) - служебное поле

### Таблица: video_snapshots
Почасовые замеры статистики по каждому видео.

Поля:
- id (BIGINT, PRIMARY KEY) - идентификатор снапшота
- video_id (BIGINT, NOT NULL, FOREIGN KEY -> videos.id) - ссылка на видео
- views_count (BIGINT, NOT NULL, DEFAULT 0) - текущее количество просмотров на момент замера
- likes_count (BIGINT, NOT NULL, DEFAULT 0) - текущее количество лайков на момент замера
- comments_count (BIGINT, NOT NULL, DEFAULT 0) - текущее количество комментариев на момент замера
- reports_count (BIGINT, NOT NULL, DEFAULT 0) - текущее количество жалоб на момент замера
- delta_views_count (BIGINT, NOT NULL, DEFAULT 0) - приращение просмотров с прошлого замера
- delta_likes_count (BIGINT, NOT NULL, DEFAULT 0) - приращение лайков с прошлого замера
- delta_comments_count (BIGINT, NOT NULL, DEFAULT 0) - приращение комментариев с прошлого замера
- delta_reports_count (BIGINT, NOT NULL, DEFAULT 0) - приращение жалоб с прошлого замера
- created_at (TIMESTAMP, NOT NULL) - время замера (раз в час)
- updated_at (TIMESTAMP, NOT NULL) - служебное поле

## Важные правила:

1. Даты в русском формате нужно преобразовывать в SQL формат:
   - "28 ноября 2025" -> DATE '2025-11-28'
   - "с 1 по 5 ноября 2025" -> BETWEEN DATE '2025-11-01' AND DATE '2025-11-05'
   - "1 ноября 2025" -> DATE '2025-11-01'
   
2. Месяца на русском:
   - январь = 01, февраль = 02, март = 03, апрель = 04, май = 05, июнь = 06
   - июль = 07, август = 08, сентябрь = 09, октябрь = 10, ноябрь = 11, декабрь = 12

3. Запрос должен возвращать ОДНО ЧИСЛО (результат агрегации: COUNT, SUM, и т.д.)

4. Используй только SELECT запросы. НЕ используй DROP, DELETE, UPDATE, INSERT, ALTER.

5. Если запрос про количество видео - используй COUNT(*) или COUNT(DISTINCT ...)
6. Если запрос про сумму просмотров/лайков и т.д. - используй SUM()
7. Если запрос про прирост - используй SUM(delta_...) из таблицы video_snapshots
8. Если запрос про конкретную дату - используй DATE() функцию для сравнения

## Примеры:

Запрос: "Сколько всего видео есть в системе?"
SQL: SELECT COUNT(*) FROM videos;

Запрос: "Сколько видео у креатора с id 123 вышло с 1 ноября 2025 по 5 ноября 2025 включительно?"
SQL: SELECT COUNT(*) FROM videos WHERE creator_id = 123 AND video_created_at BETWEEN DATE '2025-11-01' AND DATE '2025-11-05';

Запрос: "Сколько видео набрало больше 100000 просмотров за всё время?"
SQL: SELECT COUNT(*) FROM videos WHERE views_count > 100000;

Запрос: "На сколько просмотров в сумме выросли все видео 28 ноября 2025?"
SQL: SELECT COALESCE(SUM(delta_views_count), 0) FROM video_snapshots WHERE DATE(created_at) = DATE '2025-11-28';

Запрос: "Сколько разных видео получали новые просмотры 27 ноября 2025?"
SQL: SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = DATE '2025-11-27' AND delta_views_count > 0;

## Твоя задача:

Преобразуй следующий запрос на русском языке в SQL запрос. Верни ТОЛЬКО SQL запрос, без дополнительных объяснений.
"""


class LLMService:
    """Сервис для работы с LLM API."""
    
    def __init__(self):
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OPENAI_API_KEY не установлен в переменных окружения")
        
        self.client = AsyncOpenAI(api_key=api_key)
        self.model = os.getenv('OPENAI_MODEL', 'gpt-4o-mini')
    
    def _validate_sql(self, sql: str) -> bool:
        """
        Валидирует SQL запрос на безопасность.
        Разрешает только SELECT запросы.
        """
        sql_upper = sql.strip().upper()
        
        # Проверяем, что это SELECT запрос
        if not sql_upper.startswith('SELECT'):
            return False
        
        # Запрещаем опасные операции
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'TRUNCATE', 'CREATE', 'EXEC']
        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                return False
        
        return True
    
    def _extract_sql(self, response: str) -> Optional[str]:
        """
        Извлекает SQL запрос из ответа LLM.
        Удаляет markdown форматирование если есть.
        """
        # Убираем markdown code blocks
        sql = response.strip()
        
        # Убираем ```sql или ``` в начале и конце
        if sql.startswith('```'):
            lines = sql.split('\n')
            # Убираем первую строку с ```
            if lines[0].strip().startswith('```'):
                lines = lines[1:]
            # Убираем последнюю строку с ```
            if lines and lines[-1].strip() == '```':
                lines = lines[:-1]
            sql = '\n'.join(lines)
        
        sql = sql.strip()
        
        # Убираем точку с запятой в конце если есть
        if sql.endswith(';'):
            sql = sql[:-1]
        
        return sql
    
    async def generate_sql(self, user_query: str) -> str:
        """
        Генерирует SQL запрос из запроса на естественном языке.
        
        Args:
            user_query: Запрос пользователя на русском языке
            
        Returns:
            SQL запрос в виде строки
            
        Raises:
            ValueError: Если SQL запрос невалиден
        """
        try:
            logger.info(f"Генерация SQL для запроса: {user_query}")
            
            messages = [
                {"role": "system", "content": SCHEMA_PROMPT},
                {"role": "user", "content": user_query}
            ]
            
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=0.1,  # Низкая температура для более точных SQL запросов
                max_tokens=500
            )
            
            sql_response = response.choices[0].message.content
            sql = self._extract_sql(sql_response)
            
            if not sql:
                raise ValueError("Не удалось извлечь SQL из ответа LLM")
            
            # Валидация SQL
            if not self._validate_sql(sql):
                raise ValueError(f"Небезопасный SQL запрос: {sql}")
            
            logger.info(f"Сгенерированный SQL: {sql}")
            return sql
        
        except Exception as e:
            logger.error(f"Ошибка при генерации SQL: {e}")
            raise

