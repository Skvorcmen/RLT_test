"""
Модуль для выполнения SQL запросов и извлечения результатов.
"""
import logging

from bot.database import Database

logger = logging.getLogger(__name__)


class QueryBuilder:
    """Класс для выполнения SQL запросов."""
    
    def __init__(self, database: Database):
        self.db = database
    
    async def execute(self, sql: str) -> float:
        """
        Выполняет SQL запрос и возвращает числовой результат.
        
        Args:
            sql: SQL запрос для выполнения
            
        Returns:
            Числовой результат запроса
        """
        result = await self.db.execute_query(sql)
        return result if result is not None else 0.0

