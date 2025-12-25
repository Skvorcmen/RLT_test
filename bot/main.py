"""
Точка входа для Telegram бота.
"""
import asyncio
import logging
import os
import sys

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv

from bot.database import Database
from bot.handlers import router
from bot.llm_service import LLMService
from bot.query_builder import QueryBuilder

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Основная функция для запуска бота."""
    # Проверяем наличие токена
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    if not bot_token:
        logger.error("TELEGRAM_BOT_TOKEN не установлен в переменных окружения")
        sys.exit(1)
    
    # Инициализируем компоненты
    bot = Bot(token=bot_token)
    dp = Dispatcher(storage=MemoryStorage())
    
    # Инициализируем сервисы
    database = Database()
    llm_service = LLMService()
    
    # Подключаемся к базе данных
    try:
        await database.connect()
    except Exception as e:
        logger.error(f"Ошибка при подключении к базе данных: {e}")
        sys.exit(1)
    
    query_builder = QueryBuilder(database)
    
    # Устанавливаем сервисы в обработчики
    from bot.handlers import set_services
    set_services(llm_service, query_builder)
    
    # Подключаем обработчики
    dp.include_router(router)
    
    try:
        logger.info("Бот запущен и готов к работе")
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except Exception as e:
        logger.error(f"Ошибка при работе бота: {e}", exc_info=True)
    finally:
        await database.disconnect()
        await bot.session.close()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")

