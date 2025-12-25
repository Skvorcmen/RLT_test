"""
Обработчики сообщений для Telegram бота.
"""
import logging

from aiogram import Router, F
from aiogram.types import Message

from bot.llm_service import LLMService
from bot.query_builder import QueryBuilder

logger = logging.getLogger(__name__)

router = Router()


@router.message(F.text)
async def handle_text_message(message: Message, llm_service: LLMService, query_builder: QueryBuilder):
    """
    Обрабатывает текстовые сообщения от пользователя.
    Преобразует запрос на естественном языке в SQL и возвращает результат.
    """
    user_query = message.text.strip()
    
    if not user_query:
        await message.answer("Пожалуйста, отправьте запрос на русском языке.")
        return
    
    try:
        logger.info(f"Получен запрос от пользователя {message.from_user.id}: {user_query}")
        
        # Генерируем SQL из запроса пользователя
        sql = await llm_service.generate_sql(user_query)
        
        # Выполняем SQL запрос
        result = await query_builder.execute(sql)
        
        # Отправляем результат пользователю
        # Форматируем число: убираем десятичные знаки если они нулевые
        if result == int(result):
            answer = str(int(result))
        else:
            answer = str(result)
        
        await message.answer(answer)
        logger.info(f"Отправлен ответ пользователю {message.from_user.id}: {answer}")
    
    except ValueError as e:
        logger.error(f"Ошибка валидации: {e}")
        await message.answer(f"Ошибка: {str(e)}")
    
    except Exception as e:
        logger.error(f"Ошибка при обработке запроса: {e}", exc_info=True)
        await message.answer("Произошла ошибка при обработке вашего запроса. Попробуйте переформулировать вопрос.")

