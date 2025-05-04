from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import ChatMemberUpdatedFilter, IS_NOT_MEMBER, IS_MEMBER
from aiogram.types import ChatMemberUpdated
from decouple import config
import logging
import asyncio
import json
from pathlib import Path
import asyncpg  # Используем asyncpg для работы с PostgreSQL асинхронно

# Логирование
logging.basicConfig(level=logging.INFO)

# Настройки подключения к PostgreSQL
DB_CONFIG = {
    'user': config('DB_USER', default='postgres'),
    'password': config('DB_PASSWORD'),
    'database': config('DB_NAME', default='tg_stats'),
    'host': config('DB_HOST', default='localhost'),
    'port': config('DB_PORT', default=5432),
}


async def create_db_pool():
    """Создаем пул подключений к PostgreSQL"""
    return await asyncpg.create_pool(**DB_CONFIG)


async def create_tables(conn):
    """Создаем таблицы в PostgreSQL"""
    # Создание таблицы users
    await conn.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id BIGINT PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        last_name TEXT
    )
    ''')

    # Создание таблицы messages
    await conn.execute('''
    CREATE TABLE IF NOT EXISTS messages (
        message_id BIGINT,
        user_id BIGINT REFERENCES users(user_id),
        chat_id BIGINT,
        text TEXT,
        date TIMESTAMP WITH TIME ZONE
    )
    ''')

    # Создание таблицы reposts
    await conn.execute('''
    CREATE TABLE IF NOT EXISTS reposts (
        message_id BIGINT,
        user_id BIGINT REFERENCES users(user_id),
        source_chat_id BIGINT,
        target_chat_id BIGINT,
        date TIMESTAMP WITH TIME ZONE
    )
    ''')

    # Создание таблицы joins_leaves
    await conn.execute('''
    CREATE TABLE IF NOT EXISTS joins_leaves (
        user_id BIGINT REFERENCES users(user_id),
        action TEXT,
        date TIMESTAMP WITH TIME ZONE
    )
    ''')

    # Создание таблицы reactions
    await conn.execute('''
    CREATE TABLE IF NOT EXISTS reactions (
        reaction_id BIGINT,
        user_id BIGINT REFERENCES users(user_id),
        username TEXT,
        chat_id BIGINT,
        reaction TEXT,
        text TEXT,
        date TIMESTAMP WITH TIME ZONE
    )
    ''')


async def on_startup(bot: Bot, pool):
    """
    Функция для удаления Webhook при запуске бота и инициализации БД.
    """
    try:
        webhook_info = await bot.get_webhook_info()
        if webhook_info.url:
            await bot.delete_webhook()
            logging.info("Webhook успешно удалён.")
        else:
            logging.info("Webhook не установлен.")

        # Инициализация таблиц
        async with pool.acquire() as conn:
            await create_tables(conn)
            logging.info("Таблицы в PostgreSQL инициализированы.")

    except Exception as e:
        logging.error(f"Ошибка при запуске: {e}")


async def main():
    API_TOKEN = config('API_TOKEN')
    pool = await create_db_pool()

    async with Bot(token=API_TOKEN) as bot:
        dp = Dispatcher()

        # Инициализация БД при запуске
        await on_startup(bot, pool)

        @dp.chat_member(ChatMemberUpdatedFilter(IS_NOT_MEMBER >> IS_MEMBER))
        async def on_user_join(event: ChatMemberUpdated):
            user = event.new_chat_member.user
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO users (user_id, username, first_name, last_name) "
                    "VALUES ($1, $2, $3, $4) ON CONFLICT (user_id) DO NOTHING",
                    user.id, user.username, user.first_name, user.last_name
                )
                await conn.execute(
                    "INSERT INTO joins_leaves (user_id, action, date) "
                    "VALUES ($1, 'join', NOW())",
                    user.id
                )
            await bot.send_message(event.chat.id, f"👋 {user.first_name} зашёл в чат!")

        @dp.chat_member(ChatMemberUpdatedFilter(IS_MEMBER >> IS_NOT_MEMBER))
        async def on_user_leave(event: ChatMemberUpdated):
            user = event.new_chat_member.user
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO joins_leaves (user_id, action, date) "
                    "VALUES ($1, 'leave', NOW())",
                    user.id
                )
            await bot.send_message(event.chat.id, f"👋 {user.first_name} покинул чат.")

        @dp.message(F.chat.type == "supergroup")
        async def count_messages(message: types.Message):
            message_data = message.model_dump()
            json_file = Path("2.json")
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(message_data, f, ensure_ascii=False, indent=4)

            user = message.from_user
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO users (user_id, username, first_name, last_name) "
                    "VALUES ($1, $2, $3, $4) ON CONFLICT (user_id) DO NOTHING",
                    user.id, user.username, user.first_name, user.last_name
                )
                await conn.execute(
                    "INSERT INTO messages (message_id, user_id, chat_id, text, date) "
                    "VALUES ($1, $2, $3, $4, NOW())",
                    message.message_id, user.id, message.chat.id, message.text
                )
                if message.forward_from_chat:
                    await conn.execute(
                        "INSERT INTO reposts (message_id, user_id, source_chat_id, target_chat_id, date) "
                        "VALUES ($1, $2, $3, $4, NOW())",
                        message.message_id, user.id, message.forward_from_chat.id, message.chat.id
                    )

        @dp.message_reaction()
        async def track_reactions(reaction: types.MessageReactionUpdated):
            user = reaction.user
            message_id = reaction.message_id

            async with pool.acquire() as conn:
                # Получаем текст сообщения
                message_text = await conn.fetchval(
                    "SELECT text FROM messages WHERE message_id = $1",
                    message_id
                ) or ""  # Если сообщение не найдено, используем пустую строку

                # Обрабатываем старую и новую реакции
                old_reaction = str(reaction.old_reaction[0].emoji) if reaction.old_reaction else None
                new_reaction = str(reaction.new_reaction[0].emoji) if reaction.new_reaction else None

                # Если удалена реакция
                if not reaction.new_reaction and old_reaction:
                    await conn.execute(
                        "DELETE FROM reactions WHERE reaction_id = $1 AND reaction = $2 AND user_id = $3",
                        message_id, old_reaction, user.id
                    )
                elif new_reaction:
                    # Проверяем существование реакции
                    reaction_exists = await conn.fetchval(
                        "SELECT COUNT(*) > 0 FROM reactions WHERE reaction_id = $1 AND user_id = $2",
                        message_id, user.id
                    ) if old_reaction else False

                    # Если у пользователя нет премиума и реакция существует
                    if reaction_exists and not user.is_premium:
                        await conn.execute(
                            "UPDATE reactions SET reaction = $1 WHERE reaction_id = $2 AND user_id = $3",
                            new_reaction, message_id, user.id
                        )
                    else:
                        await conn.execute(
                            "INSERT INTO users (user_id, username, first_name, last_name) "
                            "VALUES ($1, $2, $3, $4) ON CONFLICT (user_id) DO NOTHING",
                            user.id, user.username, user.first_name, user.last_name
                        )
                        await conn.execute(
                            "INSERT INTO reactions (reaction_id, user_id, username, chat_id, reaction, text, date) "
                            "VALUES ($1, $2, $3, $4, $5, $6, NOW())",
                            message_id, user.id, user.username, reaction.chat.id,
                            new_reaction, message_text
                        )

            logging.info(
                f"Реакция: {reaction.new_reaction} от пользователя {user.username} старая {reaction.old_reaction}")

        @dp.message(F.text == "/stats")
        async def show_stats(message: types.Message):
            user_id = message.from_user.id
            async with pool.acquire() as conn:
                # Сколько сообщений написал пользователь
                msg_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM messages WHERE user_id = $1",
                    user_id
                )

                # Сколько репостов сделал
                reposts_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM reposts WHERE user_id = $1",
                    user_id
                )

                # Сколько реакций сделал
                reactions_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM reactions WHERE user_id = $1",
                    user_id
                )

                # Сколько реакций всего
                total_reactions = await conn.fetchval(
                    "SELECT COUNT(*) FROM reactions"
                )

                # Сколько входов/выходов
                joins_leaves = await conn.fetchval(
                    "SELECT COUNT(*) FROM joins_leaves"
                )

                # Сколько пользователей
                users_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM users"
                )

                # Топовое сообщение
                top_message = await conn.fetchrow(
                    "SELECT text, COUNT(*) AS total_likes FROM reactions "
                    "GROUP BY text ORDER BY total_likes DESC LIMIT 1"
                )

                top_text = top_message['text'] if top_message else "Нет сообщений с реакциями"
                top_reactions = top_message['total_likes'] if top_message else "Нет сообщений с реакциями"

                await message.answer(
                    f"📊 Ваша активность:\n"
                    f"✉️ Сообщений: {msg_count}\n"
                    f"🔁 Репостов: {reposts_count}\n"
                    f"🔁 Реакций: {reactions_count}\n\n"
                    f"📊 Общая активность:\n"
                    f"🔁 Реакций всего: {total_reactions}\n"
                    f"🔁 Входов/выходов: {joins_leaves}\n"
                    f"🔁 Лучшее сообщение: {top_text}\n"
                    f"🔁 Кол-во реакций у него: {top_reactions}\n"
                    f"🔁 Пользователей: {users_count}"
                )

        try:
            await dp.start_polling(bot)
        except Exception as e:
            logging.error(f"Ошибка при запуске Polling: {e}")
        finally:
            await pool.close()
            logging.info("Соединение с базой данных закрыто.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Бот остановлен вручную.")
