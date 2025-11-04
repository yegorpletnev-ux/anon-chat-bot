import asyncio
import aiosqlite
from aiogram import Bot, Dispatcher, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
import logging
from datetime import datetime
import os

# --- НАСТРОЙКА ---
BOT_TOKEN = os.environ.get('BOT_TOKEN')  # Берем токен из переменных окружения
ADMIN_ID = 6302652536  # Ваш ID для админки
ADMIN_PASS = "1234"
DB_PATH = "anon_chat.db"

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- СОСТОЯНИЯ ---
waiting_users = []  # [(user_id, gender, age, filters)]
active_chats = {}  # user_id -> partner_id
user_gender = {}  # user_id -> "M"/"F"
user_age = {}  # user_id -> возраст
user_state = {}  # user_id -> "choosing_gender"/"choosing_age"/"idle"/"in_chat"/"admin_pass"/"rating"/"setting_filters"
awaiting_rating = {}  # user_id -> partner_id (кого нужно оценить)
user_filters = {}  # user_id -> {"min_rating": 0, "max_age": 100, "min_age": 14}
chat_start_time = {}  # (user1, user2) -> start_time

# --- КЛАВИАТУРЫ ---
gender_kb = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="Мужской"), KeyboardButton(text="Женский")]],
    resize_keyboard=True
)

menu_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🔎 Найти собеседника"), KeyboardButton(text="⚙️ Фильтры")],
        [KeyboardButton(text="🛠 Панель"), KeyboardButton(text="📊 Моя статистика")],
        [KeyboardButton(text="⛔ Выйти из поиска")]
    ],
    resize_keyboard=True
)

chat_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="⏭️ Скипнуть"), KeyboardButton(text="❌ Завершить чат")]
    ],
    resize_keyboard=True
)

rating_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="⭐ 1"), KeyboardButton(text="⭐⭐ 2"), KeyboardButton(text="⭐⭐⭐ 3")],
        [KeyboardButton(text="⭐⭐⭐⭐ 4"), KeyboardButton(text="⭐⭐⭐⭐⭐ 5"), KeyboardButton(text="🚫 Пропустить")]
    ],
    resize_keyboard=True
)

filters_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Минимальный рейтинг"), KeyboardButton(text="🎂 Возрастной диапазон")],
        [KeyboardButton(text="❌ Сбросить фильтры"), KeyboardButton(text="📋 Текущие настройки")],
        [KeyboardButton(text="🔙 Назад")]
    ],
    resize_keyboard=True
)

# --- БАЗА ДАННЫХ ---
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            banned INTEGER DEFAULT 0,
            gender TEXT,
            age INTEGER DEFAULT 0,
            rating REAL DEFAULT 0.0,
            rating_count INTEGER DEFAULT 0,
            interests TEXT,
            filters TEXT,
            created_at TEXT
        )""")
        
        await db.execute("""CREATE TABLE IF NOT EXISTS admin_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id INTEGER,
            target_user INTEGER,
            action TEXT,
            ts TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        
        await db.execute("""CREATE TABLE IF NOT EXISTS ratings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_user INTEGER,
            to_user INTEGER,
            rating INTEGER,
            ts TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        
        await db.execute("""CREATE TABLE IF NOT EXISTS chats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user1 INTEGER,
            user2 INTEGER,
            start_time TEXT,
            end_time TEXT,
            duration INTEGER DEFAULT 0
        )""")
        await db.commit()

async def get_user_stats():
    """Получить статистику пользователей"""
    async with aiosqlite.connect(DB_PATH) as db:
        # Всего пользователей
        cur = await db.execute("SELECT COUNT(*) FROM users")
        total_users = (await cur.fetchone())[0]
        
        # Активных пользователей (не забаненных)
        cur = await db.execute("SELECT COUNT(*) FROM users WHERE banned = 0")
        active_users = (await cur.fetchone())[0]
        
        # Забаненных пользователей
        cur = await db.execute("SELECT COUNT(*) FROM users WHERE banned = 1")
        banned_users = (await cur.fetchone())[0]
        
        # Пользователей онлайн (в активных чатах)
        online_users = len(active_chats) * 2
        
        return {
            "total_users": total_users,
            "active_users": active_users,
            "banned_users": banned_users,
            "online_users": online_users
        }

async def ban_user(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO users(user_id, banned) VALUES(?,1)", (user_id,))
        await db.commit()

async def unban_user(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO users(user_id, banned) VALUES(?,0)", (user_id,))
        await db.commit()

async def is_banned(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT banned FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return row and row[0] == 1

async def save_user_data(user_id, gender, age):
    async with aiosqlite.connect(DB_PATH) as db:
        current_time = datetime.now().isoformat()
        await db.execute(
            "INSERT OR REPLACE INTO users(user_id, gender, age, created_at) VALUES(?,?,?,?)", 
            (user_id, gender, age, current_time)
        )
        await db.commit()

async def update_rating(user_id, rating):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT rating, rating_count FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        
        if row and row[0] is not None:
            current_rating, count = row[0] or 0, row[1] or 0
            new_rating = (current_rating * count + rating) / (count + 1)
            await db.execute(
                "UPDATE users SET rating=?, rating_count=? WHERE user_id=?", 
                (new_rating, count + 1, user_id)
            )
        else:
            await db.execute(
                "INSERT OR REPLACE INTO users(user_id, rating, rating_count, created_at) VALUES(?,?,?,?)", 
                (user_id, rating, 1, datetime.now().isoformat())
            )
        
        from_user = None
        for uid, partner_id in awaiting_rating.items():
            if partner_id == user_id:
                from_user = uid
                break
        
        if from_user:
            await db.execute(
                "INSERT INTO ratings(from_user, to_user, rating) VALUES(?,?,?)",
                (from_user, user_id, rating)
            )
        await db.commit()

async def get_user_rating(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT rating, rating_count FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        if row and row[0] is not None:
            return round(row[0], 1), row[1] or 0
        return 0, 0

async def save_user_filters(user_id, filters):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO users(user_id, filters) VALUES(?,?)",
            (user_id, str(filters))
        )
        await db.commit()

async def get_user_filters(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT filters FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        if row and row[0]:
            try:
                return eval(row[0])
            except:
                return {"min_rating": 0, "min_age": 14, "max_age": 100}
        return {"min_rating": 0, "min_age": 14, "max_age": 100}

async def log_chat_start(user1, user2):
    start_time = datetime.now().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO chats(user1, user2, start_time) VALUES(?,?,?)",
            (user1, user2, start_time)
        )
        await db.commit()
    chat_start_time[(user1, user2)] = start_time

async def log_chat_end(user1, user2):
    end_time = datetime.now()
    start_time = chat_start_time.get((user1, user2))
    if not start_time:
        return
    
    duration = (end_time - datetime.fromisoformat(start_time)).seconds
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE chats SET end_time=?, duration=? WHERE user1=? AND user2=? AND end_time IS NULL",
            (end_time.isoformat(), duration, user1, user2)
        )
        await db.commit()
    
    if (user1, user2) in chat_start_time:
        del chat_start_time[(user1, user2)]

# --- СТАТИСТИКА ДЛЯ АДМИНА ---
async def get_admin_stats():
    async with aiosqlite.connect(DB_PATH) as db:
        # Общее количество диалогов
        cur = await db.execute("SELECT COUNT(*) FROM chats")
        total_chats = (await cur.fetchone())[0]
        
        # Средняя продолжительность чата
        cur = await db.execute("SELECT AVG(duration) FROM chats WHERE duration > 0")
        avg_duration = (await cur.fetchone())[0] or 0
        
        # Популярное время активности (по часам)
        cur = await db.execute("""
            SELECT strftime('%H', start_time) as hour, COUNT(*) as count 
            FROM chats 
            GROUP BY hour 
            ORDER BY count DESC 
            LIMIT 3
        """)
        popular_hours = await cur.fetchall()
        
        # Статистика пользователей
        user_stats = await get_user_stats()
        
        return {
            "total_chats": total_chats,
            "avg_duration": round(avg_duration / 60, 1),
            "popular_hours": popular_hours,
            "user_stats": user_stats
        }

# --- ПОМОЩНИКИ ---
async def find_pair(user_id):
    if await is_banned(user_id):
        await bot.send_message(user_id, "⛔ Вы заблокированы админом.")
        return

    gender = user_gender.get(user_id)
    age = user_age.get(user_id)
    
    if not gender:
        await bot.send_message(user_id, "❌ Сначала завершите регистрацию.")
        return

    # УБИРАЕМ ОГРАНИЧЕНИЕ ПО ПОЛУ - можно подключаться к любому полу
    user_filters_data = user_filters.get(user_id, {"min_rating": 0, "min_age": 14, "max_age": 100})

    print(f"🔍 Поиск пары для {user_id} ({gender}, {age} лет)")
    print(f"📋 Очередь ожидания: {[(uid, g, a) for uid, g, a, f in waiting_users]}")

    for i, (uid, ugender, uage, ufilters) in enumerate(waiting_users):
        if uid != user_id:  # Убираем проверку на противоположный пол
            print(f"🔎 Проверяем пользователя {uid} ({ugender}, {uage} лет)")
            
            partner_rating, partner_count = await get_user_rating(uid)
            user_rating, user_count = await get_user_rating(user_id)
            
            if user_rating >= 4.0 and partner_rating < 3.5:
                print(f"❌ Не подходит по рейтингу: {partner_rating} < 3.5")
                continue
                
            if partner_rating < user_filters_data.get("min_rating", 0):
                print(f"❌ Не подходит по минимальному рейтингу: {partner_rating} < {user_filters_data.get('min_rating', 0)}")
                continue
                
            min_age = user_filters_data.get("min_age", 14)
            max_age = user_filters_data.get("max_age", 100)
            if not (min_age <= uage <= max_age):
                print(f"❌ Не подходит по возрасту: {uage} не в диапазоне {min_age}-{max_age}")
                continue

            partner_id = uid
            waiting_users.pop(i)
            active_chats[user_id] = partner_id
            active_chats[partner_id] = user_id
            user_state[user_id] = "in_chat"
            user_state[partner_id] = "in_chat"
            
            await log_chat_start(user_id, partner_id)
            
            rating, count = await get_user_rating(partner_id)
            rating_text = f" (Рейтинг: {rating}⭐)" if count > 0 else ""
            age_text = f", возраст: {uage} лет"
            gender_text = f", пол: {'Мужской' if ugender == 'M' else 'Женский'}"
            
            print(f"✅ Соединили {user_id} с {partner_id}")
            await bot.send_message(user_id, f"✅ Собеседник найден!{rating_text}{age_text}{gender_text}", reply_markup=chat_kb)
            await bot.send_message(partner_id, f"✅ Собеседник найден!{rating_text}{age_text}{gender_text}", reply_markup=chat_kb)
            return

    user_data = (user_id, gender, age, user_filters_data)
    if user_data not in waiting_users:
        waiting_users.append(user_data)
        print(f"➕ Добавили {user_id} в очередь ожидания. Теперь в очереди: {len(waiting_users)}")
    
    user_state[user_id] = "idle"
    await bot.send_message(user_id, "⏳ Ожидание собеседника...", reply_markup=menu_kb)

async def end_chat(user_id, notify=True):
    partner_id = active_chats.get(user_id)
    if partner_id:
        await log_chat_end(user_id, partner_id)
        
        del active_chats[user_id]
        del active_chats[partner_id]
        user_state[user_id] = "rating"
        user_state[partner_id] = "rating"
        
        awaiting_rating[user_id] = partner_id
        awaiting_rating[partner_id] = user_id
        
        if notify:
            await bot.send_message(partner_id, "❌ Собеседник покинул чат. Оцените диалог:", reply_markup=rating_kb)
        await bot.send_message(user_id, "❌ Чат завершен. Оцените диалог:", reply_markup=rating_kb)
    else:
        await bot.send_message(user_id, "❌ Чат завершен.", reply_markup=menu_kb)

# --- КОМАНДЫ ---
@dp.message(Command("start"))
async def cmd_start(msg: types.Message):
    uid = msg.from_user.id
    user_state[uid] = "choosing_gender"
    await msg.answer("👋 Добро пожаловать! Выберите свой пол:", reply_markup=gender_kb)

@dp.message(Command("rating"))
async def cmd_rating(msg: types.Message):
    uid = msg.from_user.id
    rating, count = await get_user_rating(uid)
    if count > 0:
        await msg.answer(f"📊 Ваш рейтинг: {rating}⭐ из {count} оценок")
    else:
        await msg.answer("📊 У вас пока нет оценок")

@dp.message(Command("stats"))
async def cmd_stats(msg: types.Message):
    uid = msg.from_user.id
    if uid != ADMIN_ID:
        await msg.answer("⛔ Нет доступа")
        return
    
    stats = await get_admin_stats()
    user_stats = stats["user_stats"]
    
    popular_hours_text = ""
    for hour, count in stats["popular_hours"]:
        popular_hours_text += f"{hour}:00 - {count} чатов\n"
    
    text = f"""
📊 **Статистика бота:**

👥 **Пользователи:**
• Всего: {user_stats['total_users']}
• Активных: {user_stats['active_users']}
• Онлайн: {user_stats['online_users']}
• Забанено: {user_stats['banned_users']}

💬 **Диалоги:**
• Всего: {stats['total_chats']}
• Средняя продолжительность: {stats['avg_duration']} мин.

🕐 **Популярное время:**
{popular_hours_text}
    """
    await msg.answer(text)

# --- ГЛАВНЫЙ ОБРАБОТЧИК ДЛЯ ВСЕХ СООБЩЕНИЙ ---
@dp.message()
async def handle_all_messages(msg: types.Message):
    uid = msg.from_user.id
    
    # Если пользователь в чате - обрабатываем все типы сообщений
    if user_state.get(uid) == "in_chat":
        await handle_chat_message(msg)
        return
    
    # Для всех остальных состояний - только текстовые сообщения
    if not msg.text:
        await msg.answer("❌ Используйте текстовые сообщения для навигации по меню.")
        return
    
    await handle_text_message(msg)

async def handle_chat_message(msg: types.Message):
    """Обработка всех типов сообщений в чате"""
    uid = msg.from_user.id
    partner_id = active_chats.get(uid)
    
    if not partner_id:
        await msg.answer("❌ Чат не активен.")
        return
    
    # Обработка кнопок чата
    if msg.text:
        text = msg.text.strip()
        if text == "⏭️ Скипнуть":
            await end_chat(uid, notify=False)
            await find_pair(uid)
            return
        elif text == "❌ Завершить чат":
            await end_chat(uid)
            return
    
    # Пересылка сообщений партнеру
    try:
        if msg.text:
            # Текстовое сообщение
            await bot.send_message(partner_id, msg.text)
            print(f"💬 Текст отправлен от {uid} к {partner_id}: {msg.text}")
            
        elif msg.photo:
            # Фото
            await bot.send_photo(partner_id, msg.photo[-1].file_id, caption=msg.caption)
            print(f"📷 Фото отправлено от {uid} к {partner_id}")
            
        elif msg.video:
            # Видео
            await bot.send_video(partner_id, msg.video.file_id, caption=msg.caption)
            print(f"🎥 Видео отправлено от {uid} к {partner_id}")
            
        elif msg.sticker:
            # Стикер
            await bot.send_sticker(partner_id, msg.sticker.file_id)
            print(f"😊 Стикер отправлен от {uid} к {partner_id}")
            
        elif msg.voice:
            # Голосовое сообщение
            await bot.send_voice(partner_id, msg.voice.file_id)
            print(f"🎤 Голосовое отправлено от {uid} к {partner_id}")
            
        elif msg.document:
            # Документ
            await bot.send_document(partner_id, msg.document.file_id, caption=msg.caption)
            print(f"📄 Документ отправлен от {uid} к {partner_id}")
            
        elif msg.audio:
            # Аудио
            await bot.send_audio(partner_id, msg.audio.file_id, caption=msg.caption)
            print(f"🎵 Аудио отправлено от {uid} к {partner_id}")
            
    except Exception as e:
        print(f"❌ Ошибка отправки сообщения от {uid} к {partner_id}: {e}")
        await msg.answer("❌ Не удалось отправить сообщение.")

async def handle_text_message(msg: types.Message):
    """Обработка текстовых сообщений для меню и настроек"""
    uid = msg.from_user.id
    text = msg.text.strip()

    # Выбор пола
    if user_state.get(uid) == "choosing_gender":
        if text not in ["Мужской", "Женский"]:
            await msg.answer("Выберите пол кнопкой.")
            return
        gender = "M" if text == "Мужской" else "F"
        user_gender[uid] = gender
        user_state[uid] = "choosing_age"
        await msg.answer(f"✅ Пол: {text}\n\nВведите возраст (14-100 лет):")
        return

    # Ввод возраста
    if user_state.get(uid) == "choosing_age":
        try:
            age = int(text)
            if 14 <= age <= 100:
                user_age[uid] = age
                await save_user_data(uid, user_gender[uid], age)
                user_state[uid] = "idle"
                user_filters[uid] = {"min_rating": 0, "min_age": 14, "max_age": 100}
                await save_user_filters(uid, user_filters[uid])
                await msg.answer(
                    f"✅ Регистрация завершена!\n"
                    f"Пол: {'Мужской' if user_gender[uid] == 'M' else 'Женский'}\n"
                    f"Возраст: {age} лет\n\n"
                    f"Теперь вы можете найти собеседника!", 
                    reply_markup=menu_kb
                )
            else:
                await msg.answer("❌ Возраст должен быть от 14 до 100 лет.")
        except ValueError:
            await msg.answer("❌ Введите число от 14 до 100:")
        return

    # Оценка собеседника
    if user_state.get(uid) == "rating":
        partner_id = awaiting_rating.get(uid)
        if text in ["⭐ 1", "⭐⭐ 2", "⭐⭐⭐ 3", "⭐⭐⭐⭐ 4", "⭐⭐⭐⭐⭐ 5"]:
            rating = len(text.split("⭐")[0])
            if partner_id:
                await update_rating(partner_id, rating)
                await msg.answer(f"✅ Вы поставили оценку {rating}⭐", reply_markup=menu_kb)
            else:
                await msg.answer("❌ Не удалось найти собеседника", reply_markup=menu_kb)
        elif text != "🚫 Пропустить":
            await msg.answer("Спасибо за диалог!", reply_markup=menu_kb)
        else:
            await msg.answer("Диалог завершен", reply_markup=menu_kb)
        
        user_state[uid] = "idle"
        if uid in awaiting_rating:
            del awaiting_rating[uid]
        return

    # Настройка фильтров
    if user_state.get(uid) == "setting_filters":
        if text == "📊 Минимальный рейтинг":
            await msg.answer("Введите минимальный рейтинг (0-5):")
            user_state[uid] = "setting_min_rating"
        elif text == "🎂 Возрастной диапазон":
            await msg.answer("Введите возрастной диапазон в формате 'мин-макс' (например: 14-25):")
            user_state[uid] = "setting_age_range"
        elif text == "❌ Сбросить фильтры":
            user_filters[uid] = {"min_rating": 0, "min_age": 14, "max_age": 100}
            await save_user_filters(uid, user_filters[uid])
            await msg.answer("✅ Фильтры сброшены", reply_markup=menu_kb)
            user_state[uid] = "idle"
        elif text == "📋 Текущие настройки":
            filters = user_filters.get(uid, {"min_rating": 0, "min_age": 14, "max_age": 100})
            await msg.answer(
                f"📋 Ваши фильтры:\n"
                f"⭐ Минимальный рейтинг: {filters.get('min_rating', 0)}\n"
                f"🎂 Возраст: {filters.get('min_age', 14)}-{filters.get('max_age', 100)} лет",
                reply_markup=filters_kb
            )
        elif text == "🔙 Назад":
            user_state[uid] = "idle"
            await msg.answer("Главное меню", reply_markup=menu_kb)
        return

    # Установка минимального рейтинга
    if user_state.get(uid) == "setting_min_rating":
        try:
            min_rating = float(text)
            if 0 <= min_rating <= 5:
                if uid not in user_filters:
                    user_filters[uid] = {}
                user_filters[uid]["min_rating"] = min_rating
                await save_user_filters(uid, user_filters[uid])
                await msg.answer(f"✅ Минимальный рейтинг установлен: {min_rating}", reply_markup=filters_kb)
                user_state[uid] = "setting_filters"
            else:
                await msg.answer("❌ Введите число от 0 до 5")
        except:
            await msg.answer("❌ Введите корректное число")

    # Установка возрастного диапазона
    if user_state.get(uid) == "setting_age_range":
        try:
            min_age, max_age = map(int, text.split('-'))
            if 14 <= min_age <= max_age <= 100:
                if uid not in user_filters:
                    user_filters[uid] = {}
                user_filters[uid]["min_age"] = min_age
                user_filters[uid]["max_age"] = max_age
                await save_user_filters(uid, user_filters[uid])
                await msg.answer(f"✅ Возрастной диапазон установлен: {min_age}-{max_age} лет", reply_markup=filters_kb)
                user_state[uid] = "setting_filters"
            else:
                await msg.answer("❌ Введите диапазон от 14 до 100 лет (мин-макс)")
        except:
            await msg.answer("❌ Введите в формате 'мин-макс' (например: 14-25)")

    # Панель админа - ВВОД ПАРОЛЯ
    if user_state.get(uid) == "admin_pass":
        if text == ADMIN_PASS:
            user_state[uid] = "idle"
            
            # Получаем список пользователей
            async with aiosqlite.connect(DB_PATH) as db:
                cur = await db.execute("SELECT user_id FROM users WHERE user_id != ?", (ADMIN_ID,))
                users = await cur.fetchall()
            
            # Создаем кнопки
            buttons = []
            for user in users:
                user_id = user[0]
                rating, count = await get_user_rating(user_id)
                rating_text = f" ({rating}⭐)" if count > 0 else ""
                
                buttons.append([
                    InlineKeyboardButton(text=f"⛔ Ban {user_id}", callback_data=f"ban_{user_id}"),
                    InlineKeyboardButton(text=f"✅ Unban {user_id}", callback_data=f"unban_{user_id}")
                ])
                buttons.append([
                    InlineKeyboardButton(text=f"❌ EndChat {user_id}", callback_data=f"end_chat_{user_id}")
                ])
            
            # Добавляем кнопку статистики
            buttons.append([InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")])
            
            kb = InlineKeyboardMarkup(inline_keyboard=buttons)
            await msg.answer("✅ Панель администратора:", reply_markup=kb)
        else:
            await msg.answer("❌ Неверный пароль")
        return

    # Проверка бана
    if await is_banned(uid):
        await msg.answer("⛔ Вы заблокированы админом.")
        return

    # Основное меню
    if text == "🔎 Найти собеседника":
        await find_pair(uid)
    elif text == "⚙️ Фильтры":
        user_state[uid] = "setting_filters"
        await msg.answer("Настройте фильтры поиска:", reply_markup=filters_kb)
    elif text == "📊 Моя статистика":
        rating, count = await get_user_rating(uid)
        age = user_age.get(uid, "не указан")
        gender = user_gender.get(uid, "не указан")
        gender_text = "Мужской" if gender == "M" else "Женский" if gender == "F" else "не указан"
        await msg.answer(f"📊 Ваша статистика:\nРейтинг: {rating}⭐ из {count} оценок\nВозраст: {age} лет\nПол: {gender_text}")
    elif text == "⛔ Выйти из поиска":
        waiting_users[:] = [x for x in waiting_users if x[0] != uid]
        user_state[uid] = "idle"
        await msg.answer("✅ Вы вышли из поиска.", reply_markup=menu_kb)
    elif text == "🛠 Панель":
        if uid == ADMIN_ID:
            user_state[uid] = "admin_pass"
            await msg.answer("Введите пароль для панели администратора:")
        else:
            await msg.answer("⛔ У вас нет доступа к панели администратора.")
    else:
        await msg.answer("Неизвестная команда. Используйте кнопки.")

# --- ПАНЕЛЬ АДМИНА ---
@dp.callback_query()
async def admin_callback(callback: types.CallbackQuery):
    admin_id = callback.from_user.id
    data = callback.data
    
    if data == "admin_stats":
        stats = await get_admin_stats()
        user_stats = stats["user_stats"]
        
        popular_hours_text = ""
        for hour, count in stats["popular_hours"]:
            popular_hours_text += f"{hour}:00 - {count} чатов\n"
        
        text = f"""
📊 **Статистика бота:**

👥 **Пользователи:**
• Всего: {user_stats['total_users']}
• Активных: {user_stats['active_users']}
• Онлайн: {user_stats['online_users']}
• Забанено: {user_stats['banned_users']}

💬 **Диалоги:**
• Всего: {stats['total_chats']}
• Средняя продолжительность: {stats['avg_duration']} мин.

🕐 **Популярное время:**
{popular_hours_text}
        """
        await callback.message.answer(text)
        return

    if data.startswith(("ban_", "unban_", "end_chat_")):
        action, target_id = data.split("_", 1)
        target_id = int(target_id)

        if action == "ban":
            await ban_user(target_id)
            await end_chat(target_id)
            await callback.message.answer(f"⛔ Пользователь {target_id} заблокирован.")
        elif action == "unban":
            await unban_user(target_id)
            await callback.message.answer(f"✅ Пользователь {target_id} разблокирован.")
        elif action == "end_chat":
            await end_chat(target_id)
            await callback.message.answer(f"✅ Чат пользователя {target_id} завершён.")
        
        await callback.answer()

# --- ЗАПУСК ---
async def main():
    # Проверяем наличие токена
    if not BOT_TOKEN:
        print("❌ Ошибка: BOT_TOKEN не установлен!")
        print("💡 Установите переменную окружения BOT_TOKEN на Railway")
        return
    
    await init_db()
    print("✅ Бот запущен...")
    print(f"🤖 Токен бота: {'установлен' if BOT_TOKEN else 'отсутствует'}")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())