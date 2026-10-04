import asyncio
import io
import logging
import os
import re
from datetime import datetime, timedelta, timezone

import aiohttp
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    BufferedInputFile,
)
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError

try:
    from zoneinfo import ZoneInfo
    KYIV = ZoneInfo("Europe/Kyiv")
except Exception:  # якщо немає tzdata
    KYIV = timezone(timedelta(hours=3))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("lurelle_bot")

BOT_TOKEN = os.environ["BOT_TOKEN"]
BOT_PASSWORD = os.environ["BOT_PASSWORD"]
MONGO_URI = os.environ["MONGO_URI"]
API_BASE_URL = os.environ["API_BASE_URL"].rstrip("/")  # https://lurelle-server.onrender.com

ONLINE_ALERT_THRESHOLD = int(os.environ.get("ONLINE_ALERT_THRESHOLD", "50"))
LOW_STOCK_THRESHOLD = int(os.environ.get("LOW_STOCK_THRESHOLD", "3"))

DB_ERROR_TEXT = "⚠️ Тимчасова проблема з базою даних. Спробуйте ще раз через кілька секунд."
API_ERROR_TEXT = "⚠️ Сайт зараз не відповідає, дані по відвідувачах недоступні."

STATUS_LABELS = {
    "new": "новий",
    "proc": "в обробці",
    "sent": "відправлено",
    "transit": "в дорозі",
    "done": "успішно",
    "refused": "відмовлено",
}
STATUS_EMOJI = {
    "новий": "🆕",
    "в обробці": "⚙️",
    "відправлено": "📦",
    "в дорозі": "🚚",
    "успішно": "✅",
    "відмовлено": "❌",
}
STATUS_COLOR = {
    "новий": "⚪",
    "в обробці": "🟡",
    "відправлено": "🟠",
    "в дорозі": "🟣",
    "успішно": "🟢",
    "відмовлено": "🔴",
}
NOT_SHIPPED_STATUSES = ["новий", "в обробці"]

mongo_client: AsyncIOMotorClient | None = None
db = None
orders_col = None
products_col = None
visitors_col = None
auth_col = None
notes_col = None
online_history_col = None

authorized_uids: set[int] = set()


# ---------------------------------------------------------------------------
# Час. У базі дати зберігаються в UTC, а "сьогодні" рахуємо за київським часом
# ---------------------------------------------------------------------------
def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def day_start_utc() -> datetime:
    now = datetime.now(KYIV)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return start.astimezone(timezone.utc).replace(tzinfo=None)


def period_start(period: str) -> datetime | None:
    if period == "day":
        return day_start_utc()
    if period == "week":
        return utcnow() - timedelta(days=7)
    if period == "month":
        return utcnow() - timedelta(days=30)
    return None


# ---------------------------------------------------------------------------
# Mongo
# ---------------------------------------------------------------------------
def init_mongo():
    global mongo_client, db, orders_col, products_col, visitors_col, auth_col, notes_col, online_history_col
    mongo_client = AsyncIOMotorClient(
        MONGO_URI,
        serverSelectionTimeoutMS=8000,
        connectTimeoutMS=8000,
        socketTimeoutMS=15000,
        maxPoolSize=20,
        retryWrites=True,
    )
    # Назва бази береться з MONGO_URI (.../назва_бази?...) — та сама, що й на сервері сайту
    db = mongo_client.get_default_database()
    orders_col = db["orders"]
    products_col = db["products"]
    visitors_col = db["visitors"]
    auth_col = db["bot_auth"]
    notes_col = db["customer_notes"]
    online_history_col = db["online_history"]


class DBUnavailable(Exception):
    pass


async def db_call(coro, default=None, retries=2, raise_on_fail=True):
    last_exc = None
    for attempt in range(retries + 1):
        try:
            return await coro
        except PyMongoError as e:
            last_exc = e
            if attempt < retries:
                await asyncio.sleep(0.5)
                continue
    logger.exception("MongoDB error: %s", last_exc)
    if raise_on_fail:
        raise DBUnavailable(str(last_exc)) from last_exc
    return default


async def is_authorized(uid: int) -> bool:
    if uid in authorized_uids:
        return True
    doc = await db_call(auth_col.find_one({"uid": uid}))
    if doc is not None:
        authorized_uids.add(uid)
        return True
    return False


async def authorize(uid: int):
    authorized_uids.add(uid)
    await db_call(auth_col.update_one({"uid": uid}, {"$set": {"uid": uid}}, upsert=True))


async def load_authorized_uids():
    cursor = auth_col.find({}, {"_id": 0, "uid": 1})
    docs = await db_call(cursor.to_list(length=None), default=[], raise_on_fail=False) or []
    for d in docs:
        if "uid" in d:
            authorized_uids.add(d["uid"])
    logger.info("Loaded %d authorized users into cache", len(authorized_uids))


async def get_all_uids() -> list:
    if authorized_uids:
        return list(authorized_uids)
    cursor = auth_col.find({}, {"_id": 0, "uid": 1})
    docs = await db_call(cursor.to_list(length=None), default=[], raise_on_fail=False) or []
    return [d["uid"] for d in docs if "uid" in d]


# ---------------------------------------------------------------------------
# Форматування
# ---------------------------------------------------------------------------
def esc(s) -> str:
    """Екранує спецсимволи Markdown у текстах, які вводять клієнти."""
    return re.sub(r"([_*`\[])", r"\\\1", str(s if s is not None else ""))


def order_profit(order: dict) -> float:
    items = order.get("items") or []
    revenue = sum((i.get("price") or 0) * (i.get("quantity") or 1) for i in items)
    cost = sum((i.get("costPrice") or 0) * (i.get("quantity") or 1) for i in items)
    delivery = order.get("deliveryCost") or 0
    return revenue - cost - delivery


def order_revenue(order: dict) -> float:
    return order.get("total") or 0


def fmt_money(v: float) -> str:
    return f"{v:,.0f}".replace(",", " ")


def fmt_order_items(order: dict) -> str:
    lines = []
    for i in order.get("items") or []:
        color = f" ({esc(i.get('color'))})" if i.get("color") else ""
        size = f", розмір: {esc(i.get('size'))}" if i.get("size") else ""
        lines.append(
            f"👜 {esc(i.get('name', ''))}{color}{size}\n"
            f"     {i.get('quantity', 1)} шт. × {fmt_money(i.get('price', 0))} грн"
        )
    return "\n".join(lines) if lines else "—"


async def get_repeat_count(phone: str) -> int:
    if not phone:
        return 0
    return await db_call(orders_col.count_documents({"customer.phone": phone}), default=0, raise_on_fail=False) or 0


async def get_customer_note(phone: str) -> str:
    if not phone:
        return ""
    doc = await db_call(notes_col.find_one({"_id": phone}), default=None, raise_on_fail=False)
    return doc.get("text", "") if doc else ""


async def fmt_order_card(order: dict) -> str:
    customer = order.get("customer") or {}
    phone = customer.get("phone", "")
    mail = customer.get("mail", "")
    status = order.get("status", "новий")
    profit = order_profit(order)
    revenue = order_revenue(order)
    repeat_count = await get_repeat_count(phone)
    note = await get_customer_note(phone)

    repeat_line = "🆕 Новий клієнт" if repeat_count <= 1 else f"🔄 Повторний клієнт: {repeat_count} замовлень"
    note_line = f"\n📝 Нотатка: {esc(note)}" if note else ""
    mail_line = f"\n📧 {esc(mail)}" if mail else ""
    carrier = order.get("carrier") or "—"

    return (
        f"{STATUS_EMOJI.get(status, '🆕')} *Замовлення* #{str(order.get('_id'))[-6:].upper()}\n\n"
        f"{fmt_order_items(order)}\n\n"
        f"👤 {esc(customer.get('name', ''))} {esc(customer.get('surname', ''))}\n"
        f"📞 {esc(phone)}{mail_line}\n"
        f"🚚 {esc(carrier)}\n"
        f"📍 {esc(order.get('city', ''))}, відділення/поштомат: {esc(order.get('department', ''))}\n\n"
        f"💰 Сума: {fmt_money(revenue)} грн\n"
        f"📦 Вартість доставки: {fmt_money(order.get('deliveryCost', 0))} грн\n"
        f"📈 Прибуток: {fmt_money(profit)} грн\n\n"
        f"{repeat_line}{note_line}\n\n"
        f"{STATUS_COLOR.get(status, '⚪')} Статус: *{status}*"
    )


def ikb_order_actions(order_id: str) -> InlineKeyboardMarkup:
    rows = []
    row = []
    for key, label in STATUS_LABELS.items():
        row.append(InlineKeyboardButton(text=f"{STATUS_COLOR[label]} {label}", callback_data=f"setstatus:{order_id}:{key}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton(text="🚚 Вартість доставки", callback_data=f"setdelivery:{order_id}"),
        InlineKeyboardButton(text="📝 Нотатка клієнту", callback_data=f"setnote:{order_id}"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


class SetDelivery(StatesGroup):
    typing = State()


class SetNote(StatesGroup):
    typing = State()


class LookupCustomer(StatesGroup):
    typing = State()


class Auth(StatesGroup):
    waiting_password = State()


def kb_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🎯 Дашборд"), KeyboardButton(text="📋 Замовлення")],
        [KeyboardButton(text="📦 Не відправлені"), KeyboardButton(text="💸 Виручка і прибуток")],
        [KeyboardButton(text="🔥 Топ товарів"), KeyboardButton(text="👜 Залишки")],
        [KeyboardButton(text="👥 Онлайн зараз"), KeyboardButton(text="📊 Аналітика сайту")],
        [KeyboardButton(text="📱 Пристрої"), KeyboardButton(text="🔍 Клієнт за телефоном")],
        [KeyboardButton(text="📈 Онлайн за 24 год")],
    ], resize_keyboard=True)


def kb_cancel() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ Скасувати")]], resize_keyboard=True)


def ikb_period(prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Сьогодні", callback_data=f"{prefix}:day"),
        InlineKeyboardButton(text="Тиждень", callback_data=f"{prefix}:week"),
        InlineKeyboardButton(text="Місяць", callback_data=f"{prefix}:month"),
        InlineKeyboardButton(text="Весь час", callback_data=f"{prefix}:all"),
    ]])


# Унікальні відвідувачі (дедуплікація за IP)
async def get_unique_visitors_count(since: datetime | None = None) -> int:
    query = {"lastSeen": {"$gte": since}} if since is not None else {}
    ips = await db_call(visitors_col.distinct("ip", query), default=[], raise_on_fail=False) or []
    return len([ip for ip in ips if ip])


bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
dp = Dispatcher(storage=MemoryStorage())


async def safe_send(uid: int, text: str, **kwargs):
    """Відправка з запасним варіантом без Markdown, якщо розмітка зламалась."""
    try:
        return await bot.send_message(uid, text, **kwargs)
    except TelegramBadRequest:
        return await bot.send_message(uid, text, parse_mode=None, **kwargs)


async def require_auth(msg: Message, state: FSMContext) -> bool:
    try:
        authed = await is_authorized(msg.from_user.id)
    except DBUnavailable:
        await msg.answer(DB_ERROR_TEXT)
        return False
    if authed:
        return True
    current_state = await state.get_state()
    if current_state != Auth.waiting_password:
        await state.set_state(Auth.waiting_password)
        await msg.answer("🔒 *Доступ закритий*\n\nВведіть пароль:", reply_markup=ReplyKeyboardRemove())
    return False


@dp.errors()
async def global_error_handler(event, exception=None):
    exc = exception if exception is not None else getattr(event, "exception", None)
    logger.exception("Unhandled error while processing update: %s", exc)
    update = getattr(event, "update", None)
    chat_id = None
    try:
        if update and update.message:
            chat_id = update.message.chat.id
        elif update and update.callback_query and update.callback_query.message:
            chat_id = update.callback_query.message.chat.id
    except Exception:
        chat_id = None
    if chat_id is not None:
        try:
            await bot.send_message(chat_id, DB_ERROR_TEXT, reply_markup=kb_main())
        except Exception:
            pass
    return True


async def build_dashboard_text() -> str:
    today_start = day_start_utc()
    today_orders = await db_call(
        orders_col.find({"createdAt": {"$gte": today_start}}, {"_id": 0}).to_list(length=None),
        default=[], raise_on_fail=False
    ) or []

    total_count = len(today_orders)
    done_count = sum(1 for o in today_orders if o.get("status") == "успішно")
    refused_count = sum(1 for o in today_orders if o.get("status") == "відмовлено")
    not_shipped = sum(1 for o in today_orders if o.get("status") in NOT_SHIPPED_STATUSES)
    revenue = sum(order_revenue(o) for o in today_orders if o.get("status") != "відмовлено")
    profit = sum(order_profit(o) for o in today_orders if o.get("status") != "відмовлено")

    online = None
    visitors_today = None
    conversion_text = "н/д"
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
            async with session.get(f"{API_BASE_URL}/api/online-count") as r:
                online = (await r.json()).get("online")
            async with session.get(f"{API_BASE_URL}/api/visitors/stats") as r:
                visitors_today = (await r.json()).get("dailyVisitors")
        if visitors_today:
            conversion_text = f"{total_count} із {visitors_today} відвідувачів ({round(total_count / visitors_today * 100, 1)}%)"
    except Exception:
        logger.exception("dashboard api fetch failed")

    unique_today = await get_unique_visitors_count(since=today_start)

    lines = [
        "🎯 *Дашборд сьогодні*", "",
        f"📦 Замовлень: *{total_count}*",
        f"✅ Успішних: *{done_count}*",
        f"❌ Відмов: *{refused_count}*",
        f"🚚 Не відправлено: *{not_shipped}*", "",
        f"💰 Виручка: *{fmt_money(revenue)} грн*",
        f"📈 Прибуток: *{fmt_money(profit)} грн*", "",
    ]
    if online is not None:
        lines.append(f"👥 Онлайн зараз: *{online}*")
    if visitors_today is not None:
        lines.append(f"📊 Відвідувачів сьогодні (з сайту): *{visitors_today}*")
    lines.append(f"🧑‍🤝‍🧑 Унікальних відвідувачів сьогодні: *{unique_today}*")
    lines.append(f"🎯 Конверсія: {conversion_text}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Авторизація
# ---------------------------------------------------------------------------
@dp.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    try:
        authed = await is_authorized(msg.from_user.id)
    except DBUnavailable:
        await msg.answer(DB_ERROR_TEXT)
        return
    if authed:
        await msg.answer("👋 *Lurelle Orders Bot*", reply_markup=kb_main())
        try:
            await msg.answer(await build_dashboard_text(), reply_markup=kb_main())
        except DBUnavailable:
            await msg.answer(DB_ERROR_TEXT)
    else:
        await state.set_state(Auth.waiting_password)
        await msg.answer("🔒 *Доступ закритий*\n\nВведіть пароль:", reply_markup=ReplyKeyboardRemove())


@dp.message(Auth.waiting_password)
async def check_password(msg: Message, state: FSMContext):
    if msg.text == BOT_PASSWORD:
        try:
            await authorize(msg.from_user.id)
        except DBUnavailable:
            await msg.answer(DB_ERROR_TEXT)
            return
        await state.clear()
        await msg.answer("✅ *Пароль вірний! Ласкаво просимо.*", reply_markup=kb_main())
        try:
            await msg.answer(await build_dashboard_text(), reply_markup=kb_main())
        except DBUnavailable:
            await msg.answer(DB_ERROR_TEXT)
    else:
        await msg.answer("❌ Невірний пароль. Спробуй ще раз:")


# ---------------------------------------------------------------------------
# Меню
# ---------------------------------------------------------------------------
@dp.message(F.text == "🎯 Дашборд")
async def dashboard_cmd(msg: Message, state: FSMContext):
    if not await require_auth(msg, state): return
    try:
        await msg.answer(await build_dashboard_text(), reply_markup=kb_main())
    except DBUnavailable:
        await msg.answer(DB_ERROR_TEXT, reply_markup=kb_main())


@dp.message(F.text == "📦 Не відправлені")
async def not_shipped_cmd(msg: Message, state: FSMContext):
    if not await require_auth(msg, state): return
    try:
        orders = await db_call(
            orders_col.find({"status": {"$in": NOT_SHIPPED_STATUSES}}).sort("createdAt", 1).to_list(length=None)
        )
    except DBUnavailable:
        return await msg.answer(DB_ERROR_TEXT, reply_markup=kb_main())
    if not orders:
        return await msg.answer("📭 Усі замовлення відправлені.", reply_markup=kb_main())
    await msg.answer(f"📦 Не відправлено: *{len(orders)}*", reply_markup=kb_main())
    for o in orders[:15]:
        await safe_send(msg.chat.id, await fmt_order_card(o), reply_markup=ikb_order_actions(str(o["_id"])))


@dp.message(F.text == "📋 Замовлення")
async def orders_list_cmd(msg: Message, state: FSMContext):
    if not await require_auth(msg, state): return
    await msg.answer("Оберіть період:", reply_markup=ikb_period("allorders"))


@dp.callback_query(F.data.startswith("allorders:"))
async def orders_list_period_cb(cb: CallbackQuery):
    try:
        period = cb.data.split(":")[1]
        start = period_start(period)
        query = {"createdAt": {"$gte": start}} if start else {}
        orders = await db_call(
            orders_col.find(query).sort("createdAt", -1).to_list(length=None),
            default=[], raise_on_fail=False
        ) or []
        if not orders:
            await cb.message.edit_text("📭 Замовлень за цей період немає.", reply_markup=ikb_period("allorders"))
            await cb.answer()
            return
        await cb.message.edit_text(f"📋 Замовлень за період: *{len(orders)}*", reply_markup=ikb_period("allorders"))
        await cb.answer()
        for o in orders[:15]:
            await safe_send(cb.message.chat.id, await fmt_order_card(o), reply_markup=ikb_order_actions(str(o["_id"])))
        if len(orders) > 15:
            await cb.message.answer(f"…і ще {len(orders) - 15} замовлень. Звузьте період, щоб побачити менший список.")
    except Exception:
        logger.exception("orders_list_period_cb failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass


@dp.message(F.text == "💸 Виручка і прибуток")
async def revenue_cmd(msg: Message, state: FSMContext):
    if not await require_auth(msg, state): return
    await msg.answer("Оберіть період:", reply_markup=ikb_period("revenue"))


@dp.callback_query(F.data.startswith("revenue:"))
async def revenue_period_cb(cb: CallbackQuery):
    try:
        period = cb.data.split(":")[1]
        start = period_start(period)
        query = {"createdAt": {"$gte": start}} if start else {}
        orders = await db_call(orders_col.find(query).to_list(length=None), default=[], raise_on_fail=False) or []
        active = [o for o in orders if o.get("status") != "відмовлено"]
        revenue = sum(order_revenue(o) for o in active)
        profit = sum(order_profit(o) for o in active)
        refused = len(orders) - len(active)
        text = (
            f"💸 *Виручка і прибуток*\n\n"
            f"Замовлень: *{len(orders)}* (відмов: {refused})\n"
            f"Виручка: *{fmt_money(revenue)} грн*\n"
            f"Прибуток: *{fmt_money(profit)} грн*"
        )
        await cb.message.edit_text(text, reply_markup=ikb_period("revenue"))
        await cb.answer()
    except Exception:
        logger.exception("revenue_period_cb failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass


@dp.message(F.text == "🔥 Топ товарів")
async def top_products_cmd(msg: Message, state: FSMContext):
    if not await require_auth(msg, state): return
    await msg.answer("Оберіть період:", reply_markup=ikb_period("topprod"))


@dp.callback_query(F.data.startswith("topprod:"))
async def top_products_cb(cb: CallbackQuery):
    try:
        period = cb.data.split(":")[1]
        start = period_start(period)
        query = {"createdAt": {"$gte": start}} if start else {}
        orders = await db_call(orders_col.find(query).to_list(length=None), default=[], raise_on_fail=False) or []
        counts: dict = {}
        for o in orders:
            if o.get("status") == "відмовлено":
                continue
            for i in o.get("items") or []:
                name = i.get("name", "—")
                if i.get("color"):
                    name = f"{name} ({i.get('color')})"
                counts[name] = counts.get(name, 0) + (i.get("quantity") or 1)
        top = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:10]
        if not top:
            text = "📭 Немає продажів за цей період."
        else:
            lines = ["🔥 *Топ сумок*", ""]
            for i, (name, qty) in enumerate(top, 1):
                lines.append(f"{i}. {esc(name)} — {qty} шт.")
            text = "\n".join(lines)
        await cb.message.edit_text(text, reply_markup=ikb_period("topprod"))
        await cb.answer()
    except Exception:
        logger.exception("top_products_cb failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass


@dp.message(F.text == "👜 Залишки")
async def stock_cmd(msg: Message, state: FSMContext):
    if not await require_auth(msg, state): return
    products = await db_call(
        products_col.find({}, {"name": 1, "color": 1, "inStock": 1}).sort("inStock", 1).to_list(length=None),
        default=[], raise_on_fail=False
    ) or []
    if not products:
        return await msg.answer("📭 Товарів у базі немає.", reply_markup=kb_main())

    def label(p):
        color = f" ({esc(p.get('color'))})" if p.get("color") else ""
        return f"{esc(p.get('name', '—'))}{color}"

    out_of_stock = [p for p in products if (p.get("inStock") or 0) <= 0]
    low = [p for p in products if 0 < (p.get("inStock") or 0) <= LOW_STOCK_THRESHOLD]
    total_units = sum(max(p.get("inStock") or 0, 0) for p in products)

    lines = [
        "👜 *Залишки на складі*", "",
        f"Моделей: *{len(products)}*, одиниць на складі: *{total_units}*", "",
    ]
    if out_of_stock:
        lines.append("❌ *Немає в наявності:*")
        lines += [f"• {label(p)}" for p in out_of_stock[:30]]
        lines.append("")
    if low:
        lines.append(f"⚠️ *Закінчуються (≤ {LOW_STOCK_THRESHOLD} шт.):*")
        lines += [f"• {label(p)} — {p.get('inStock')} шт." for p in low[:30]]
        lines.append("")
    if not out_of_stock and not low:
        lines.append("✅ Усього достатньо.")
    await msg.answer("\n".join(lines), reply_markup=kb_main())


@dp.message(F.text == "👥 Онлайн зараз")
async def online_now_cmd(msg: Message, state: FSMContext):
    if not await require_auth(msg, state): return
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
            async with session.get(f"{API_BASE_URL}/api/online-count") as r:
                online = (await r.json()).get("online")
        await msg.answer(f"👥 Онлайн зараз: *{online}*", reply_markup=kb_main())
    except Exception:
        logger.exception("online_now_cmd failed")
        await msg.answer(API_ERROR_TEXT, reply_markup=kb_main())


@dp.message(F.text == "📊 Аналітика сайту")
async def site_analytics_cmd(msg: Message, state: FSMContext):
    if not await require_auth(msg, state): return
    today_start = day_start_utc()
    orders_today = await db_call(
        orders_col.count_documents({"createdAt": {"$gte": today_start}}), default=0, raise_on_fail=False
    ) or 0
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
            async with session.get(f"{API_BASE_URL}/api/online-count") as r:
                online = (await r.json()).get("online")
            async with session.get(f"{API_BASE_URL}/api/visitors/stats") as r:
                stats = await r.json()
    except Exception:
        logger.exception("site_analytics_cmd failed")
        return await msg.answer(API_ERROR_TEXT, reply_markup=kb_main())

    daily = stats.get("dailyVisitors", 0)
    total = stats.get("totalVisitors", 0)
    unique_today = await get_unique_visitors_count(since=today_start)
    unique_total = await get_unique_visitors_count(since=None)

    conversion = f"{round(orders_today / daily * 100, 1)}%" if daily else "н/д"
    conversion_unique = f"{round(orders_today / unique_today * 100, 1)}%" if unique_today else "н/д"
    text = (
        "📊 *Аналітика сайту Lurelle*\n\n"
        f"👥 Онлайн зараз: *{online}*\n"
        f"📈 Відвідувачів сьогодні (з сайту): *{daily}*\n"
        f"🧑‍🤝‍🧑 Унікальних відвідувачів сьогодні: *{unique_today}*\n"
        f"🌍 Всього відвідувачів за весь час (з сайту): *{total}*\n"
        f"🧑‍🤝‍🧑 Унікальних відвідувачів за весь час: *{unique_total}*\n"
        f"📦 Замовлень сьогодні: *{orders_today}*\n"
        f"🎯 Конверсія сьогодні (з сайту): *{conversion}*\n"
        f"🎯 Конверсія сьогодні (унікальні): *{conversion_unique}*"
    )
    await msg.answer(text, reply_markup=kb_main())


@dp.message(F.text == "📱 Пристрої")
async def devices_cmd(msg: Message, state: FSMContext):
    if not await require_auth(msg, state): return
    try:
        visitors = await db_call(
            visitors_col.find({}, {"userAgent": 1, "ip": 1}).to_list(length=None), default=[], raise_on_fail=False
        ) or []
    except DBUnavailable:
        return await msg.answer(DB_ERROR_TEXT, reply_markup=kb_main())
    if not visitors:
        return await msg.answer("📭 Ще немає даних про відвідувачів.", reply_markup=kb_main())

    unique_by_ip: dict[str, str] = {}
    for v in visitors:
        ip = v.get("ip")
        if not ip:
            continue
        unique_by_ip[ip] = v.get("userAgent") or ""

    total_sessions = len(visitors)
    total_unique = len(unique_by_ip)
    if total_unique == 0:
        return await msg.answer("📭 Ще немає даних про відвідувачів.", reply_markup=kb_main())

    counts = {"Android": 0, "iPhone/iPad": 0, "Windows": 0, "Mac": 0, "Linux": 0, "Інше": 0}
    for ua in unique_by_ip.values():
        ua = ua.lower()
        if "android" in ua:
            counts["Android"] += 1
        elif "iphone" in ua or "ipad" in ua:
            counts["iPhone/iPad"] += 1
        elif "windows" in ua:
            counts["Windows"] += 1
        elif "macintosh" in ua or "mac os" in ua:
            counts["Mac"] += 1
        elif "linux" in ua:
            counts["Linux"] += 1
        else:
            counts["Інше"] += 1

    lines = [
        "📱 *Пристрої відвідувачів*",
        f"_(унікальних: {total_unique} з {total_sessions} сесій)_", "",
    ]
    for name, c in sorted(counts.items(), key=lambda x: x[1], reverse=True):
        if c == 0:
            continue
        pct = round(c / total_unique * 100, 1)
        lines.append(f"{name}: {c} ({pct}%)")
    await msg.answer("\n".join(lines), reply_markup=kb_main())


@dp.message(F.text == "🔍 Клієнт за телефоном")
async def lookup_customer_start(msg: Message, state: FSMContext):
    if not await require_auth(msg, state): return
    await state.set_state(LookupCustomer.typing)
    await msg.answer("📞 Введіть номер телефону клієнта:", reply_markup=kb_cancel())


@dp.message(LookupCustomer.typing)
async def lookup_customer_save(msg: Message, state: FSMContext):
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_main())
    phone = msg.text.strip()
    await state.clear()
    try:
        orders = await db_call(
            orders_col.find({"customer.phone": phone}).sort("createdAt", -1).to_list(length=None)
        )
    except DBUnavailable:
        return await msg.answer(DB_ERROR_TEXT, reply_markup=kb_main())
    if not orders:
        return await msg.answer("📭 Замовлень з таким номером не знайдено.", reply_markup=kb_main())

    total_spent = sum(order_revenue(o) for o in orders if o.get("status") != "відмовлено")
    note = await get_customer_note(phone)
    note_line = f"\n📝 Нотатка: {esc(note)}" if note else "\n📝 Нотатки немає"
    text = (
        f"📞 *{esc(phone)}*\n\n"
        f"🛒 Всього замовлень: *{len(orders)}*\n"
        f"💰 Всього витрачено: *{fmt_money(total_spent)} грн*{note_line}"
    )
    await msg.answer(text, reply_markup=kb_main())
    for o in orders[:5]:
        await safe_send(msg.chat.id, await fmt_order_card(o), reply_markup=ikb_order_actions(str(o["_id"])))


@dp.message(F.text == "📈 Онлайн за 24 год")
async def online_history_cmd(msg: Message, state: FSMContext):
    if not await require_auth(msg, state): return
    since = utcnow() - timedelta(hours=24)
    points = await db_call(
        online_history_col.find({"ts": {"$gte": since}}).sort("ts", 1).to_list(length=None),
        default=[], raise_on_fail=False
    ) or []
    if len(points) < 2:
        return await msg.answer("📭 Ще недостатньо даних для графіка.", reply_markup=kb_main())

    xs = [p["ts"].replace(tzinfo=timezone.utc).astimezone(KYIV) for p in points]
    ys = [p["count"] for p in points]
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(xs, ys, color="#2e7d32")
    ax.set_title("Онлайн за останні 24 год")
    ax.set_ylabel("Користувачів")
    fig.autofmt_xdate()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=140)
    plt.close(fig)
    buf.seek(0)
    await msg.answer_photo(BufferedInputFile(buf.read(), filename="online.png"), reply_markup=kb_main())


# ---------------------------------------------------------------------------
# Дії над замовленням
# ---------------------------------------------------------------------------
@dp.callback_query(F.data.startswith("setstatus:"))
async def set_status_cb(cb: CallbackQuery):
    try:
        _, order_id, key = cb.data.split(":")
        new_status = STATUS_LABELS.get(key)
        if not new_status:
            return await cb.answer("Невідомий статус", show_alert=True)
        await db_call(orders_col.update_one({"_id": ObjectId(order_id)}, {"$set": {"status": new_status}}))
        order = await db_call(orders_col.find_one({"_id": ObjectId(order_id)}))
        if not order:
            return await cb.answer("Замовлення не знайдено", show_alert=True)
        await cb.message.edit_text(await fmt_order_card(order), reply_markup=ikb_order_actions(order_id))
        await cb.answer(f"Статус: {new_status}")
    except Exception:
        logger.exception("set_status_cb failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass


@dp.callback_query(F.data.startswith("setdelivery:"))
async def set_delivery_start(cb: CallbackQuery, state: FSMContext):
    try:
        order_id = cb.data.split(":")[1]
        await state.set_state(SetDelivery.typing)
        await state.update_data(order_id=order_id)
        await cb.message.answer("🚚 Введіть вартість доставки (грн):", reply_markup=kb_cancel())
        await cb.answer()
    except Exception:
        logger.exception("set_delivery_start failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass


@dp.message(SetDelivery.typing)
async def set_delivery_save(msg: Message, state: FSMContext):
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_main())
    try:
        value = float(msg.text.strip().replace(",", "."))
    except ValueError:
        return await msg.answer("⚠️ Введіть число, наприклад 60:", reply_markup=kb_cancel())
    fd = await state.get_data()
    order_id = fd["order_id"]
    await state.clear()
    try:
        await db_call(orders_col.update_one({"_id": ObjectId(order_id)}, {"$set": {"deliveryCost": value}}))
        order = await db_call(orders_col.find_one({"_id": ObjectId(order_id)}))
    except DBUnavailable:
        return await msg.answer(DB_ERROR_TEXT, reply_markup=kb_main())
    if not order:
        return await msg.answer("Замовлення не знайдено.", reply_markup=kb_main())
    await safe_send(msg.chat.id, await fmt_order_card(order), reply_markup=ikb_order_actions(order_id))


@dp.callback_query(F.data.startswith("setnote:"))
async def set_note_start(cb: CallbackQuery, state: FSMContext):
    try:
        order_id = cb.data.split(":")[1]
        order = await db_call(orders_col.find_one({"_id": ObjectId(order_id)}))
        if not order:
            return await cb.answer("Замовлення не знайдено", show_alert=True)
        phone = (order.get("customer") or {}).get("phone", "")
        await state.set_state(SetNote.typing)
        await state.update_data(phone=phone, order_id=order_id)
        await cb.message.answer(f"📝 Введіть нотатку для клієнта {esc(phone)}:", reply_markup=kb_cancel())
        await cb.answer()
    except Exception:
        logger.exception("set_note_start failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass


@dp.message(SetNote.typing)
async def set_note_save(msg: Message, state: FSMContext):
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_main())
    fd = await state.get_data()
    phone, order_id = fd["phone"], fd["order_id"]
    await state.clear()
    try:
        await db_call(notes_col.update_one({"_id": phone}, {"$set": {"text": msg.text.strip(), "updatedAt": utcnow()}}, upsert=True))
        order = await db_call(orders_col.find_one({"_id": ObjectId(order_id)}))
    except DBUnavailable:
        return await msg.answer(DB_ERROR_TEXT, reply_markup=kb_main())
    await msg.answer("✅ Нотатку збережено.", reply_markup=kb_main())
    if order:
        await safe_send(msg.chat.id, await fmt_order_card(order), reply_markup=ikb_order_actions(order_id))


# ---------------------------------------------------------------------------
# Фонові задачі
# ---------------------------------------------------------------------------
online_alert_active = False
site_down_active = False


async def new_order_poll_task():
    """Кожні 15 с бере з бази замовлення з notified != true і шле їх у Telegram."""
    while True:
        await asyncio.sleep(15)
        try:
            uids = await get_all_uids()
            if not uids:
                # Нікому відправляти (ніхто не ввів пароль) — не позначаємо замовлення як надіслані
                continue

            new_orders = await db_call(
                orders_col.find({"notified": {"$ne": True}}).sort("createdAt", 1).to_list(length=None),
                default=[], raise_on_fail=False
            ) or []

            for order in new_orders:
                text = "🛍 *Нове замовлення!*\n\n" + await fmt_order_card(order)
                delivered = False
                for uid in uids:
                    try:
                        await safe_send(uid, text, reply_markup=ikb_order_actions(str(order["_id"])))
                        delivered = True
                    except Exception:
                        logger.exception("Failed to send new order notice to %s", uid)
                if delivered:
                    await db_call(
                        orders_col.update_one({"_id": order["_id"]}, {"$set": {"notified": True}}),
                        raise_on_fail=False,
                    )
        except Exception:
            logger.exception("new_order_poll_task failed")


async def online_snapshot_task():
    global online_alert_active
    while True:
        await asyncio.sleep(300)
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
                async with session.get(f"{API_BASE_URL}/api/online-count") as r:
                    online = (await r.json()).get("online", 0)
            await db_call(online_history_col.insert_one({"ts": utcnow(), "count": online}), raise_on_fail=False)

            if online >= ONLINE_ALERT_THRESHOLD and not online_alert_active:
                online_alert_active = True
                uids = await get_all_uids()
                for uid in uids:
                    try:
                        await bot.send_message(uid, f"🔔 Онлайн перевищив {ONLINE_ALERT_THRESHOLD}: зараз *{online}* користувачів")
                    except Exception:
                        pass
            elif online < ONLINE_ALERT_THRESHOLD:
                online_alert_active = False
        except Exception:
            logger.exception("online_snapshot_task failed")


async def site_health_task():
    global site_down_active
    consecutive_fails = 0
    while True:
        await asyncio.sleep(60)
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
                async with session.get(f"{API_BASE_URL}/health") as r:
                    ok = r.status == 200
        except Exception:
            ok = False

        if not ok:
            consecutive_fails += 1
        else:
            consecutive_fails = 0

        uids = await get_all_uids()
        if consecutive_fails >= 3 and not site_down_active:
            site_down_active = True
            for uid in uids:
                try:
                    await bot.send_message(uid, "🚨 *Сервер сайту недоступний!* API не відповідає вже кілька хвилин.")
                except Exception:
                    pass
        elif ok and site_down_active:
            site_down_active = False
            for uid in uids:
                try:
                    await bot.send_message(uid, "✅ Сервер сайту знову працює.")
                except Exception:
                    pass


async def ping(request):
    return web.Response(status=204)


async def main():
    init_mongo()
    try:
        await mongo_client.admin.command("ping")
        logger.info("MongoDB connection OK (db: %s)", db.name)
    except Exception:
        logger.exception("MongoDB connection FAILED at startup")

    await load_authorized_uids()

    app = web.Application()
    app.router.add_get("/", ping)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.environ.get("PORT", 8080)))
    await site.start()

    asyncio.create_task(new_order_poll_task())
    asyncio.create_task(online_snapshot_task())
    asyncio.create_task(site_health_task())

    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("Lurelle orders bot запущено...")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())