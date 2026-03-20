#!/usr/bin/env python3
"""
Channel Analytics Bot
Мульти-тенантная аналитика Telegram-каналов.
Деплой: Railway (SQLite с persistent volume).
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Optional, List, Tuple

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery, FSInputFile, InlineKeyboardButton,
    InlineKeyboardMarkup, Message,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

from report_generator import generate_report

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

BOT_TOKEN    = os.getenv("BOT_TOKEN", "")
SUPER_ADMINS = [int(x) for x in os.getenv("SUPER_ADMINS", "").split(",") if x.strip()]
DB_PATH      = os.getenv("DB_PATH", "/data/analytics.db")

bot       = Bot(token=BOT_TOKEN)
dp        = Dispatcher(storage=MemoryStorage())
scheduler = AsyncIOScheduler(timezone="UTC")


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════════════════════

async def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
        CREATE TABLE IF NOT EXISTS channels (
            channel_id      INTEGER PRIMARY KEY,
            title           TEXT,
            username        TEXT,
            active          INTEGER DEFAULT 1,
            co_admin_access INTEGER DEFAULT 0,
            added_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS user_channels (
            user_id    INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            is_owner   INTEGER DEFAULT 1,
            username   TEXT,
            first_name TEXT,
            added_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, channel_id)
        );

        CREATE TABLE IF NOT EXISTS snapshots (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            members    INTEGER NOT NULL,
            taken_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS posts (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            views      INTEGER DEFAULT 0,
            has_media  INTEGER DEFAULT 0,
            posted_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(channel_id, message_id)
        );

        CREATE INDEX IF NOT EXISTS idx_snap ON snapshots(channel_id, taken_at);
        CREATE INDEX IF NOT EXISTS idx_posts ON posts(channel_id, posted_at);
        """)
        for sql in [
            "ALTER TABLE channels ADD COLUMN active INTEGER DEFAULT 1",
            "ALTER TABLE channels ADD COLUMN co_admin_access INTEGER DEFAULT 0",
            "ALTER TABLE user_channels ADD COLUMN is_owner INTEGER DEFAULT 1",
            "ALTER TABLE user_channels ADD COLUMN username TEXT",
            "ALTER TABLE user_channels ADD COLUMN first_name TEXT",
        ]:
            try:
                await db.execute(sql)
            except Exception:
                pass
        await db.commit()


# ── Каналы ───────────────────────────────────────────────────────────────────

async def ensure_channel(channel_id: int, title: str, username: Optional[str]):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO channels (channel_id, title, username) VALUES (?, ?, ?)
               ON CONFLICT(channel_id) DO UPDATE
               SET title=excluded.title, username=excluded.username""",
            (channel_id, title, username),
        )
        await db.commit()


async def get_channel_info(channel_id: int) -> Optional[Tuple]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT channel_id, title, username, active, co_admin_access FROM channels WHERE channel_id=?",
            (channel_id,),
        ) as cur:
            return await cur.fetchone()


async def set_channel_active(channel_id: int, active: bool):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE channels SET active=? WHERE channel_id=?",
            (1 if active else 0, channel_id),
        )
        await db.commit()


async def toggle_co_admin(channel_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT co_admin_access FROM channels WHERE channel_id=?", (channel_id,)
        ) as cur:
            row = await cur.fetchone()
        new = 0 if (row and row[0]) else 1
        await db.execute(
            "UPDATE channels SET co_admin_access=? WHERE channel_id=?", (new, channel_id)
        )
        await db.commit()
    return bool(new)


# ── Пользователи ─────────────────────────────────────────────────────────────

async def link_user_channel(
    user_id: int,
    channel_id: int,
    is_owner: bool = True,
    username: Optional[str] = None,
    first_name: Optional[str] = None,
):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO user_channels (user_id, channel_id, is_owner, username, first_name)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(user_id, channel_id) DO UPDATE
               SET username=excluded.username, first_name=excluded.first_name""",
            (user_id, channel_id, int(is_owner), username, first_name),
        )
        await db.commit()


async def unlink_user_channel(user_id: int, channel_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM user_channels WHERE user_id=? AND channel_id=?",
            (user_id, channel_id),
        )
        await db.commit()


async def get_user_channels_ids(user_id: int) -> List[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """SELECT uc.channel_id FROM user_channels uc
               JOIN channels c ON c.channel_id=uc.channel_id
               WHERE uc.user_id=? AND c.active=1""",
            (user_id,),
        ) as cur:
            return [r[0] for r in await cur.fetchall()]


async def get_user_channels_full(user_id: int) -> List[Tuple]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """SELECT c.channel_id, c.title, c.username, c.active, c.co_admin_access
               FROM user_channels uc
               JOIN channels c ON c.channel_id=uc.channel_id
               WHERE uc.user_id=? AND c.active=1""",
            (user_id,),
        ) as cur:
            return await cur.fetchall()


async def get_channel_owner(channel_id: int) -> Optional[Tuple]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT user_id, username, first_name FROM user_channels WHERE channel_id=? AND is_owner=1 LIMIT 1",
            (channel_id,),
        ) as cur:
            return await cur.fetchone()


async def get_all_channels_panel() -> List[Tuple]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """SELECT c.channel_id, c.title, c.username, c.active, c.co_admin_access,
                      uc.user_id, uc.username, uc.first_name
               FROM channels c
               LEFT JOIN user_channels uc ON uc.channel_id=c.channel_id AND uc.is_owner=1
               ORDER BY c.active DESC, c.title ASC"""
        ) as cur:
            return await cur.fetchall()


# ── Данные ───────────────────────────────────────────────────────────────────

async def save_snapshot(channel_id: int, members: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO snapshots (channel_id, members) VALUES (?, ?)", (channel_id, members)
        )
        await db.commit()


async def save_post(channel_id: int, message_id: int, views: int, has_media: bool):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO posts (channel_id, message_id, views, has_media) VALUES (?, ?, ?, ?)",
            (channel_id, message_id, views, int(has_media)),
        )
        await db.commit()


# ══════════════════════════════════════════════════════════════════════════════
# КОНТРОЛЬ ДОСТУПА
# ══════════════════════════════════════════════════════════════════════════════

async def can_access(user_id: int, channel_id: int) -> bool:
    if user_id in SUPER_ADMINS:
        return True
    if channel_id in await get_user_channels_ids(user_id):
        return True
    info = await get_channel_info(channel_id)
    if not info or not info[4]:
        return False
    try:
        admins = await bot.get_chat_administrators(channel_id)
        return user_id in [a.user.id for a in admins]
    except Exception:
        return False


# ══════════════════════════════════════════════════════════════════════════════
# СТАТИСТИКА
# ══════════════════════════════════════════════════════════════════════════════

async def get_stats(channel_id: int, period_days: Optional[int]) -> dict:
    since = (
        (datetime.utcnow() - timedelta(days=period_days)).isoformat()
        if period_days else "2000-01-01"
    )

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT members FROM snapshots WHERE channel_id=? ORDER BY taken_at DESC LIMIT 1",
            (channel_id,),
        ) as cur:
            row = await cur.fetchone()
            current = row[0] if row else None

        async with db.execute(
            "SELECT members FROM snapshots WHERE channel_id=? AND taken_at>=? ORDER BY taken_at ASC LIMIT 1",
            (channel_id, since),
        ) as cur:
            row = await cur.fetchone()
            start = row[0] if row else None

        async with db.execute(
            "SELECT MAX(members), MIN(members) FROM snapshots WHERE channel_id=? AND taken_at>=?",
            (channel_id, since),
        ) as cur:
            row = await cur.fetchone()
            peak, low = (row[0], row[1]) if row else (None, None)

        async with db.execute(
            "SELECT COUNT(*), SUM(has_media) FROM posts WHERE channel_id=? AND posted_at>=?",
            (channel_id, since),
        ) as cur:
            row = await cur.fetchone()
            posts_total, media_total = (row[0] or 0, row[1] or 0)

        async with db.execute(
            "SELECT SUM(views), MAX(views), AVG(views) FROM posts WHERE channel_id=? AND posted_at>=?",
            (channel_id, since),
        ) as cur:
            row = await cur.fetchone()
            views_sum = int(row[0] or 0)
            views_max = int(row[1] or 0)
            views_avg = round(row[2] or 0)

        async with db.execute(
            """SELECT DATE(taken_at), MAX(members)-MIN(members)
               FROM snapshots WHERE channel_id=? AND taken_at>=?
               GROUP BY DATE(taken_at) ORDER BY 2 DESC LIMIT 1""",
            (channel_id, since),
        ) as cur:
            best_day = await cur.fetchone()

        async with db.execute(
            """SELECT DATE(taken_at), MAX(members)-MIN(members)
               FROM snapshots WHERE channel_id=? AND taken_at>=?
               GROUP BY DATE(taken_at) ORDER BY 2 ASC LIMIT 1""",
            (channel_id, since),
        ) as cur:
            worst_day = await cur.fetchone()

        async with db.execute(
            """SELECT CAST(strftime('%H', posted_at) AS INTEGER), COUNT(*)
               FROM posts WHERE channel_id=? AND posted_at>=?
               GROUP BY 1 ORDER BY 2 DESC LIMIT 1""",
            (channel_id, since),
        ) as cur:
            best_hour = await cur.fetchone()

        async with db.execute(
            "SELECT COUNT(DISTINCT DATE(posted_at)) FROM posts WHERE channel_id=? AND posted_at>=?",
            (channel_id, since),
        ) as cur:
            row = await cur.fetchone()
            active_days = row[0] if row else 0

        async with db.execute(
            "SELECT DISTINCT DATE(posted_at) FROM posts WHERE channel_id=? ORDER BY 1 DESC",
            (channel_id,),
        ) as cur:
            streak_dates = [r[0] for r in await cur.fetchall()]

        async with db.execute(
            "SELECT COUNT(*) FROM snapshots WHERE channel_id=? AND taken_at>=?",
            (channel_id, since),
        ) as cur:
            row = await cur.fetchone()
            snap_count = row[0] if row else 0

    streak = 0
    today = datetime.utcnow().date()
    for i, d in enumerate(streak_dates):
        if d == (today - timedelta(days=i)).isoformat():
            streak += 1
        else:
            break

    growth = (current - start) if (current is not None and start is not None) else None
    growth_pct = None
    if growth is not None and start and start > 0:
        growth_pct = round(growth / start * 100, 2)

    return dict(
        current=current, start=start, growth=growth, growth_pct=growth_pct,
        peak=peak, low=low, posts_total=posts_total, media_total=media_total,
        views_sum=views_sum, views_max=views_max, views_avg=views_avg,
        best_day=best_day, worst_day=worst_day, best_hour=best_hour,
        active_days=active_days, streak=streak,
        period_days=period_days, snap_count=snap_count,
    )


# ══════════════════════════════════════════════════════════════════════════════
# ФОРМАТИРОВАНИЕ СТАТИСТИКИ
# ══════════════════════════════════════════════════════════════════════════════

def fmt_stats(display: str, s: dict, period_label: str) -> str:
    lines = [f"📊 <b>{display}</b>", f"⏱ Период: <b>{period_label}</b>", ""]

    if s["current"] is not None:
        lines.append(f"👥 Подписчиков: <b>{s['current']:,}</b>")

    if s["growth"] is not None:
        sign  = "+" if s["growth"] >= 0 else ""
        arrow = "📈" if s["growth"] >= 0 else "📉"
        pct   = f"  ({sign}{s['growth_pct']}%)" if s["growth_pct"] is not None else ""
        lines.append(f"{arrow} Прирост: <b>{sign}{s['growth']:,}</b>{pct}")

    if s["peak"] and s["low"] and s["peak"] != s["low"]:
        lines.append(f"🏆 Макс: <b>{s['peak']:,}</b>  |  🔻 Мин: <b>{s['low']:,}</b>")

    lines.append("")
    lines.append(f"✍️ Постов: <b>{s['posts_total']}</b>")

    if s["posts_total"] and s["period_days"]:
        lines.append(f"📆 В день (ср.): <b>{round(s['posts_total']/s['period_days'],1)}</b>")

    if s["posts_total"] and s["media_total"]:
        pct = round(s["media_total"] / s["posts_total"] * 100)
        lines.append(f"🖼 С медиа: <b>{s['media_total']}</b> ({pct}%)")

    if s["active_days"] and s["period_days"]:
        cons = round(s["active_days"] / min(s["period_days"], 30) * 100)
        e = "🔥" if cons >= 80 else ("✅" if cons >= 50 else "⚠️")
        lines.append(f"{e} Активных дней: <b>{s['active_days']}</b> ({cons}%)")

    if s["streak"] > 1:
        lines.append(f"🔥 Серия: <b>{s['streak']} дней подряд</b>")

    lines.append("")

    if s["views_sum"]:
        lines.append(f"👁 Просмотров всего: <b>{s['views_sum']:,}</b>")
    if s["views_avg"]:
        lines.append(f"📊 Среднее на пост: <b>{s['views_avg']:,}</b>")
    if s["views_max"]:
        lines.append(f"🚀 Лучший пост: <b>{s['views_max']:,}</b> просм.")

    lines.append("")
    insights = []

    if s["best_day"] and s["best_day"][1] and s["best_day"][1] > 0:
        insights.append(f"🌟 Лучший день: <b>{s['best_day'][0]}</b> (+{s['best_day'][1]})")
    if s["worst_day"] and s["worst_day"][1] and s["worst_day"][1] < 0:
        insights.append(f"📉 Худший день: <b>{s['worst_day'][0]}</b> ({s['worst_day'][1]})")
    if s["best_hour"] is not None:
        h = s["best_hour"][0]
        insights.append(f"⏰ Лучшее время: <b>{h:02d}:00–{h:02d}:59 UTC</b>")

    if insights:
        lines.extend(insights)
        lines.append("")

    if s["snap_count"] == 0:
        lines.append("⚠️ <i>Данных пока нет — сбор начнётся в течение часа.</i>")

    lines.append(f"<i>🕐 {datetime.utcnow().strftime('%d.%m.%Y %H:%M')} UTC</i>")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# КЛАВИАТУРЫ
# ══════════════════════════════════════════════════════════════════════════════

def kb_periods(channel_id: int, active: int, is_owner: bool = False) -> InlineKeyboardMarkup:
    def b(label, val):
        return InlineKeyboardButton(
            text=f"• {label}" if val == active else label,
            callback_data=f"stats:{channel_id}:{val}",
        )
    rows = [
        [b("День", 1), b("Неделя", 7), b("Месяц", 30), b("Всё время", 0)],
        [
            InlineKeyboardButton(text="🔄 Обновить", callback_data=f"stats:{channel_id}:{active}"),
            InlineKeyboardButton(text="◀ Назад",     callback_data="list_channels"),
        ],
    ]
    if is_owner:
        rows.append([
            InlineKeyboardButton(text="⚙️ Доступ со-администраторов", callback_data=f"coadmin:{channel_id}"),
        ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_channels(channels: List[Tuple]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"📢 {'@'+c[2] if c[2] else (c[1] or str(c[0]))}",
            callback_data=f"sel:{c[0]}",
        )]
        for c in channels
    ])


def kb_report_periods(channel_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📅 День",       callback_data=f"report:{channel_id}:1"),
        InlineKeyboardButton(text="📅 Неделя",     callback_data=f"report:{channel_id}:7"),
        InlineKeyboardButton(text="📅 Месяц",      callback_data=f"report:{channel_id}:30"),
        InlineKeyboardButton(text="📅 Всё время",  callback_data=f"report:{channel_id}:0"),
    ]])


# ══════════════════════════════════════════════════════════════════════════════
# ПАНЕЛЬ СУПЕРАДМИНА
# ══════════════════════════════════════════════════════════════════════════════

async def render_panel() -> Tuple[str, InlineKeyboardMarkup]:
    rows = await get_all_channels_panel()

    if not rows:
        return (
            "📋 <b>Панель управления</b>\n\nКаналов нет.\n\n"
            "Добавьте канал:\n<code>/adduser &lt;user_id&gt; &lt;channel_id&gt;</code>",
            InlineKeyboardMarkup(inline_keyboard=[]),
        )

    active_n   = sum(1 for r in rows if r[3])
    inactive_n = len(rows) - active_n
    lines      = [
        "📋 <b>Панель управления каналами</b>",
        f"🟢 Активных: <b>{active_n}</b>   🔴 Отключённых: <b>{inactive_n}</b>",
        "",
    ]
    btns = []

    for cid, title, uname, active, co_admin, owner_id, owner_uname, owner_fname in rows:
        status  = "🟢" if active else "🔴"
        label   = f"@{uname}" if uname else (title or str(cid))
        co_icon = " 👥" if co_admin else ""

        owner_str = (
            f"@{owner_uname}" if owner_uname else
            (owner_fname if owner_fname else
             (f"id:{owner_id}" if owner_id else "—"))
        )

        lines += [f"{status} <b>{label}</b>{co_icon}", f"     <code>{cid}</code>   👤 {owner_str}", ""]

        btns.append([
            InlineKeyboardButton(text=f"📊 {label[:20]}",             callback_data=f"sel:{cid}"),
            InlineKeyboardButton(text="🔴 Откл" if active else "🟢 Вкл", callback_data=f"ptoggle:{cid}"),
            InlineKeyboardButton(text="👥 ✅" if co_admin else "👥 ❌",  callback_data=f"pcoadmin:{cid}"),
        ])

    btns.append([InlineKeyboardButton(text="🔄 Обновить панель", callback_data="panel_refresh")])

    lines.append(
        "<i>[ 📊 статистика  |  откл/вкл  |  со-адм ]</i>\n"
        f"<i>🕐 {datetime.utcnow().strftime('%d.%m.%Y %H:%M')} UTC</i>"
    )
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=btns)


# ══════════════════════════════════════════════════════════════════════════════
# ДЕКОРАТОРЫ
# ══════════════════════════════════════════════════════════════════════════════

def superadmin(func):
    import functools
    @functools.wraps(func)
    async def wrapper(msg: Message, **kwargs):
        if msg.from_user.id not in SUPER_ADMINS:
            return
        return await func(msg, **kwargs)
    return wrapper


PERIOD_LABELS = {1: "День", 7: "Неделя", 30: "Месяц", 0: "Всё время"}


# ══════════════════════════════════════════════════════════════════════════════
# КОМАНДЫ — ОБЩИЕ
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(Command("start"))
async def cmd_start(msg: Message):
    is_sa = msg.from_user.id in SUPER_ADMINS
    text = (
        "📊 <b>Channel Analytics Bot</b>\n\n"
        "Отслеживаю статистику Telegram-каналов в реальном времени.\n\n"
        "<b>Команды:</b>\n"
        "/stats — статистика каналов\n"
        "/report — скачать отчёт Excel\n"
        "/coadmin — управление доступом со-администраторов\n"
        "/help — справка\n"
    )
    if is_sa:
        text += (
            "\n<b>Суперадмин:</b>\n"
            "/panel — панель управления всеми каналами\n"
            "/adduser <code>&lt;user_id&gt; &lt;channel_id&gt;</code>\n"
            "/removeuser <code>&lt;user_id&gt; &lt;channel_id&gt;</code>\n"
            "/remove <code>&lt;channel_id&gt;</code> — отключить канал\n"
            "/enable <code>&lt;channel_id&gt;</code> — включить канал\n"
            "/listusers — все привязки\n"
            "/snapshot — принудительный снимок данных\n"
            "/digest — разослать недельный дайджест\n"
        )
    await msg.answer(text, parse_mode="HTML")


@dp.message(Command("help"))
async def cmd_help(msg: Message):
    await msg.answer(
        "📖 <b>Справка</b>\n\n"
        "<b>Что отслеживается:</b>\n"
        "• 👥 Подписчики — прирост, убыль, пик, минимум за период\n"
        "• ✍️ Посты — кол-во, среднее в день, доля с медиа\n"
        "• 👁 Просмотры — сумма, среднее, рекорд одного поста\n"
        "• 🔥 Серия активных дней публикаций подряд\n"
        "• ⏰ Лучшее время для постов (по истории)\n"
        "• 🌟 Лучший и худший день по приросту подписчиков\n\n"
        "<b>Со-администраторы:</b>\n"
        "Владелец канала может открыть доступ к статистике всем "
        "его Telegram-администраторам командой /coadmin или кнопкой "
        "<b>⚙️ Доступ со-администраторов</b> в окне статистики.\n\n"
        "<b>Отчёт Excel:</b>\n"
        "Команда /report формирует файл .xlsx с графиками и таблицами "
        "за выбранный период.\n\n"
        "📬 Еженедельный авто-дайджест — каждый понедельник 09:00 UTC.\n"
        "⏱ Снимки подписчиков — каждый час.",
        parse_mode="HTML",
    )


@dp.message(Command("stats", "channels"))
async def cmd_stats(msg: Message):
    channels = await get_user_channels_full(msg.from_user.id)
    if not channels:
        await msg.answer("У вас нет привязанных каналов.\nОбратитесь к администратору бота.")
        return
    if len(channels) == 1:
        cid, title, uname, *_ = channels[0]
        display  = f"@{uname}" if uname else (title or str(cid))
        owner    = await get_channel_owner(cid)
        is_owner = bool(owner and owner[0] == msg.from_user.id)
        await msg.answer(
            f"📢 <b>{display}</b>\n\nВыберите период:",
            reply_markup=kb_periods(cid, 7, is_owner=is_owner),
            parse_mode="HTML",
        )
    else:
        await msg.answer(
            f"📢 <b>Ваши каналы ({len(channels)})</b>\n\nВыберите канал:",
            reply_markup=kb_channels(channels),
            parse_mode="HTML",
        )


@dp.message(Command("coadmin"))
async def cmd_coadmin(msg: Message):
    parts   = msg.text.split()
    user_id = msg.from_user.id

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """SELECT c.channel_id, c.title, c.username, c.co_admin_access
               FROM user_channels uc
               JOIN channels c ON c.channel_id=uc.channel_id
               WHERE uc.user_id=? AND uc.is_owner=1""",
            (user_id,),
        ) as cur:
            owned = await cur.fetchall()

    if not owned and user_id not in SUPER_ADMINS:
        await msg.answer("У вас нет каналов для управления доступом.")
        return

    if len(parts) == 2:
        try:
            cid = int(parts[1])
        except ValueError:
            await msg.answer("❌ Неверный ID канала.")
            return
        if cid not in [c[0] for c in owned] and user_id not in SUPER_ADMINS:
            await msg.answer("❌ Это не ваш канал.")
            return
        new = await toggle_co_admin(cid)
        state = "включён ✅" if new else "отключён ❌"
        detail = (
            "Администраторы канала теперь тоже видят статистику."
            if new else
            "Только вы видите статистику этого канала."
        )
        await msg.answer(
            f"👥 Со-администраторы: <b>{state}</b>\n\n{detail}", parse_mode="HTML"
        )
        return

    lines = ["⚙️ <b>Управление доступом со-администраторов</b>\n"]
    btns  = []
    for cid, title, uname, co_admin in owned:
        label  = f"@{uname}" if uname else (title or str(cid))
        status = "✅ вкл" if co_admin else "❌ выкл"
        lines.append(f"• <b>{label}</b> — {status}")
        action = "🔒 Закрыть" if co_admin else "🔓 Открыть"
        btns.append([InlineKeyboardButton(text=f"{action} {label[:22]}", callback_data=f"coadmin:{cid}")])

    lines.append("\nНажмите кнопку чтобы переключить:")
    await msg.answer(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=btns),
        parse_mode="HTML",
    )


@dp.message(Command("report"))
async def cmd_report(msg: Message):
    channels = await get_user_channels_full(msg.from_user.id)
    if not channels:
        await msg.answer("У вас нет привязанных каналов.")
        return
    if len(channels) == 1:
        cid, title, uname, *_ = channels[0]
        display = f"@{uname}" if uname else (title or str(cid))
        await msg.answer(
            f"📊 <b>Excel-отчёт — {display}</b>\n\nВыберите период:",
            reply_markup=kb_report_periods(cid),
            parse_mode="HTML",
        )
    else:
        btns = [
            [InlineKeyboardButton(
                text=f"📢 {'@'+c[2] if c[2] else (c[1] or str(c[0]))}",
                callback_data=f"report_sel:{c[0]}",
            )]
            for c in channels
        ]
        await msg.answer(
            "📊 <b>Excel-отчёт</b>\n\nВыберите канал:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=btns),
            parse_mode="HTML",
        )


# ══════════════════════════════════════════════════════════════════════════════
# КОМАНДЫ — СУПЕРАДМИН
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(Command("panel"))
@superadmin
async def cmd_panel(msg: Message):
    text, kb = await render_panel()
    await msg.answer(text, reply_markup=kb, parse_mode="HTML")


@dp.message(Command("adduser"))
@superadmin
async def cmd_adduser(msg: Message):
    parts = msg.text.split()
    if len(parts) != 3:
        await msg.answer("Использование: /adduser <user_id> <channel_id>")
        return
    try:
        user_id, channel_id = int(parts[1]), int(parts[2])
    except ValueError:
        await msg.answer("❌ user_id и channel_id должны быть числами.")
        return

    # Гарантируем запись в channels даже если get_chat недоступен
    await ensure_channel(channel_id, str(channel_id), None)
    ch_name = str(channel_id)
    try:
        chat = await bot.get_chat(channel_id)
        await ensure_channel(channel_id, chat.title, chat.username)
        ch_name = f"@{chat.username}" if chat.username else chat.title
    except Exception as e:
        logger.warning(f"get_chat {channel_id}: {e} — канал сохранён без названия")

    uname = fname = None
    try:
        m     = await bot.get_chat_member(channel_id, user_id)
        uname = m.user.username
        fname = m.user.first_name
    except Exception:
        pass

    await link_user_channel(user_id, channel_id, is_owner=True, username=uname, first_name=fname)
    display = f"@{uname}" if uname else (fname or str(user_id))
    await msg.answer(
        f"✅ <b>{display}</b> (<code>{user_id}</code>) → <b>{ch_name}</b>",
        parse_mode="HTML",
    )


@dp.message(Command("removeuser"))
@superadmin
async def cmd_removeuser(msg: Message):
    parts = msg.text.split()
    if len(parts) != 3:
        await msg.answer("Использование: /removeuser <user_id> <channel_id>")
        return
    try:
        await unlink_user_channel(int(parts[1]), int(parts[2]))
        await msg.answer("✅ Привязка удалена.")
    except ValueError:
        await msg.answer("❌ Неверный формат.")


@dp.message(Command("remove"))
@superadmin
async def cmd_remove(msg: Message):
    parts = msg.text.split()
    if len(parts) < 2:
        await msg.answer("Использование: /remove <channel_id> [channel_id2 ...]")
        return
    results = []
    for p in parts[1:]:
        try:
            cid  = int(p)
            info = await get_channel_info(cid)
            if not info:
                await ensure_channel(cid, str(cid), None)
                info = await get_channel_info(cid)
            await set_channel_active(cid, False)
            label = f"@{info[2]}" if info[2] else (info[1] or str(cid))
            results.append(f"🔴 <b>{label}</b> — отключён")
        except ValueError:
            results.append(f"❌ <code>{p}</code> — не число")
    await msg.answer("\n".join(results), parse_mode="HTML")


@dp.message(Command("enable"))
@superadmin
async def cmd_enable(msg: Message):
    parts = msg.text.split()
    if len(parts) < 2:
        await msg.answer("Использование: /enable <channel_id> [channel_id2 ...]")
        return
    results = []
    for p in parts[1:]:
        try:
            cid  = int(p)
            info = await get_channel_info(cid)
            if not info:
                # Канал есть в user_channels но не в channels — создаём запись
                await ensure_channel(cid, str(cid), None)
                info = await get_channel_info(cid)
            await set_channel_active(cid, True)
            label = f"@{info[2]}" if info[2] else (info[1] or str(cid))
            results.append(f"🟢 <b>{label}</b> — включён")
        except ValueError:
            results.append(f"❌ <code>{p}</code> — не число")
    await msg.answer("\n".join(results), parse_mode="HTML")


@dp.message(Command("listusers"))
@superadmin
async def cmd_listusers(msg: Message):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """SELECT uc.user_id, uc.username, uc.first_name, uc.is_owner,
                      c.title, c.username, uc.channel_id, c.active, uc.added_at
               FROM user_channels uc
               LEFT JOIN channels c ON c.channel_id=uc.channel_id
               ORDER BY uc.user_id, uc.added_at"""
        ) as cur:
            rows = await cur.fetchall()

    if not rows:
        await msg.answer("Нет привязанных пользователей.")
        return

    lines    = ["<b>👥 Все привязки:</b>"]
    cur_user = None
    for uid, uname, fname, is_owner, ch_title, ch_uname, cid, ch_active, added_at in rows:
        if uid != cur_user:
            u = f"@{uname}" if uname else (fname or str(uid))
            lines.append(f"\n👤 <b>{u}</b> <code>{uid}</code>")
            cur_user = uid
        ch   = f"@{ch_uname}" if ch_uname else (ch_title or str(cid))
        st   = "🟢" if ch_active else "🔴"
        role = "👑" if is_owner else "👥"
        date = (added_at or "")[:10]
        lines.append(f"   {st}{role} {ch} <code>{cid}</code> — {date}")

    await msg.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("snapshot"))
@superadmin
async def cmd_snapshot(msg: Message):
    m = await msg.answer("⏳ Собираю данные...")
    n = await take_snapshots()
    await m.edit_text(f"✅ Снимки для <b>{n}</b> каналов.", parse_mode="HTML")


@dp.message(Command("digest"))
@superadmin
async def cmd_digest(msg: Message):
    m = await msg.answer("⏳ Рассылаю дайджесты...")
    sent, failed = await send_weekly_digest()
    await m.edit_text(
        f"✅ Отправлено: <b>{sent}</b>   ✗ Ошибок: <b>{failed}</b>", parse_mode="HTML"
    )


# ══════════════════════════════════════════════════════════════════════════════
# СОБЫТИЯ КАНАЛА
# ══════════════════════════════════════════════════════════════════════════════

@dp.channel_post()
async def on_channel_post(msg: Message):
    cid = msg.chat.id
    await ensure_channel(cid, msg.chat.title, msg.chat.username)
    has_media = bool(
        msg.photo or msg.video or msg.document or
        msg.animation or msg.audio or msg.voice or msg.video_note
    )
    await save_post(cid, msg.message_id, getattr(msg, "views", 0) or 0, has_media)


# ══════════════════════════════════════════════════════════════════════════════
# CALLBACKS
# ══════════════════════════════════════════════════════════════════════════════

@dp.callback_query(F.data == "list_channels")
async def cb_list(call: CallbackQuery):
    channels = await get_user_channels_full(call.from_user.id)
    if not channels:
        await call.answer("Нет активных каналов.", show_alert=True)
        return
    await call.message.edit_text(
        f"📢 <b>Ваши каналы ({len(channels)})</b>\n\nВыберите канал:",
        reply_markup=kb_channels(channels),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("sel:"))
async def cb_sel(call: CallbackQuery):
    cid = int(call.data.split(":")[1])
    if not await can_access(call.from_user.id, cid):
        await call.answer("❌ Нет доступа.", show_alert=True)
        return
    info = await get_channel_info(cid)
    if not info or not info[3]:
        await call.answer("🔴 Канал отключён.", show_alert=True)
        return
    display  = f"@{info[2]}" if info[2] else (info[1] or str(cid))
    owner    = await get_channel_owner(cid)
    is_owner = bool(owner and owner[0] == call.from_user.id)
    await call.message.edit_text(
        f"📢 <b>{display}</b>\n\nВыберите период:",
        reply_markup=kb_periods(cid, 7, is_owner=is_owner),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("stats:"))
async def cb_stats(call: CallbackQuery):
    _, ch, per = call.data.split(":")
    cid, period = int(ch), int(per)
    if not await can_access(call.from_user.id, cid):
        await call.answer("❌ Нет доступа.", show_alert=True)
        return
    info = await get_channel_info(cid)
    if not info or not info[3]:
        await call.answer("🔴 Канал отключён.", show_alert=True)
        return
    await call.answer("⏳")
    display     = f"@{info[2]}" if info[2] else (info[1] or str(cid))
    owner       = await get_channel_owner(cid)
    is_owner    = bool(owner and owner[0] == call.from_user.id)
    period_days = period if period > 0 else None
    s    = await get_stats(cid, period_days)
    text = fmt_stats(display, s, PERIOD_LABELS.get(period, f"{period} дней"))
    try:
        await call.message.edit_text(
            text, reply_markup=kb_periods(cid, period, is_owner=is_owner), parse_mode="HTML"
        )
    except Exception:
        pass


@dp.callback_query(F.data.startswith("coadmin:"))
async def cb_coadmin(call: CallbackQuery):
    cid     = int(call.data.split(":")[1])
    user_id = call.from_user.id
    owner   = await get_channel_owner(cid)
    if not (owner and owner[0] == user_id) and user_id not in SUPER_ADMINS:
        await call.answer("❌ Только владелец канала.", show_alert=True)
        return
    new   = await toggle_co_admin(cid)
    state = "включён ✅" if new else "отключён ❌"
    await call.answer(f"Со-администраторы: {state}", show_alert=True)
    info    = await get_channel_info(cid)
    display = f"@{info[2]}" if info and info[2] else (info[1] if info else str(cid))
    try:
        await call.message.edit_text(
            f"📢 <b>{display}</b>\n\nВыберите период:",
            reply_markup=kb_periods(cid, 7, is_owner=True),
            parse_mode="HTML",
        )
    except Exception:
        pass


# ── Панель — callbacks ────────────────────────────────────────────────────────

@dp.callback_query(F.data == "panel_refresh")
async def cb_panel_refresh(call: CallbackQuery):
    if call.from_user.id not in SUPER_ADMINS:
        await call.answer("❌", show_alert=True)
        return
    text, kb = await render_panel()
    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass
    await call.answer("Обновлено ✓")


@dp.callback_query(F.data.startswith("ptoggle:"))
async def cb_ptoggle(call: CallbackQuery):
    if call.from_user.id not in SUPER_ADMINS:
        await call.answer("❌", show_alert=True)
        return
    cid  = int(call.data.split(":")[1])
    info = await get_channel_info(cid)
    if not info:
        await call.answer("Не найден.", show_alert=True)
        return
    new   = not bool(info[3])
    await set_channel_active(cid, new)
    label = f"@{info[2]}" if info[2] else (info[1] or str(cid))
    await call.answer(f"{'🟢 Включён' if new else '🔴 Отключён'}: {label}", show_alert=True)
    text, kb = await render_panel()
    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass


@dp.callback_query(F.data.startswith("pcoadmin:"))
async def cb_pcoadmin(call: CallbackQuery):
    if call.from_user.id not in SUPER_ADMINS:
        await call.answer("❌", show_alert=True)
        return
    cid = int(call.data.split(":")[1])
    new = await toggle_co_admin(cid)
    await call.answer(f"Со-администраторы: {'✅' if new else '❌'}", show_alert=True)
    text, kb = await render_panel()
    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass


# ── Отчёт — callbacks ────────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("report_sel:"))
async def cb_report_sel(call: CallbackQuery):
    cid = int(call.data.split(":")[1])
    if not await can_access(call.from_user.id, cid):
        await call.answer("❌ Нет доступа.", show_alert=True)
        return
    info    = await get_channel_info(cid)
    display = f"@{info[2]}" if info and info[2] else (info[1] if info else str(cid))
    await call.message.edit_text(
        f"📊 <b>Excel-отчёт — {display}</b>\n\nВыберите период:",
        reply_markup=kb_report_periods(cid),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("report:"))
async def cb_report(call: CallbackQuery):
    _, ch, per = call.data.split(":")
    cid, period = int(ch), int(per)
    if not await can_access(call.from_user.id, cid):
        await call.answer("❌ Нет доступа.", show_alert=True)
        return
    info        = await get_channel_info(cid)
    display     = f"@{info[2]}" if info and info[2] else (info[1] if info else str(cid))
    period_days = period if period > 0 else None
    period_lbl  = PERIOD_LABELS.get(period, f"{period} дней")

    await call.answer("⏳ Формирую отчёт...")
    status_msg = await call.message.answer(
        "⏳ <b>Генерирую Excel-отчёт...</b>", parse_mode="HTML"
    )

    tmp_path = None
    try:
        tmp_path = await generate_report(
            db_path=DB_PATH,
            channel_id=cid,
            channel_name=display,
            period_label=period_lbl,
            period_days=period_days,
        )
        await bot.send_document(
            chat_id=call.from_user.id,
            document=FSInputFile(tmp_path),
            caption=(
                f"📊 <b>{display}</b> — отчёт за <b>{period_lbl}</b>\n"
                f"<i>🕐 {datetime.utcnow().strftime('%d.%m.%Y %H:%M')} UTC</i>"
            ),
            parse_mode="HTML",
        )
        await status_msg.edit_text("✅ <b>Отчёт готов!</b>", parse_mode="HTML")
    except Exception as e:
        logger.error(f"Report error: {e}", exc_info=True)
        await status_msg.edit_text(
            "❌ Не удалось сформировать отчёт. Попробуйте позже.", parse_mode="HTML"
        )
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)


# ══════════════════════════════════════════════════════════════════════════════
# SCHEDULER
# ══════════════════════════════════════════════════════════════════════════════

async def take_snapshots() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT channel_id FROM channels WHERE active=1") as cur:
            ids = [r[0] for r in await cur.fetchall()]
    count = 0
    for cid in ids:
        try:
            members = await bot.get_chat_member_count(cid)
            await save_snapshot(cid, members)
            count += 1
            await asyncio.sleep(0.3)
        except Exception as e:
            logger.warning(f"Snapshot {cid}: {e}")
    logger.info(f"Snapshots: {count}/{len(ids)}")
    return count


async def send_weekly_digest() -> Tuple[int, int]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """SELECT DISTINCT uc.user_id FROM user_channels uc
               JOIN channels c ON c.channel_id=uc.channel_id WHERE c.active=1"""
        ) as cur:
            user_ids = [r[0] for r in await cur.fetchall()]

    sent = failed = 0
    for uid in user_ids:
        for cid, title, uname, *_ in await get_user_channels_full(uid):
            display  = f"@{uname}" if uname else (title or str(cid))
            s        = await get_stats(cid, 7)
            text     = "📬 <b>Еженедельный дайджест</b>\n\n" + fmt_stats(display, s, "Неделя")
            owner    = await get_channel_owner(cid)
            is_owner = bool(owner and owner[0] == uid)
            try:
                await bot.send_message(
                    uid, text, parse_mode="HTML",
                    reply_markup=kb_periods(cid, 7, is_owner=is_owner),
                )
                sent += 1
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.warning(f"Digest {uid}: {e}")
                failed += 1
    return sent, failed


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN не задан!")
    if not SUPER_ADMINS:
        logger.warning("SUPER_ADMINS пуст — никто не сможет использовать команды суперадмина!")

    await init_db()

    scheduler.add_job(take_snapshots,     "interval", hours=1,       id="hourly")
    scheduler.add_job(send_weekly_digest, "cron", day_of_week="mon",
                      hour=9, minute=0,   id="weekly")
    scheduler.start()

    asyncio.create_task(take_snapshots())

    logger.info(f"Бот запущен. Суперадмины: {SUPER_ADMINS}")
    await dp.start_polling(
        bot,
        allowed_updates=["message", "callback_query", "channel_post"],
    )


if __name__ == "__main__":
    asyncio.run(main())
