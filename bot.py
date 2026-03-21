# версия v1.4
#!/usr/bin/env python3
"""
Channel Analytics Bot
Тарифы / Inline UI / AI-анализ / Сравнение периодов / Сводный отчёт
"""

import asyncio
import functools
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Optional, List, Tuple

import aiosqlite
import httpx
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery, FSInputFile, LabeledPrice,
    InlineKeyboardButton, InlineKeyboardMarkup, Message,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

from report_generator import generate_report
from mediakit_generator import generate_mediakit

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

BOT_TOKEN          = os.getenv("BOT_TOKEN", "")
SUPER_ADMINS       = [int(x) for x in os.getenv("SUPER_ADMINS", "").split(",") if x.strip()]
DB_PATH            = os.getenv("DB_PATH", "/data/analytics.db")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
AI_MODEL           = os.getenv("AI_MODEL", "meta-llama/llama-3.1-8b-instruct:free")
SUPPORT_USERNAME   = os.getenv("SUPPORT_USERNAME", "")  # для тарифа Бизнес

bot       = Bot(token=BOT_TOKEN)
dp        = Dispatcher(storage=MemoryStorage())
scheduler = AsyncIOScheduler(timezone="Europe/Moscow")

MSK_OFFSET = timedelta(hours=3)

def now_msk() -> datetime:
    return datetime.utcnow() + MSK_OFFSET

def fmt_msk(dt: datetime) -> str:
    return (dt + MSK_OFFSET).strftime("%d.%m.%Y %H:%M")


# ══════════════════════════════════════════════════════════════════════════════
# ТАРИФЫ
# ══════════════════════════════════════════════════════════════════════════════

PLANS = {
    "basic": {
        "name": "Базовый", "emoji": "🔹",
        "channels": 1,
        "price_month": 0, "price_year": 0,
        "periods": [1, 7, 30],
        "excel": False, "coadmin": False, "compare": False,
        "ai": False, "summary": False, "top_posts": False,
    },
    "pro": {
        "name": "Про", "emoji": "🔷",
        "channels": 4,
        "price_month": 699, "price_year": 6710,
        "periods": [1, 7, 30, 0],
        "excel": True, "coadmin": True, "compare": True,
        "ai": False, "summary": False, "top_posts": True,
    },
    "business": {
        "name": "Бизнес", "emoji": "💎",
        "channels": 15,
        "price_month": 1490, "price_year": 14300,
        "periods": [1, 7, 30, 0],
        "excel": True, "coadmin": True, "compare": True,
        "ai": True, "summary": True, "top_posts": True,
    },
    "agency": {
        "name": "Агентство", "emoji": "👑",
        "channels": 99999,
        "price_month": 2990, "price_year": 28700,
        "periods": [1, 7, 30, 0],
        "excel": True, "coadmin": True, "compare": True,
        "ai": True, "summary": True, "top_posts": True,
    },
}

TRIAL_PLAN = "pro"
TRIAL_DAYS = 7
PERIOD_LABELS = {1: "День", 7: "Неделя", 30: "Месяц", 0: "Всё время"}

# Цены в Telegram Stars (1 Star ≈ 0.013 USD)
STARS_PRICES = {
    "pro":      {"month": 550,   "year": 4900},
    "business": {"month": 1200,  "year": 10800},
    "agency":   {"month": 2400,  "year": 21600},
}


def plan_cfg(plan: str) -> dict:
    return PLANS.get(plan, PLANS["basic"])


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
        CREATE TABLE IF NOT EXISTS subscriptions (
            user_id     INTEGER PRIMARY KEY,
            plan        TEXT DEFAULT 'basic',
            expires_at  TIMESTAMP,
            trial_used  INTEGER DEFAULT 0,
            referred_by INTEGER,
            ref_code    TEXT UNIQUE,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS referrals (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            referrer_id INTEGER NOT NULL,
            referred_id INTEGER NOT NULL,
            bonus_days  INTEGER DEFAULT 10,
            credited    INTEGER DEFAULT 0,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(referred_id)
        );
        CREATE INDEX IF NOT EXISTS idx_snap ON snapshots(channel_id, taken_at);
        CREATE INDEX IF NOT EXISTS idx_posts ON posts(channel_id, posted_at);
        CREATE TABLE IF NOT EXISTS user_settings (
            user_id         INTEGER PRIMARY KEY,
            daily_digest    INTEGER DEFAULT 0,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        for sql in [
            "ALTER TABLE channels ADD COLUMN active INTEGER DEFAULT 1",
            "ALTER TABLE channels ADD COLUMN co_admin_access INTEGER DEFAULT 0",
            "ALTER TABLE user_channels ADD COLUMN is_owner INTEGER DEFAULT 1",
            "ALTER TABLE user_channels ADD COLUMN username TEXT",
            "ALTER TABLE user_channels ADD COLUMN first_name TEXT",
            "ALTER TABLE subscriptions ADD COLUMN referred_by INTEGER",
            "ALTER TABLE subscriptions ADD COLUMN ref_code TEXT",
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
    user_id: int, channel_id: int, is_owner: bool = True,
    username: Optional[str] = None, first_name: Optional[str] = None,
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


# ── Подписки ─────────────────────────────────────────────────────────────────

async def get_user_plan(user_id: int) -> Tuple[str, Optional[datetime], bool]:
    """Возвращает (plan, expires_at, trial_used). Суперадмины → agency навсегда."""
    if user_id in SUPER_ADMINS:
        return "agency", None, True

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT plan, expires_at, trial_used FROM subscriptions WHERE user_id=?",
            (user_id,),
        ) as cur:
            row = await cur.fetchone()

    if not row:
        return "basic", None, False

    plan, expires_str, trial_used = row
    if expires_str:
        exp = datetime.fromisoformat(expires_str)
        if exp < datetime.utcnow():
            return "basic", None, bool(trial_used)
    exp_dt = datetime.fromisoformat(expires_str) if expires_str else None
    return plan, exp_dt, bool(trial_used)


async def set_user_plan(user_id: int, plan: str, days: int):
    expires = (datetime.utcnow() + timedelta(days=days)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO subscriptions (user_id, plan, expires_at)
               VALUES (?, ?, ?)
               ON CONFLICT(user_id) DO UPDATE
               SET plan=excluded.plan, expires_at=excluded.expires_at""",
            (user_id, plan, expires),
        )
        await db.commit()


async def activate_trial(user_id: int) -> bool:
    """Активирует пробный период. Возвращает True если успешно."""
    _, _, trial_used = await get_user_plan(user_id)
    if trial_used:
        return False
    expires = (datetime.utcnow() + timedelta(days=TRIAL_DAYS)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO subscriptions (user_id, plan, expires_at, trial_used)
               VALUES (?, ?, ?, 1)
               ON CONFLICT(user_id) DO UPDATE
               SET plan=excluded.plan, expires_at=excluded.expires_at, trial_used=1""",
            (user_id, TRIAL_PLAN, expires),
        )
        await db.commit()
    return True


async def has_feature(user_id: int, feature: str) -> bool:
    plan, _, _ = await get_user_plan(user_id)
    return plan_cfg(plan).get(feature, False)


async def channel_limit_reached(user_id: int) -> bool:
    plan, _, _ = await get_user_plan(user_id)
    limit = plan_cfg(plan)["channels"]
    count = len(await get_user_channels_ids(user_id))
    return count >= limit


# ── Реферальная система ──────────────────────────────────────────────────────

import hashlib

def _make_ref_code(user_id: int) -> str:
    return hashlib.md5(f"ref_{user_id}_analytic".encode()).hexdigest()[:8].upper()


async def get_or_create_ref_code(user_id: int) -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT ref_code FROM subscriptions WHERE user_id=?", (user_id,)
        ) as cur:
            row = await cur.fetchone()
        if row and row[0]:
            return row[0]
        code = _make_ref_code(user_id)
        await db.execute(
            """INSERT INTO subscriptions (user_id, ref_code) VALUES (?, ?)
               ON CONFLICT(user_id) DO UPDATE SET ref_code=excluded.ref_code""",
            (user_id, code),
        )
        await db.commit()
    return code


async def get_referral_stats(user_id: int) -> dict:
    """Возвращает статистику рефералов пользователя."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*), SUM(credited), SUM(bonus_days) FROM referrals WHERE referrer_id=?",
            (user_id,),
        ) as cur:
            row = await cur.fetchone()
            total      = row[0] or 0
            credited   = row[1] or 0
            bonus_days = row[2] or 0
    return {"total": total, "credited": credited, "bonus_days": bonus_days}


async def process_referral(new_user_id: int, ref_code: str) -> bool:
    """Обрабатывает переход по реферальной ссылке. Возвращает True если засчитан."""
    # Найти владельца кода
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT user_id FROM subscriptions WHERE ref_code=?", (ref_code,)
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return False
        referrer_id = row[0]
        if referrer_id == new_user_id:
            return False

        # Проверить что этот пользователь ещё не был реферальным
        async with db.execute(
            "SELECT id FROM referrals WHERE referred_id=?", (new_user_id,)
        ) as cur:
            if await cur.fetchone():
                return False

        # Сохранить реферала
        await db.execute(
            """INSERT OR IGNORE INTO referrals (referrer_id, referred_id)
               VALUES (?, ?)""",
            (referrer_id, new_user_id),
        )
        # Сохранить referred_by у нового пользователя
        await db.execute(
            """INSERT INTO subscriptions (user_id, referred_by) VALUES (?, ?)
               ON CONFLICT(user_id) DO UPDATE SET referred_by=excluded.referred_by""",
            (new_user_id, referrer_id),
        )
        await db.commit()
    return True


async def credit_referral_bonus(referred_id: int):
    """
    Начисляет бонус рефереру когда новый пользователь активирует пробный период
    или покупает тариф. Вызывается однократно.
    """
    BONUS_DAYS = 10
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT referrer_id, credited FROM referrals WHERE referred_id=?",
            (referred_id,),
        ) as cur:
            row = await cur.fetchone()
        if not row or row[1]:  # уже начислено
            return
        referrer_id = row[0]

        # Продлеваем тариф рефереру
        async with db.execute(
            "SELECT plan, expires_at FROM subscriptions WHERE user_id=?", (referrer_id,)
        ) as cur:
            sub = await cur.fetchone()

        if sub and sub[1]:
            base = datetime.fromisoformat(sub[1])
            if base < datetime.utcnow():
                base = datetime.utcnow()
        else:
            base = datetime.utcnow()

        new_exp = (base + timedelta(days=BONUS_DAYS)).isoformat()
        plan = sub[0] if sub else "basic"

        await db.execute(
            """INSERT INTO subscriptions (user_id, plan, expires_at) VALUES (?, ?, ?)
               ON CONFLICT(user_id) DO UPDATE SET expires_at=excluded.expires_at""",
            (referrer_id, plan, new_exp),
        )
        await db.execute(
            "UPDATE referrals SET credited=1 WHERE referred_id=?", (referred_id,)
        )
        await db.commit()

    # Уведомляем реферера
    try:
        await bot.send_message(
            referrer_id,
            f"🎉 <b>Реферальный бонус!</b>\n\n"
            f"Приглашённый вами пользователь активировал подписку.\n"
            f"Вам начислено <b>+{BONUS_DAYS} дней</b> к текущему тарифу!",
            parse_mode="HTML",
        )
    except Exception:
        pass


# ── Настройки пользователя ───────────────────────────────────────────────────

async def get_daily_digest(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT daily_digest FROM user_settings WHERE user_id=?", (user_id,)
        ) as cur:
            row = await cur.fetchone()
    return bool(row[0]) if row else False


async def toggle_daily_digest(user_id: int) -> bool:
    current = await get_daily_digest(user_id)
    new_val = 0 if current else 1
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO user_settings (user_id, daily_digest) VALUES (?, ?)
               ON CONFLICT(user_id) DO UPDATE SET daily_digest=excluded.daily_digest""",
            (user_id, new_val),
        )
        await db.commit()
    return bool(new_val)


async def get_daily_digest_users() -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT user_id FROM user_settings WHERE daily_digest=1"
        ) as cur:
            return [r[0] for r in await cur.fetchall()]


# ── Снимки / Посты ───────────────────────────────────────────────────────────

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

async def _stats_query(db, channel_id: int, since: str, until: str) -> dict:
    async with db.execute(
        "SELECT members FROM snapshots WHERE channel_id=? AND taken_at<=? ORDER BY taken_at DESC LIMIT 1",
        (channel_id, until),
    ) as cur:
        row = await cur.fetchone()
        current = row[0] if row else None

    async with db.execute(
        "SELECT members FROM snapshots WHERE channel_id=? AND taken_at>=? AND taken_at<=? ORDER BY taken_at ASC LIMIT 1",
        (channel_id, since, until),
    ) as cur:
        row = await cur.fetchone()
        start = row[0] if row else None

    async with db.execute(
        "SELECT MAX(members), MIN(members) FROM snapshots WHERE channel_id=? AND taken_at>=? AND taken_at<=?",
        (channel_id, since, until),
    ) as cur:
        row = await cur.fetchone()
        peak, low = (row[0], row[1]) if row else (None, None)

    async with db.execute(
        "SELECT COUNT(*), SUM(has_media) FROM posts WHERE channel_id=? AND posted_at>=? AND posted_at<=?",
        (channel_id, since, until),
    ) as cur:
        row = await cur.fetchone()
        posts_total, media_total = (row[0] or 0, row[1] or 0)

    async with db.execute(
        "SELECT SUM(views), MAX(views), AVG(views) FROM posts WHERE channel_id=? AND posted_at>=? AND posted_at<=?",
        (channel_id, since, until),
    ) as cur:
        row = await cur.fetchone()
        views_sum = int(row[0] or 0)
        views_max = int(row[1] or 0)
        views_avg = round(row[2] or 0)

    async with db.execute(
        """SELECT DATE(taken_at), MAX(members)-MIN(members)
           FROM snapshots WHERE channel_id=? AND taken_at>=? AND taken_at<=?
           GROUP BY DATE(taken_at) ORDER BY 2 DESC LIMIT 1""",
        (channel_id, since, until),
    ) as cur:
        best_day = await cur.fetchone()

    async with db.execute(
        """SELECT DATE(taken_at), MAX(members)-MIN(members)
           FROM snapshots WHERE channel_id=? AND taken_at>=? AND taken_at<=?
           GROUP BY DATE(taken_at) ORDER BY 2 ASC LIMIT 1""",
        (channel_id, since, until),
    ) as cur:
        worst_day = await cur.fetchone()

    async with db.execute(
        """SELECT CAST(strftime('%H', posted_at) AS INTEGER), COUNT(*)
           FROM posts WHERE channel_id=? AND posted_at>=? AND posted_at<=?
           GROUP BY 1 ORDER BY 2 DESC LIMIT 1""",
        (channel_id, since, until),
    ) as cur:
        best_hour = await cur.fetchone()

    async with db.execute(
        "SELECT COUNT(DISTINCT DATE(posted_at)) FROM posts WHERE channel_id=? AND posted_at>=? AND posted_at<=?",
        (channel_id, since, until),
    ) as cur:
        row = await cur.fetchone()
        active_days = row[0] if row else 0

    async with db.execute(
        "SELECT COUNT(*) FROM snapshots WHERE channel_id=? AND taken_at>=? AND taken_at<=?",
        (channel_id, since, until),
    ) as cur:
        row = await cur.fetchone()
        snap_count = row[0] if row else 0

    growth = (current - start) if (current is not None and start is not None) else None
    growth_pct = None
    if growth is not None and start and start > 0:
        growth_pct = round(growth / start * 100, 2)

    return dict(
        current=current, start=start, growth=growth, growth_pct=growth_pct,
        peak=peak, low=low, posts_total=posts_total, media_total=media_total,
        views_sum=views_sum, views_max=views_max, views_avg=views_avg,
        best_day=best_day, worst_day=worst_day, best_hour=best_hour,
        active_days=active_days, snap_count=snap_count,
    )


async def get_stats(channel_id: int, period_days: Optional[int]) -> dict:
    now   = datetime.utcnow()
    since = (now - timedelta(days=period_days)).isoformat() if period_days else "2000-01-01"
    until = now.isoformat()

    async with aiosqlite.connect(DB_PATH) as db:
        s = await _stats_query(db, channel_id, since, until)

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT DISTINCT DATE(posted_at) FROM posts WHERE channel_id=? ORDER BY 1 DESC",
            (channel_id,),
        ) as cur:
            streak_dates = [r[0] for r in await cur.fetchall()]

    streak = 0
    today = now.date()
    for i, d in enumerate(streak_dates):
        if d == (today - timedelta(days=i)).isoformat():
            streak += 1
        else:
            break

    s["streak"]     = streak
    s["period_days"] = period_days
    return s


async def get_comparison(channel_id: int, window_days: int) -> Tuple[dict, dict]:
    """Возвращает (текущий период, предыдущий период)."""
    now  = datetime.utcnow()
    cur_since  = (now - timedelta(days=window_days)).isoformat()
    prev_since = (now - timedelta(days=window_days * 2)).isoformat()
    prev_until = cur_since

    async with aiosqlite.connect(DB_PATH) as db:
        current  = await _stats_query(db, channel_id, cur_since, now.isoformat())
        previous = await _stats_query(db, channel_id, prev_since, prev_until)

    current["period_days"]  = window_days
    previous["period_days"] = window_days
    return current, previous


async def get_top_posts(channel_id: int, period_days: Optional[int], limit: int = 5) -> List[Tuple]:
    since = (
        (datetime.utcnow() - timedelta(days=period_days)).isoformat()
        if period_days else "2000-01-01"
    )
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """SELECT message_id, views, has_media, posted_at
               FROM posts WHERE channel_id=? AND posted_at>=?
               ORDER BY views DESC LIMIT ?""",
            (channel_id, since, limit),
        ) as cur:
            return await cur.fetchall()


# ══════════════════════════════════════════════════════════════════════════════
# AI АНАЛИЗ
# ══════════════════════════════════════════════════════════════════════════════

async def ai_analyze(channel_name: str, s: dict, period_label: str) -> str:
    if not OPENROUTER_API_KEY:
        return "⚠️ AI-анализ не настроен. Добавьте OPENROUTER_API_KEY в переменные окружения."

    growth_str = f"{'+' if (s['growth'] or 0) >= 0 else ''}{s['growth'] or 0}"
    prompt = f"""Ты аналитик Telegram-каналов. Проанализируй данные и дай краткие конкретные рекомендации на русском языке.

Канал: {channel_name}
Период: {period_label}

Данные:
- Подписчиков сейчас: {s['current'] or 'нет данных'}
- Изменение за период: {growth_str} ({s['growth_pct'] or 0}%)
- Постов опубликовано: {s['posts_total']}
- Постов с медиа: {s['media_total']} ({round(s['media_total']/s['posts_total']*100) if s['posts_total'] else 0}%)
- Суммарно просмотров: {s['views_sum']:,}
- Среднее просмотров на пост: {s['views_avg']:,}
- Лучший пост: {s['views_max']:,} просмотров
- Активных дней: {s['active_days']}
- Серия публикаций: {s.get('streak', 0)} дней
- Лучший день роста: {s['best_day'][0] if s['best_day'] and s['best_day'][1] else 'нет данных'} (+{s['best_day'][1] if s['best_day'] else 0} подп.)
- Лучшее время постов: {f"{(s['best_hour'][0]+3)%24:02d}:00 МСК" if s['best_hour'] else 'нет данных'}

Напиши анализ в формате:
1. Одна строка — общий вывод о состоянии канала
2. 2-3 конкретных наблюдения из данных
3. 2-3 конкретных рекомендации что улучшить

Отвечай кратко, по делу, без воды. Максимум 200 слов."""

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://t.me/whanalyticbot",
                    "X-Title": "WH Analytics Bot",
                },
                json={
                    "model": AI_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 500,
                    "temperature": 0.7,
                },
            )
            logger.info(f"OpenRouter status: {resp.status_code}")
            data = resp.json()
            logger.info(f"OpenRouter response keys: {list(data.keys())}")

            if "choices" not in data:
                err  = data.get("error", {})
                msg  = err.get("message", str(data)) if isinstance(err, dict) else str(err)
                code = err.get("code", "") if isinstance(err, dict) else ""
                logger.error(f"OpenRouter full response: {data}")
                return (
                    f"❌ Ошибка AI ({code}): {msg[:300]}\n\n"
                    f"<i>Модель: <code>{AI_MODEL}</code>\n"
                    f"HTTP статус: {resp.status_code}</i>"
                )
            return data["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logger.error(f"OpenRouter exception: {e}", exc_info=True)
        return f"❌ Ошибка при обращении к AI: {str(e)[:200]}"


# ══════════════════════════════════════════════════════════════════════════════
# ФОРМАТИРОВАНИЕ
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
    if s.get("streak", 0) > 1:
        lines.append(f"🔥 Серия: <b>{s['streak']} дней подряд</b>")

    lines.append("")
    if s["views_sum"]:
        lines.append(f"👁 Просмотров: <b>{s['views_sum']:,}</b>")
    if s["views_avg"]:
        lines.append(f"📊 Среднее: <b>{s['views_avg']:,}</b>")
    if s["views_max"]:
        lines.append(f"🚀 Лучший пост: <b>{s['views_max']:,}</b> просм.")

    lines.append("")
    if s["best_day"] and s["best_day"][1] and s["best_day"][1] > 0:
        lines.append(f"🌟 Лучший день: <b>{s['best_day'][0]}</b> (+{s['best_day'][1]})")
    if s["worst_day"] and s["worst_day"][1] and s["worst_day"][1] < 0:
        lines.append(f"📉 Худший день: <b>{s['worst_day'][0]}</b> ({s['worst_day'][1]})")
    if s["best_hour"] is not None:
        h = s["best_hour"][0]
        lines.append(f"⏰ Лучшее время: <b>{(h+3)%24:02d}:00–{(h+3)%24:02d}:59 МСК</b>")

    if s["snap_count"] == 0:
        lines.append("\n⚠️ <i>Данных пока нет — первые появятся через час.</i>")

    lines.append(f"\n<i>🕐 {fmt_msk(datetime.utcnow())} МСК</i>")
    return "\n".join(lines)


def fmt_comparison(display: str, cur: dict, prev: dict, window_days: int) -> str:
    label = "неделю" if window_days == 7 else "месяц"
    lines = [
        f"📊 <b>{display}</b>",
        f"🔄 Сравнение: эта {label} vs прошлая {label}",
        "",
    ]

    def delta(a, b, fmt=","):
        if a is None or b is None:
            return "—"
        diff = a - b
        sign = "+" if diff >= 0 else ""
        return f"{sign}{diff:{fmt}}"

    def row(label, a, b, key):
        va = a.get(key) or 0
        vb = b.get(key) or 0
        d  = delta(va, vb)
        arrow = "▲" if (va > vb) else ("▼" if va < vb else "→")
        return f"{label}: <b>{va:,}</b>  {arrow} {d}  <i>(было {vb:,})</i>"

    if cur["current"] is not None:
        lines.append(row("👥 Подписчики", cur, prev, "current"))
    lines.append(row("📈 Прирост", cur, prev, "growth"))
    lines.append(row("✍️ Постов", cur, prev, "posts_total"))
    lines.append(row("👁 Просмотров", cur, prev, "views_sum"))
    lines.append(row("📊 Ср. просм./пост", cur, prev, "views_avg"))

    lines.append(f"\n<i>🕐 {fmt_msk(datetime.utcnow())} МСК</i>")
    return "\n".join(lines)


async def fmt_summary(user_id: int) -> str:
    channels = await get_user_channels_full(user_id)
    if not channels:
        return "У вас нет активных каналов."

    lines = [
        "📋 <b>Сводный отчёт по всем каналам</b>",
        f"⏱ За последние 7 дней\n",
    ]

    for cid, title, uname, *_ in channels:
        display = f"@{uname}" if uname else (title or str(cid))
        s = await get_stats(cid, 7)
        sign  = "+" if (s["growth"] or 0) >= 0 else ""
        arrow = "📈" if (s["growth"] or 0) >= 0 else "📉"
        lines.append(f"<b>{display}</b>")
        if s["current"] is not None:
            lines.append(f"  👥 {s['current']:,}  {arrow} {sign}{s['growth'] or 0}")
        lines.append(f"  ✍️ {s['posts_total']} постов   👁 {s['views_sum']:,} просм.")
        lines.append("")

    lines.append(f"<i>🕐 {fmt_msk(datetime.utcnow())} МСК</i>")
    return "\n".join(lines)


async def fmt_cabinet(user_id: int) -> str:
    plan, expires, trial_used = await get_user_plan(user_id)
    cfg     = plan_cfg(plan)
    ref_stats = await get_referral_stats(user_id)
    ref_code  = await get_or_create_ref_code(user_id)
    bot_info  = await bot.get_me()
    ref_link  = f"https://t.me/{bot_info.username}?start=ref_{ref_code}"

    # Блок подписки
    now = datetime.utcnow()
    if expires:
        delta     = expires - now
        days_left = max(delta.days, 0)
        hrs_left  = max(int(delta.total_seconds() // 3600), 0)
        exp_str   = (expires + MSK_OFFSET).strftime("%d.%m.%Y")

        if days_left == 0:
            time_str = f"⚠️ Истекает сегодня (через {hrs_left}ч)"
        elif days_left <= 3:
            time_str = f"⚠️ Осталось <b>{days_left} дн.</b> (до {exp_str})"
        elif days_left <= 14:
            time_str = f"🕐 Осталось <b>{days_left} дн.</b> (до {exp_str})"
        else:
            time_str = f"✅ Активен до <b>{exp_str}</b> ({days_left} дн.)"
    else:
        time_str = "✅ Бессрочно" if user_id in SUPER_ADMINS else "— (базовый бесплатный)"

    channels_count = len(await get_user_channels_ids(user_id))
    ch_limit = "∞" if cfg["channels"] > 1000 else str(cfg["channels"])

    lines = [
        "👤 <b>Личный кабинет</b>",
        "",
        "━━━━━━━━━━━━━━━━━",
        f"<b>Тариф:</b> {cfg['emoji']} {cfg['name']}",
        f"<b>Статус:</b> {time_str}",
        f"<b>Каналов:</b> {channels_count} / {ch_limit}",
        "━━━━━━━━━━━━━━━━━",
        "",
        "<b>Возможности тарифа:</b>",
    ]

    features = [
        ("excel",     "📊 Excel-отчёты"),
        ("coadmin",   "👥 Со-администраторы"),
        ("compare",   "🔄 Сравнение периодов"),
        ("top_posts", "🏆 Топ постов"),
        ("ai",        "🤖 AI-анализ"),
        ("summary",   "📋 Сводный отчёт"),
    ]
    for key, label in features:
        icon = "✅" if cfg.get(key) else "❌"
        lines.append(f"  {icon} {label}")

    lines += [
        "",
        "━━━━━━━━━━━━━━━━━",
        "🤝 <b>Реферальная программа</b>",
        f"Приглашено друзей: <b>{ref_stats['total']}</b>",
        f"Засчитано бонусов: <b>{ref_stats['credited']}</b>",
        f"Бонусных дней получено: <b>{ref_stats['bonus_days']}</b>",
        "",
        "За каждого приглашённого кто <b>оплатит</b> любой тариф — <b>+10 дней</b> к вашему тарифу.",
        "",
        f"Ваша ссылка: <code>{ref_link}</code>",
        "━━━━━━━━━━━━━━━━━",
    ]

    if not trial_used and user_id not in SUPER_ADMINS:
        lines += ["", f"🎁 У вас есть <b>{TRIAL_DAYS} дней Про</b> бесплатно — активируйте ниже!"]

    return "\n".join(lines)


def fmt_plans_list(current_plan: str) -> str:
    lines = ["💳 <b>Доступные тарифы</b>", ""]
    for pkey, pcfg in PLANS.items():
        mark = "  ◀ текущий" if pkey == current_plan else ""
        ch   = "∞" if pcfg["channels"] > 1000 else str(pcfg["channels"])
        lines.append(f"{pcfg['emoji']} <b>{pcfg['name']}</b>{mark}")
        lines.append(f"  • Каналов: {ch}")
        lines.append(f"  • {pcfg['price_month']}₽/мес  |  {pcfg['price_year']}₽/год (−20%)")
        lines.append("")
    lines.append("Для смены тарифа обратитесь к администратору.")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# КЛАВИАТУРЫ
# ══════════════════════════════════════════════════════════════════════════════

async def kb_main_menu(user_id: int) -> InlineKeyboardMarkup:
    plan, expires, _ = await get_user_plan(user_id)
    # Показываем дату окончания прямо в кнопке если тариф платный
    plan_btn_text = "👤 Личный кабинет"
    if expires:
        days_left = (expires - datetime.utcnow()).days
        if days_left <= 3:
            plan_btn_text = f"👤 Кабинет  ⚠️ {days_left}д"
        else:
            plan_btn_text = f"👤 Кабинет  ({days_left}д)"

    rows = [
        [
            InlineKeyboardButton(text="📢 Мои каналы",  callback_data="menu:channels"),
            InlineKeyboardButton(text=plan_btn_text,    callback_data="menu:cabinet"),
        ],
    ]
    if plan_cfg(plan)["summary"]:
        rows.append([
            InlineKeyboardButton(text="📋 Сводный отчёт", callback_data="menu:summary"),
        ])
    rows.append([
        InlineKeyboardButton(text="⚙️ Настройки", callback_data="menu:settings"),
        InlineKeyboardButton(text="❓ Помощь",    callback_data="menu:help"),
    ])
    if user_id in SUPER_ADMINS:
        rows.append([
            InlineKeyboardButton(text="🛠 Панель суперадмина", callback_data="admin:panel"),
        ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def kb_channel_menu(cid: int, user_id: int) -> InlineKeyboardMarkup:
    plan, _, _ = await get_user_plan(user_id)
    pcfg = plan_cfg(plan)
    owner = await get_channel_owner(cid)
    is_owner = bool(owner and owner[0] == user_id) or (user_id in SUPER_ADMINS)

    rows = [
        [
            InlineKeyboardButton(text="📈 Статистика",   callback_data=f"stats_menu:{cid}"),
            InlineKeyboardButton(text="🔄 Сравнить",     callback_data=f"compare_menu:{cid}") if pcfg["compare"] else
            InlineKeyboardButton(text="🔄 Сравнить 🔒",  callback_data="plan:upgrade:compare"),
        ],
        [
            InlineKeyboardButton(text="📥 Excel-отчёт",  callback_data=f"report_sel:{cid}") if pcfg["excel"] else
            InlineKeyboardButton(text="📥 Excel 🔒",     callback_data="plan:upgrade:excel"),
            InlineKeyboardButton(text="🤖 AI-анализ",    callback_data=f"ai_menu:{cid}") if pcfg["ai"] else
            InlineKeyboardButton(text="🤖 AI-анализ 🔒", callback_data="plan:upgrade:ai"),
        ],
        [
            InlineKeyboardButton(text="🏆 Топ постов",   callback_data=f"top_posts:{cid}:7") if pcfg["top_posts"] else
            InlineKeyboardButton(text="🏆 Топ постов 🔒",callback_data="plan:upgrade:top_posts"),
            InlineKeyboardButton(text="📄 Медиакит PDF", callback_data=f"mediakit:{cid}") if pcfg["excel"] else
            InlineKeyboardButton(text="📄 Медиакит 🔒",  callback_data="plan:upgrade:excel"),
        ],
    ]
    if is_owner:
        rows.append([
            InlineKeyboardButton(text="⚙️ Настройки канала", callback_data=f"ch_settings:{cid}"),
        ])
    rows.append([
        InlineKeyboardButton(text="◀ Назад к каналам", callback_data="menu:channels"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_stats_periods(cid: int, active: int, plan: str, is_owner: bool = False) -> InlineKeyboardMarkup:
    periods = plan_cfg(plan)["periods"]
    labels  = {1: "День", 7: "Неделя", 30: "Месяц", 0: "Всё время"}

    def b(val):
        lbl = labels[val]
        if val not in periods:
            return InlineKeyboardButton(text=f"{lbl} 🔒", callback_data="plan:upgrade:periods")
        return InlineKeyboardButton(
            text=f"• {lbl}" if val == active else lbl,
            callback_data=f"stats:{cid}:{val}",
        )

    rows = [
        [b(1), b(7), b(30), b(0)],
        [
            InlineKeyboardButton(text="🔄 Обновить", callback_data=f"stats:{cid}:{active}"),
            InlineKeyboardButton(text="◀ Назад",     callback_data=f"ch_menu:{cid}"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_compare_periods(cid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📅 Эта неделя vs прошлая",  callback_data=f"compare:{cid}:7"),
            InlineKeyboardButton(text="📅 Этот месяц vs прошлый",  callback_data=f"compare:{cid}:30"),
        ],
        [InlineKeyboardButton(text="◀ Назад", callback_data=f"ch_menu:{cid}")],
    ])


def kb_ai_periods(cid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📅 Неделя",  callback_data=f"ai:{cid}:7"),
            InlineKeyboardButton(text="📅 Месяц",   callback_data=f"ai:{cid}:30"),
            InlineKeyboardButton(text="📅 Всё время", callback_data=f"ai:{cid}:0"),
        ],
        [InlineKeyboardButton(text="◀ Назад", callback_data=f"ch_menu:{cid}")],
    ])


def kb_top_posts_periods(cid: int, active: int) -> InlineKeyboardMarkup:
    def b(val, lbl):
        return InlineKeyboardButton(
            text=f"• {lbl}" if val == active else lbl,
            callback_data=f"top_posts:{cid}:{val}",
        )
    return InlineKeyboardMarkup(inline_keyboard=[
        [b(7, "Неделя"), b(30, "Месяц"), b(0, "Всё время")],
        [InlineKeyboardButton(text="◀ Назад", callback_data=f"ch_menu:{cid}")],
    ])


def kb_cabinet(trial_used: bool, plan: str) -> InlineKeyboardMarkup:
    rows = []
    if not trial_used:
        rows.append([InlineKeyboardButton(
            text=f"🎁 Активировать {TRIAL_DAYS} дней Про бесплатно",
            callback_data="plan:trial",
        )])
    rows.append([InlineKeyboardButton(
        text="⭐ Купить тариф за Telegram Stars",
        callback_data="stars:menu",
    )])
    rows.append([InlineKeyboardButton(
        text="💳 Все тарифы и цены",
        callback_data="menu:plans",
    )])
    rows.append([InlineKeyboardButton(
        text="🔗 Скопировать реф. ссылку",
        callback_data="ref:copy",
    )])
    rows.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="menu:cabinet")])
    rows.append([InlineKeyboardButton(text="◀ Главное меню", callback_data="menu:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_stars_menu() -> InlineKeyboardMarkup:
    rows = []
    for plan_key in ["pro", "business", "agency"]:
        pcfg   = plan_cfg(plan_key)
        pm     = STARS_PRICES[plan_key]["month"]
        py     = STARS_PRICES[plan_key]["year"]
        rows.append([
            InlineKeyboardButton(
                text=f"{pcfg['emoji']} {pcfg['name']} — {pm}⭐/мес",
                callback_data=f"stars:buy:{plan_key}:30",
            ),
            InlineKeyboardButton(
                text=f"{py}⭐/год",
                callback_data=f"stars:buy:{plan_key}:365",
            ),
        ])
    rows.append([InlineKeyboardButton(text="◀ Назад", callback_data="menu:cabinet")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_ch_settings(cid: int, co_admin: bool, is_owner: bool) -> InlineKeyboardMarkup:
    rows = []
    if is_owner:
        co_text = f"👥 Со-администраторы: {'ВКЛ ✅' if co_admin else 'ВЫКЛ ❌'}"
        rows.append([InlineKeyboardButton(text=co_text, callback_data=f"coadmin:{cid}")])
    rows.append([InlineKeyboardButton(text="◀ Назад к каналу", callback_data=f"ch_menu:{cid}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_main_settings(daily_on: bool) -> InlineKeyboardMarkup:
    daily_text = f"📬 Ежедневная сводка: {'ВКЛ ✅' if daily_on else 'ВЫКЛ ❌'}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=daily_text, callback_data="settings:toggle_daily")],
        [InlineKeyboardButton(text="◀ Назад", callback_data="menu:main")],
    ])


def kb_report_periods(cid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📅 День",       callback_data=f"report:{cid}:1"),
        InlineKeyboardButton(text="📅 Неделя",     callback_data=f"report:{cid}:7"),
        InlineKeyboardButton(text="📅 Месяц",      callback_data=f"report:{cid}:30"),
        InlineKeyboardButton(text="📅 Всё время",  callback_data=f"report:{cid}:0"),
    ], [InlineKeyboardButton(text="◀ Назад", callback_data=f"ch_menu:{cid}")]])


async def render_admin_panel() -> Tuple[str, InlineKeyboardMarkup]:
    rows_data = await get_all_channels_panel()
    if not rows_data:
        return (
            "📋 <b>Панель управления</b>\n\nКаналов нет.\n\n"
            "<code>/adduser &lt;user_id&gt; &lt;channel_id&gt;</code>",
            InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="◀ Главное меню", callback_data="menu:main")
            ]]),
        )

    active_n   = sum(1 for r in rows_data if r[3])
    inactive_n = len(rows_data) - active_n
    lines      = [
        "📋 <b>Панель управления каналами</b>",
        f"🟢 Активных: <b>{active_n}</b>   🔴 Отключённых: <b>{inactive_n}</b>",
        "",
    ]
    btns = []

    for cid, title, uname, active, co_admin, oid, ouname, ofname in rows_data:
        status  = "🟢" if active else "🔴"
        label   = f"@{uname}" if uname else (title or str(cid))
        co_icon = " 👥" if co_admin else ""
        owner_str = (f"@{ouname}" if ouname else (ofname if ofname else (f"id:{oid}" if oid else "—")))
        lines += [f"{status} <b>{label}</b>{co_icon}", f"     <code>{cid}</code>   👤 {owner_str}", ""]
        btns.append([
            InlineKeyboardButton(text=f"📊 {label[:18]}", callback_data=f"ch_menu:{cid}"),
            InlineKeyboardButton(text="🔴 Откл" if active else "🟢 Вкл", callback_data=f"ptoggle:{cid}"),
            InlineKeyboardButton(text="👥✅" if co_admin else "👥❌",     callback_data=f"pcoadmin:{cid}"),
        ])

    btns.append([
        InlineKeyboardButton(text="🔄 Обновить",   callback_data="admin:panel"),
        InlineKeyboardButton(text="◀ Главное меню", callback_data="menu:main"),
    ])
    lines.append(f"<i>🕐 {fmt_msk(datetime.utcnow())} МСК</i>")
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=btns)


# ══════════════════════════════════════════════════════════════════════════════
# ДЕКОРАТОР СУПЕРАДМИН
# ══════════════════════════════════════════════════════════════════════════════

def superadmin(func):
    @functools.wraps(func)
    async def wrapper(msg: Message, **kwargs):
        if msg.from_user.id not in SUPER_ADMINS:
            return
        return await func(msg, **kwargs)
    return wrapper


# ══════════════════════════════════════════════════════════════════════════════
# КОМАНДЫ — ОБЩИЕ
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(Command("start"))
async def cmd_start(msg: Message):
    user_id = msg.from_user.id
    args    = msg.text.split(maxsplit=1)
    ref_code = args[1].strip() if len(args) > 1 else None

    # Обработать реферальную ссылку
    ref_credited = False
    if ref_code and ref_code.startswith("ref_"):
        code = ref_code[4:]
        ref_credited = await process_referral(user_id, code)

    # Убедиться что ref_code сгенерирован для нового пользователя
    await get_or_create_ref_code(user_id)

    _, _, trial_used = await get_user_plan(user_id)
    trial_text = ""
    if not trial_used and user_id not in SUPER_ADMINS:
        trial_text = (
            f"\n\n🎁 <b>У вас есть {TRIAL_DAYS} дней бесплатного тарифа Про!</b>\n"
            "Нажмите <b>👤 Личный кабинет</b> → <b>Активировать пробный период</b>."
        )

    ref_text = "\n\n✅ Реферальная ссылка применена! Бонус будет начислен после активации подписки." if ref_credited else ""

    text = (
        "📊 <b>Channel Analytics Bot</b>\n\n"
        "Отслеживаю статистику ваших Telegram-каналов.\n"
        "Всё управление — через кнопки ниже."
        + trial_text + ref_text
    )
    await msg.answer(text, reply_markup=await kb_main_menu(user_id), parse_mode="HTML")


@dp.message(Command("menu"))
async def cmd_menu(msg: Message):
    await msg.answer(
        "📊 <b>Главное меню</b>",
        reply_markup=await kb_main_menu(msg.from_user.id),
        parse_mode="HTML",
    )


@dp.message(Command("help"))
async def cmd_help(msg: Message):
    await msg.answer(
        "📖 <b>Справка</b>\n\n"
        "Всё управление через кнопку <b>/menu</b>.\n\n"
        "<b>Что отслеживается:</b>\n"
        "• 👥 Подписчики — прирост, убыль, пик, минимум\n"
        "• ✍️ Посты — кол-во, среднее в день, доля медиа\n"
        "• 👁 Просмотры — сумма, среднее, рекорд\n"
        "• 🔥 Серия активных дней подряд\n"
        "• ⏰ Лучшее время публикаций\n"
        "• 🔄 Сравнение текущего периода с предыдущим\n"
        "• 🤖 AI-анализ с рекомендациями\n"
        "• 📋 Сводный отчёт по всем каналам\n"
        "• 📥 Excel-отчёт с графиками\n\n"
        "📬 Еженедельный дайджест — каждый понедельник 09:00 МСК.",
        parse_mode="HTML",
    )


# ══════════════════════════════════════════════════════════════════════════════
# КОМАНДЫ — СУПЕРАДМИН
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(Command("panel"))
@superadmin
async def cmd_panel(msg: Message):
    text, kb = await render_admin_panel()
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
        await msg.answer("❌ Числа только.")
        return

    await ensure_channel(channel_id, str(channel_id), None)
    ch_name = str(channel_id)
    try:
        chat = await bot.get_chat(channel_id)
        await ensure_channel(channel_id, chat.title, chat.username)
        ch_name = f"@{chat.username}" if chat.username else chat.title
    except Exception as e:
        logger.warning(f"get_chat {channel_id}: {e}")

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
                await ensure_channel(cid, str(cid), None)
                info = await get_channel_info(cid)
            await set_channel_active(cid, True)
            label = f"@{info[2]}" if info[2] else (info[1] or str(cid))
            results.append(f"🟢 <b>{label}</b> — включён")
        except ValueError:
            results.append(f"❌ <code>{p}</code> — не число")
    await msg.answer("\n".join(results), parse_mode="HTML")


@dp.message(Command("setplan"))
@superadmin
async def cmd_setplan(msg: Message):
    """
    /setplan <user_id> <plan> <days>
    plan: basic | pro | business | agency
    /setplan <user_id> year <plan>  — годовая подписка (365 дней, −20%)
    """
    parts = msg.text.split()
    if len(parts) not in (4,):
        await msg.answer(
            "Использование:\n"
            "<code>/setplan &lt;user_id&gt; &lt;plan&gt; &lt;days&gt;</code>\n\n"
            "Планы: basic · pro · business · agency\n"
            "Пример: <code>/setplan 123456789 pro 30</code>\n"
            "Годовая: <code>/setplan 123456789 pro 365</code>",
            parse_mode="HTML",
        )
        return
    try:
        user_id = int(parts[1])
        plan    = parts[2].lower()
        days    = int(parts[3])
    except ValueError:
        await msg.answer("❌ Неверный формат.")
        return

    if plan not in PLANS:
        await msg.answer(f"❌ Неизвестный план: {plan}\nДоступны: {', '.join(PLANS)}")
        return

    await set_user_plan(user_id, plan, days)
    # Начислить реферальный бонус — только при реальной оплате (не за пробник)
    if plan != "basic":
        await credit_referral_bonus(user_id)
    cfg     = plan_cfg(plan)
    expires = (datetime.utcnow() + timedelta(days=days)).strftime("%d.%m.%Y")
    await msg.answer(
        f"✅ Пользователю <code>{user_id}</code> установлен тариф "
        f"<b>{cfg['emoji']} {cfg['name']}</b> до <b>{expires}</b> ({days} дней)",
        parse_mode="HTML",
    )


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
            plan, exp, _ = await get_user_plan(uid)
            pcfg = plan_cfg(plan)
            exp_s = exp.strftime("%d.%m") if exp else "∞"
            lines.append(f"\n👤 <b>{u}</b> <code>{uid}</code>  {pcfg['emoji']} {pcfg['name']} ({exp_s})")
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
# CALLBACKS — МЕНЮ
# ══════════════════════════════════════════════════════════════════════════════

@dp.callback_query(F.data == "menu:main")
async def cb_main_menu(call: CallbackQuery):
    await call.message.edit_text(
        "📊 <b>Главное меню</b>",
        reply_markup=await kb_main_menu(call.from_user.id),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data == "menu:channels")
async def cb_menu_channels(call: CallbackQuery):
    user_id  = call.from_user.id
    channels = await get_user_channels_full(user_id)

    if not channels:
        await call.message.edit_text(
            "📢 <b>Мои каналы</b>\n\nУ вас нет привязанных каналов.\n"
            "Обратитесь к администратору бота.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="◀ Назад", callback_data="menu:main")
            ]]),
            parse_mode="HTML",
        )
        await call.answer()
        return

    rows = []
    for cid, title, uname, active, co_admin in channels:
        label = f"@{uname}" if uname else (title or str(cid))
        rows.append([InlineKeyboardButton(text=f"📢 {label}", callback_data=f"ch_menu:{cid}")])
    rows.append([InlineKeyboardButton(text="◀ Назад", callback_data="menu:main")])

    await call.message.edit_text(
        f"📢 <b>Мои каналы ({len(channels)})</b>\n\nВыберите канал:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.in_({"menu:cabinet", "menu:plan"}))
async def cb_menu_cabinet(call: CallbackQuery):
    plan, _, trial_used = await get_user_plan(call.from_user.id)
    text = await fmt_cabinet(call.from_user.id)
    await call.message.edit_text(
        text,
        reply_markup=kb_cabinet(trial_used, plan),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )
    await call.answer()


@dp.callback_query(F.data == "menu:plans")
async def cb_menu_plans(call: CallbackQuery):
    plan, _, _ = await get_user_plan(call.from_user.id)
    await call.message.edit_text(
        fmt_plans_list(plan),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="◀ Назад в кабинет", callback_data="menu:cabinet"),
        ]]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data == "ref:copy")
async def cb_ref_copy(call: CallbackQuery):
    ref_code = await get_or_create_ref_code(call.from_user.id)
    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start=ref_{ref_code}"
    await call.answer(
        f"Ваша ссылка скопирована!\n{ref_link}",
        show_alert=True,
    )


@dp.callback_query(F.data == "menu:summary")
async def cb_menu_summary(call: CallbackQuery):
    if not await has_feature(call.from_user.id, "summary"):
        await call.answer("❌ Доступно с тарифа Бизнес", show_alert=True)
        return
    await call.answer("⏳")
    text = await fmt_summary(call.from_user.id)
    try:
        await call.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔄 Обновить", callback_data="menu:summary"),
                InlineKeyboardButton(text="◀ Назад",     callback_data="menu:main"),
            ]]),
            parse_mode="HTML",
        )
    except Exception:
        pass  # message not modified — ignore


@dp.callback_query(F.data == "menu:help")
async def cb_menu_help(call: CallbackQuery):
    await call.message.edit_text(
        "📖 <b>Как пользоваться ботом</b>\n\n"
        "1. Нажмите <b>📢 Мои каналы</b>\n"
        "2. Выберите нужный канал\n"
        "3. Выберите действие:\n"
        "   • 📈 Статистика — показатели за период\n"
        "   • 🔄 Сравнить — эта неделя/месяц vs прошлая\n"
        "   • 📥 Excel-отчёт — скачать файл с графиками\n"
        "   • 🤖 AI-анализ — рекомендации от нейросети\n"
        "   • 🏆 Топ постов — самые просматриваемые\n"
        "   • ⚙️ Настройки — доступ со-администраторов\n\n"
        "📬 Еженедельный дайджест — каждый понедельник 09:00 МСК.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="◀ Назад", callback_data="menu:main")
        ]]),
        parse_mode="HTML",
    )
    await call.answer()


# ── Тариф ────────────────────────────────────────────────────────────────────

@dp.callback_query(F.data == "plan:trial")
async def cb_plan_trial(call: CallbackQuery):
    user_id = call.from_user.id
    ok = await activate_trial(user_id)
    if not ok:
        await call.answer("Пробный период уже был использован.", show_alert=True)
        return
    cfg     = plan_cfg(TRIAL_PLAN)
    expires = (datetime.utcnow() + timedelta(days=TRIAL_DAYS)).strftime("%d.%m.%Y")
    await call.answer(f"🎁 Тариф {cfg['name']} активирован до {expires}!", show_alert=True)
    plan, _, trial_used = await get_user_plan(user_id)
    text = await fmt_cabinet(user_id)
    await call.message.edit_text(
        text,
        reply_markup=kb_cabinet(trial_used, plan),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


@dp.callback_query(F.data.startswith("plan:upgrade:"))
async def cb_plan_upgrade(call: CallbackQuery):
    feature_map = {
        "compare":   "Сравнение периодов — тариф Про",
        "excel":     "Excel-отчёты — тариф Про",
        "ai":        "AI-анализ — тариф Бизнес",
        "top_posts": "Топ постов — тариф Про",
        "periods":   "Период «Всё время» — тариф Про",
        "summary":   "Сводный отчёт — тариф Бизнес",
    }
    feature = call.data.split(":")[-1]
    msg_text = feature_map.get(feature, "Эта функция")
    plan, _, trial_used = await get_user_plan(call.from_user.id)
    await call.answer(
        f"🔒 {msg_text}\n\nПерейдите в «Мой тариф» для обновления.",
        show_alert=True,
    )


# ── Канал ────────────────────────────────────────────────────────────────────

async def _send_channel_card(call: CallbackQuery, cid: int):
    """Карточка канала: аватарка + название + описание + кнопки."""
    user_id = call.from_user.id
    info    = await get_channel_info(cid)
    kb      = await kb_channel_menu(cid, user_id)

    title       = info[1] if info else str(cid)
    description = None
    members     = None
    photo_id    = None

    try:
        chat        = await bot.get_chat(cid)
        title       = chat.title or title
        description = getattr(chat, "description", None)
        members     = await bot.get_chat_member_count(cid)
        if chat.photo:
            photo_id = chat.photo.big_file_id
    except Exception:
        pass

    lines = [f"<b>{title}</b>"]
    if info and info[2]:
        lines.append(f"@{info[2]}")
    if members is not None:
        lines.append(f"\n👥 <b>{members:,}</b> подписчиков")
    if description:
        desc = description[:200] + ("…" if len(description) > 200 else "")
        lines.append(f"\n{desc}")

    caption = "\n".join(lines)

    try:
        await call.message.delete()
    except Exception:
        pass

    if photo_id:
        try:
            await bot.send_photo(
                chat_id=user_id,
                photo=photo_id,
                caption=caption,
                reply_markup=kb,
                parse_mode="HTML",
            )
            return
        except Exception:
            pass

    await bot.send_message(
        chat_id=user_id,
        text=caption,
        reply_markup=kb,
        parse_mode="HTML",
    )


@dp.callback_query(F.data.startswith("ch_menu:"))
async def cb_ch_menu(call: CallbackQuery):
    cid = int(call.data.split(":")[1])
    if not await can_access(call.from_user.id, cid):
        await call.answer("❌ Нет доступа.", show_alert=True)
        return
    info = await get_channel_info(cid)
    if not info or not info[3]:
        await call.answer("🔴 Канал отключён.", show_alert=True)
        return
    await call.answer()
    await _send_channel_card(call, cid)


@dp.callback_query(F.data.startswith("stats_menu:"))
async def cb_stats_menu(call: CallbackQuery):
    cid = int(call.data.split(":")[1])
    if not await can_access(call.from_user.id, cid):
        await call.answer("❌ Нет доступа.", show_alert=True)
        return
    info    = await get_channel_info(cid)
    display = f"@{info[2]}" if info and info[2] else (info[1] if info else str(cid))
    plan, _, _ = await get_user_plan(call.from_user.id)
    owner   = await get_channel_owner(cid)
    is_owner = bool(owner and owner[0] == call.from_user.id) or (call.from_user.id in SUPER_ADMINS)

    await call.message.edit_text(
        f"📈 <b>{display}</b>\n\nВыберите период:",
        reply_markup=kb_stats_periods(cid, 7, plan, is_owner=is_owner),
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

    plan, _, _ = await get_user_plan(call.from_user.id)
    if period not in plan_cfg(plan)["periods"]:
        await call.answer("🔒 Недоступно на вашем тарифе.", show_alert=True)
        return

    info = await get_channel_info(cid)
    if not info or not info[3]:
        await call.answer("🔴 Канал отключён.", show_alert=True)
        return

    await call.answer("⏳")
    display     = f"@{info[2]}" if info[2] else (info[1] or str(cid))
    owner       = await get_channel_owner(cid)
    is_owner    = bool(owner and owner[0] == call.from_user.id) or (call.from_user.id in SUPER_ADMINS)
    period_days = period if period > 0 else None
    s    = await get_stats(cid, period_days)
    text = fmt_stats(display, s, PERIOD_LABELS.get(period, f"{period} дней"))

    try:
        await call.message.edit_text(
            text,
            reply_markup=kb_stats_periods(cid, period, plan, is_owner=is_owner),
            parse_mode="HTML",
        )
    except Exception:
        pass


# ── Сравнение ─────────────────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("compare_menu:"))
async def cb_compare_menu(call: CallbackQuery):
    cid = int(call.data.split(":")[1])
    if not await has_feature(call.from_user.id, "compare"):
        await call.answer("🔒 Доступно с тарифа Про.", show_alert=True)
        return
    info    = await get_channel_info(cid)
    display = f"@{info[2]}" if info and info[2] else str(cid)
    await call.message.edit_text(
        f"🔄 <b>Сравнение периодов — {display}</b>\n\nВыберите окно сравнения:",
        reply_markup=kb_compare_periods(cid),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("compare:"))
async def cb_compare(call: CallbackQuery):
    _, ch, win = call.data.split(":")
    cid, window = int(ch), int(win)

    if not await has_feature(call.from_user.id, "compare"):
        await call.answer("🔒 Доступно с тарифа Про.", show_alert=True)
        return
    if not await can_access(call.from_user.id, cid):
        await call.answer("❌ Нет доступа.", show_alert=True)
        return

    await call.answer("⏳")
    info    = await get_channel_info(cid)
    display = f"@{info[2]}" if info and info[2] else str(cid)
    cur, prev = await get_comparison(cid, window)
    text = fmt_comparison(display, cur, prev, window)

    await call.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🔄 Обновить", callback_data=f"compare:{cid}:{win}"),
                InlineKeyboardButton(text="◀ Назад",     callback_data=f"compare_menu:{cid}"),
            ]
        ]),
        parse_mode="HTML",
    )


# ── AI-анализ ────────────────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("ai_menu:"))
async def cb_ai_menu(call: CallbackQuery):
    cid = int(call.data.split(":")[1])
    if not await has_feature(call.from_user.id, "ai"):
        await call.answer("🔒 Доступно с тарифа Бизнес.", show_alert=True)
        return
    info    = await get_channel_info(cid)
    display = f"@{info[2]}" if info and info[2] else str(cid)
    await call.message.edit_text(
        f"🤖 <b>AI-анализ — {display}</b>\n\nЗа какой период анализировать?",
        reply_markup=kb_ai_periods(cid),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("ai:"))
async def cb_ai(call: CallbackQuery):
    _, ch, per = call.data.split(":")
    cid, period = int(ch), int(per)

    if not await has_feature(call.from_user.id, "ai"):
        await call.answer("🔒 Доступно с тарифа Бизнес.", show_alert=True)
        return
    if not await can_access(call.from_user.id, cid):
        await call.answer("❌ Нет доступа.", show_alert=True)
        return

    await call.answer("⏳ Анализирую...")
    info        = await get_channel_info(cid)
    display     = f"@{info[2]}" if info and info[2] else str(cid)
    period_days = period if period > 0 else None
    period_lbl  = PERIOD_LABELS.get(period, f"{period} дней")

    status = await call.message.answer("🤖 <b>Генерирую AI-анализ...</b>", parse_mode="HTML")

    s       = await get_stats(cid, period_days)
    result  = await ai_analyze(display, s, period_lbl)

    await status.edit_text(
        f"🤖 <b>AI-анализ — {display}</b>\n"
        f"⏱ Период: <b>{period_lbl}</b>\n\n"
        f"{result}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔄 Обновить", callback_data=f"ai:{cid}:{per}"),
            InlineKeyboardButton(text="◀ Назад",     callback_data=f"ai_menu:{cid}"),
        ]]),
        parse_mode="HTML",
    )


# ── Топ постов ────────────────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("top_posts:"))
async def cb_top_posts(call: CallbackQuery):
    parts  = call.data.split(":")
    cid    = int(parts[1])
    period = int(parts[2])

    if not await has_feature(call.from_user.id, "top_posts"):
        await call.answer("🔒 Доступно с тарифа Про.", show_alert=True)
        return
    if not await can_access(call.from_user.id, cid):
        await call.answer("❌ Нет доступа.", show_alert=True)
        return

    await call.answer("⏳")
    info    = await get_channel_info(cid)
    display = f"@{info[2]}" if info and info[2] else str(cid)
    uname   = info[2] if info else None

    period_days = period if period > 0 else None
    period_lbl  = PERIOD_LABELS.get(period, f"{period} дней")
    top = await get_top_posts(cid, period_days)

    lines = [f"🏆 <b>Топ постов — {display}</b>", f"⏱ {period_lbl}", ""]

    if not top:
        lines.append("Нет данных о постах за этот период.")
    else:
        for i, (msg_id, views, has_media, posted_at) in enumerate(top, 1):
            media_icon = "🖼 " if has_media else ""
            date = posted_at[:10] if posted_at else "?"
            if uname:
                link = f'<a href="https://t.me/{uname}/{msg_id}">Пост #{msg_id}</a>'
            else:
                ch_short = str(cid).replace("-100", "")
                link = f'<a href="https://t.me/c/{ch_short}/{msg_id}">Пост #{msg_id}</a>'
            lines.append(f"{i}. {media_icon}{link}")
            lines.append(f"   👁 <b>{views:,}</b> просм.  •  {date}")
            lines.append("")

    await call.message.edit_text(
        "\n".join(lines),
        reply_markup=kb_top_posts_periods(cid, period),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


# ── Настройки канала ──────────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("ch_settings:"))
async def cb_ch_settings(call: CallbackQuery):
    cid     = int(call.data.split(":")[1])
    user_id = call.from_user.id
    owner   = await get_channel_owner(cid)
    is_owner = bool(owner and owner[0] == user_id) or (user_id in SUPER_ADMINS)

    if not is_owner:
        await call.answer("❌ Только владелец канала.", show_alert=True)
        return
    if not await has_feature(user_id, "coadmin"):
        await call.answer("🔒 Доступно с тарифа Про.", show_alert=True)
        return

    info    = await get_channel_info(cid)
    display = f"@{info[2]}" if info and info[2] else str(cid)
    co_admin = bool(info[4]) if info else False

    await call.message.edit_text(
        f"⚙️ <b>Настройки — {display}</b>\n\n"
        f"👥 Со-администраторы: {'<b>включены</b> — они видят статистику.' if co_admin else '<b>выключены</b> — только вы видите статистику.'}",
        reply_markup=kb_ch_settings(cid, co_admin, is_owner),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("coadmin:"))
async def cb_coadmin(call: CallbackQuery):
    cid     = int(call.data.split(":")[1])
    user_id = call.from_user.id
    owner   = await get_channel_owner(cid)
    if not (owner and owner[0] == user_id) and user_id not in SUPER_ADMINS:
        await call.answer("❌ Только владелец канала.", show_alert=True)
        return
    new     = await toggle_co_admin(cid)
    state   = "включён ✅" if new else "отключён ❌"
    await call.answer(f"Со-администраторы: {state}", show_alert=True)

    info    = await get_channel_info(cid)
    display = f"@{info[2]}" if info and info[2] else str(cid)
    co_admin = bool(info[4]) if info else False
    await call.message.edit_text(
        f"⚙️ <b>Настройки — {display}</b>\n\n"
        f"👥 Со-администраторы: {'<b>включены</b> — они видят статистику.' if new else '<b>выключены</b> — только вы видите статистику.'}",
        reply_markup=kb_ch_settings(cid, new, True),
        parse_mode="HTML",
    )


# ── Excel-отчёт ───────────────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("report_sel:"))
async def cb_report_sel(call: CallbackQuery):
    cid = int(call.data.split(":")[1])
    if not await has_feature(call.from_user.id, "excel"):
        await call.answer("🔒 Excel-отчёты доступны с тарифа Про.", show_alert=True)
        return
    if not await can_access(call.from_user.id, cid):
        await call.answer("❌ Нет доступа.", show_alert=True)
        return
    info    = await get_channel_info(cid)
    display = f"@{info[2]}" if info and info[2] else str(cid)
    await call.message.edit_text(
        f"📥 <b>Excel-отчёт — {display}</b>\n\nВыберите период:",
        reply_markup=kb_report_periods(cid),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("report:"))
async def cb_report(call: CallbackQuery):
    _, ch, per = call.data.split(":")
    cid, period = int(ch), int(per)

    if not await has_feature(call.from_user.id, "excel"):
        await call.answer("🔒 Excel-отчёты доступны с тарифа Про.", show_alert=True)
        return
    if not await can_access(call.from_user.id, cid):
        await call.answer("❌ Нет доступа.", show_alert=True)
        return

    info        = await get_channel_info(cid)
    display     = f"@{info[2]}" if info and info[2] else str(cid)
    period_days = period if period > 0 else None
    period_lbl  = PERIOD_LABELS.get(period, f"{period} дней")

    await call.answer("⏳ Формирую отчёт...")
    status = await call.message.answer("⏳ <b>Генерирую Excel-отчёт...</b>", parse_mode="HTML")

    tmp_path = None
    try:
        tmp_path = await generate_report(
            db_path=DB_PATH, channel_id=cid,
            channel_name=display, period_label=period_lbl, period_days=period_days,
        )
        await bot.send_document(
            chat_id=call.from_user.id,
            document=FSInputFile(tmp_path),
            caption=(
                f"📊 <b>{display}</b> — отчёт за <b>{period_lbl}</b>\n"
                f"<i>🕐 {fmt_msk(datetime.utcnow())} МСК</i>"
            ),
            parse_mode="HTML",
        )
        await status.edit_text("✅ <b>Отчёт готов!</b>", parse_mode="HTML")
    except Exception as e:
        logger.error(f"Report error: {e}", exc_info=True)
        await status.edit_text("❌ Ошибка при генерации отчёта.", parse_mode="HTML")
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)


# ── Панель суперадмина — callbacks ────────────────────────────────────────────

@dp.callback_query(F.data == "admin:panel")
async def cb_admin_panel(call: CallbackQuery):
    if call.from_user.id not in SUPER_ADMINS:
        await call.answer("❌", show_alert=True)
        return
    text, kb = await render_admin_panel()
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
    new = not bool(info[3])
    await set_channel_active(cid, new)
    label = f"@{info[2]}" if info[2] else (info[1] or str(cid))
    await call.answer(f"{'🟢 Включён' if new else '🔴 Отключён'}: {label}", show_alert=True)
    text, kb = await render_admin_panel()
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
    text, kb = await render_admin_panel()
    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass


# ── Старые callback-пути (backward compat) ────────────────────────────────────

@dp.callback_query(F.data == "list_channels")
async def cb_list_channels_compat(call: CallbackQuery):
    call.data = "menu:channels"
    await cb_menu_channels(call)


# ── Медиакит ─────────────────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("mediakit:"))
async def cb_mediakit(call: CallbackQuery):
    cid = int(call.data.split(":")[1])
    if not await has_feature(call.from_user.id, "excel"):
        await call.answer("🔒 Медиакит доступен с тарифа Про.", show_alert=True)
        return
    if not await can_access(call.from_user.id, cid):
        await call.answer("❌ Нет доступа.", show_alert=True)
        return

    await call.answer("⏳ Генерирую медиакит...")
    status = await call.message.answer("⏳ <b>Генерирую медиакит PDF...</b>", parse_mode="HTML")

    info    = await get_channel_info(cid)
    display = f"@{info[2]}" if info and info[2] else (info[1] if info else str(cid))
    description = None
    try:
        chat = await bot.get_chat(cid)
        description = getattr(chat, "description", None)
    except Exception:
        pass

    tmp_path = None
    try:
        tmp_path = await generate_mediakit(
            db_path=DB_PATH,
            channel_id=cid,
            channel_name=display,
            channel_description=description,
        )
        await bot.send_document(
            chat_id=call.from_user.id,
            document=FSInputFile(tmp_path),
            caption=(
                f"📄 <b>Медиакит — {display}</b>\n"
                f"<i>Готов к отправке рекламодателям</i>\n"
                f"<i>🕐 {fmt_msk(datetime.utcnow())} МСК</i>"
            ),
            parse_mode="HTML",
        )
        await status.edit_text("✅ <b>Медиакит готов!</b>", parse_mode="HTML")
    except Exception as e:
        logger.error(f"Mediakit error: {e}", exc_info=True)
        await status.edit_text("❌ Ошибка при генерации медиакита.", parse_mode="HTML")
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)


# ── Настройки ────────────────────────────────────────────────────────────────

@dp.callback_query(F.data == "menu:settings")
async def cb_menu_settings(call: CallbackQuery):
    daily_on = await get_daily_digest(call.from_user.id)
    await call.message.edit_text(
        "⚙️ <b>Настройки</b>\n\n"
        "📬 <b>Ежедневная сводка</b> — каждое утро в 09:00 МСК бот присылает "
        "краткую статистику за вчера по всем вашим каналам.",
        reply_markup=kb_main_settings(daily_on),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data == "settings:toggle_daily")
async def cb_toggle_daily(call: CallbackQuery):
    new_val = await toggle_daily_digest(call.from_user.id)
    state   = "включена ✅" if new_val else "отключена ❌"
    await call.answer(f"Ежедневная сводка {state}", show_alert=True)
    await call.message.edit_text(
        "⚙️ <b>Настройки</b>\n\n"
        "📬 <b>Ежедневная сводка</b> — каждое утро в 09:00 МСК бот присылает "
        "краткую статистику за вчера по всем вашим каналам.",
        reply_markup=kb_main_settings(new_val),
        parse_mode="HTML",
    )


# ── Telegram Stars оплата ─────────────────────────────────────────────────────

@dp.callback_query(F.data == "stars:menu")
async def cb_stars_menu(call: CallbackQuery):
    await call.message.edit_text(
        "⭐ <b>Оплата через Telegram Stars</b>\n\n"
        "Выберите тариф и срок подписки.\n"
        "Оплата происходит мгновенно прямо в Telegram — без карт и банков.\n\n"
        "<i>1 месяц = 30 дней · 1 год = 365 дней</i>",
        reply_markup=kb_stars_menu(),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("stars:buy:"))
async def cb_stars_buy(call: CallbackQuery):
    parts   = call.data.split(":")
    plan    = parts[2]
    days    = int(parts[3])
    period  = "месяц" if days == 30 else "год"
    pcfg    = plan_cfg(plan)
    stars   = STARS_PRICES[plan]["month" if days == 30 else "year"]

    await bot.send_invoice(
        chat_id=call.from_user.id,
        title=f"{pcfg['emoji']} {pcfg['name']} — {period}",
        description=(
            f"Доступ к тарифу {pcfg['name']} на {days} дней.\n"
            f"Каналов: {'∞' if pcfg['channels'] > 1000 else pcfg['channels']} · "
            f"Excel · AI · {'Бизнес-функции' if pcfg.get('ai') else 'Про-функции'}"
        ),
        payload=f"{plan}:{days}:{call.from_user.id}",
        currency="XTR",
        prices=[LabeledPrice(label=f"{pcfg['name']} {period}", amount=stars)],
    )
    await call.answer()


@dp.pre_checkout_query()
async def pre_checkout(query):
    await bot.answer_pre_checkout_query(query.id, ok=True)


@dp.message(F.successful_payment)
async def successful_payment(msg: Message):
    payload = msg.successful_payment.invoice_payload
    parts   = payload.split(":")
    if len(parts) != 3:
        return

    plan, days, uid_str = parts
    user_id = int(uid_str)
    days    = int(days)

    await set_user_plan(user_id, plan, days)
    await credit_referral_bonus(user_id)

    pcfg    = plan_cfg(plan)
    expires = (datetime.utcnow() + timedelta(days=days)).strftime("%d.%m.%Y")
    period  = "месяц" if days == 30 else "год"

    await msg.answer(
        f"✅ <b>Оплата прошла!</b>\n\n"
        f"Тариф {pcfg['emoji']} <b>{pcfg['name']}</b> активирован на {period}.\n"
        f"Действует до <b>{expires}</b>.\n\n"
        f"Нажмите /menu чтобы открыть бота.",
        parse_mode="HTML",
    )


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


async def send_expiry_reminders():
    """Напоминает пользователям за 3 дня и в день истечения подписки."""
    now = datetime.utcnow()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT user_id, plan, expires_at FROM subscriptions WHERE expires_at IS NOT NULL"
        ) as cur:
            rows = await cur.fetchall()

    for user_id, plan, expires_str in rows:
        try:
            exp       = datetime.fromisoformat(expires_str)
            days_left = (exp - now).days
            cfg       = plan_cfg(plan)

            if days_left == 3:
                await bot.send_message(
                    user_id,
                    f"⏰ <b>Подписка истекает через 3 дня</b>\n\n"
                    f"Тариф {cfg['emoji']} <b>{cfg['name']}</b> активен до "
                    f"<b>{(exp + MSK_OFFSET).strftime('%d.%m.%Y')}</b>.\n\n"
                    f"Обратитесь к администратору для продления.",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="👤 Личный кабинет", callback_data="menu:cabinet"),
                    ]]),
                )
            elif days_left == 0:
                await bot.send_message(
                    user_id,
                    f"⚠️ <b>Подписка истекает сегодня!</b>\n\n"
                    f"После истечения тариф {cfg['emoji']} <b>{cfg['name']}</b> будет "
                    f"понижен до 🔹 <b>Базового</b>.",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="👤 Личный кабинет", callback_data="menu:cabinet"),
                    ]]),
                )
            await asyncio.sleep(0.3)
        except Exception as e:
            logger.warning(f"Expiry reminder {user_id}: {e}")


async def send_daily_digest():
    """Ежедневная сводка за вчера для пользователей у которых она включена."""
    user_ids = await get_daily_digest_users()
    yesterday_start = (datetime.utcnow() - timedelta(days=1)).replace(hour=0, minute=0, second=0).isoformat()
    yesterday_end   = (datetime.utcnow() - timedelta(days=1)).replace(hour=23, minute=59, second=59).isoformat()
    yesterday_str   = (datetime.utcnow() - timedelta(days=1)).strftime("%d.%m.%Y")

    for uid in user_ids:
        channels = await get_user_channels_full(uid)
        if not channels:
            continue

        lines = [f"📬 <b>Сводка за {yesterday_str}</b>", ""]
        for cid, title, uname, *_ in channels:
            display = f"@{uname}" if uname else (title or str(cid))

            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT members FROM snapshots WHERE channel_id=? AND taken_at>=? ORDER BY taken_at DESC LIMIT 1",
                    (cid, yesterday_start),
                ) as cur:
                    row = await cur.fetchone()
                    members_end = row[0] if row else None

                async with db.execute(
                    "SELECT members FROM snapshots WHERE channel_id=? AND taken_at>=? ORDER BY taken_at LIMIT 1",
                    (cid, yesterday_start),
                ) as cur:
                    row = await cur.fetchone()
                    members_start = row[0] if row else None

                async with db.execute(
                    "SELECT COUNT(*), SUM(views) FROM posts WHERE channel_id=? AND posted_at>=? AND posted_at<=?",
                    (cid, yesterday_start, yesterday_end),
                ) as cur:
                    row = await cur.fetchone()
                    posts_cnt  = row[0] or 0
                    views_sum  = int(row[1] or 0)

            growth = (members_end - members_start) if (members_end and members_start) else None
            sign   = "+" if (growth or 0) >= 0 else ""
            arrow  = "📈" if (growth or 0) > 0 else ("📉" if (growth or 0) < 0 else "➡️")

            lines.append(f"<b>{display}</b>")
            if members_end:
                lines.append(f"  👥 {members_end:,}  {arrow} {sign}{growth or 0}")
            lines.append(f"  ✍️ {posts_cnt} постов   👁 {views_sum:,} просм.")
            lines.append("")

        lines.append(f"<i>⚙️ Отключить: /menu → Настройки</i>")

        try:
            await bot.send_message(uid, "\n".join(lines), parse_mode="HTML")
            await asyncio.sleep(0.3)
        except Exception as e:
            logger.warning(f"Daily digest {uid}: {e}")


async def send_weekly_digest() -> Tuple[int, int]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """SELECT DISTINCT uc.user_id FROM user_channels uc
               JOIN channels c ON c.channel_id=uc.channel_id WHERE c.active=1"""
        ) as cur:
            user_ids = [r[0] for r in await cur.fetchall()]

    sent = failed = 0
    for uid in user_ids:
        channels = await get_user_channels_full(uid)
        plan, _, _ = await get_user_plan(uid)
        for cid, title, uname, *_ in channels:
            display  = f"@{uname}" if uname else (title or str(cid))
            s        = await get_stats(cid, 7)
            text     = "📬 <b>Еженедельный дайджест</b>\n\n" + fmt_stats(display, s, "Неделя")
            owner    = await get_channel_owner(cid)
            is_owner = bool(owner and owner[0] == uid)
            try:
                await bot.send_message(
                    uid, text, parse_mode="HTML",
                    reply_markup=kb_stats_periods(cid, 7, plan, is_owner=is_owner),
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
        logger.warning("SUPER_ADMINS пуст!")

    await init_db()

    scheduler.add_job(take_snapshots,       "interval", hours=1,        id="hourly")
    scheduler.add_job(send_weekly_digest,   "cron", day_of_week="mon",   hour=9,  id="weekly")
    scheduler.add_job(send_expiry_reminders,"cron", hour=10, minute=0,            id="expiry")
    scheduler.add_job(send_daily_digest,      "cron", hour=9,  minute=0,            id="daily")
    scheduler.start()

    asyncio.create_task(take_snapshots())

    logger.info(f"Бот запущен. Суперадмины: {SUPER_ADMINS}")
    await dp.start_polling(
        bot,
        allowed_updates=["message", "callback_query", "channel_post"],
    )


if __name__ == "__main__":
    asyncio.run(main())
