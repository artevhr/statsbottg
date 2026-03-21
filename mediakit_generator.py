# версия v1.4
"""
mediakit_generator.py
Генерирует PDF медиакит канала для отправки рекламодателям.
Использует DejaVuSans для поддержки кириллицы.
"""

import os
import tempfile
from datetime import datetime, timedelta
from typing import Optional

import aiosqlite
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    HRFlowable, Paragraph, SimpleDocTemplate,
    Spacer, Table, TableStyle,
)

W, H = A4

# ── Цвета ─────────────────────────────────────────────────────────────────────
DARK    = colors.HexColor("#0F1117")
ACCENT  = colors.HexColor("#5B6EF5")
ACCENT2 = colors.HexColor("#7C3AED")
GREEN   = colors.HexColor("#22C55E")
RED     = colors.HexColor("#EF4444")
GOLD    = colors.HexColor("#F59E0B")
WHITE   = colors.white
LIGHT   = colors.HexColor("#F1F5F9")
MID     = colors.HexColor("#94A3B8")
BORDER  = colors.HexColor("#E2E8F0")

# ── Шрифты ────────────────────────────────────────────────────────────────────

_FONTS_REGISTERED = False
_FONT_NORMAL = "Helvetica"
_FONT_BOLD   = "Helvetica-Bold"

def _ensure_fonts():
    global _FONTS_REGISTERED, _FONT_NORMAL, _FONT_BOLD
    if _FONTS_REGISTERED:
        return _FONT_NORMAL, _FONT_BOLD

    candidates = [
        ("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",      "DejaVu",     False),
        ("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", "DejaVuBold", True),
    ]
    ok_normal = ok_bold = False
    for path, name, is_bold in candidates:
        if os.path.exists(path):
            try:
                if name not in pdfmetrics.getRegisteredFontNames():
                    pdfmetrics.registerFont(TTFont(name, path))
                if is_bold:
                    _FONT_BOLD   = name
                    ok_bold      = True
                else:
                    _FONT_NORMAL = name
                    ok_normal    = True
            except Exception:
                pass

    if not ok_bold:
        _FONT_BOLD = _FONT_NORMAL  # fallback

    _FONTS_REGISTERED = True
    return _FONT_NORMAL, _FONT_BOLD


# ── Стили ─────────────────────────────────────────────────────────────────────

def _st():
    n, b = _ensure_fonts()
    return {
        "title":    ParagraphStyle("mk_title",    fontSize=24, textColor=WHITE,  fontName=b, leading=30, spaceAfter=2),
        "subtitle": ParagraphStyle("mk_sub",      fontSize=11, textColor=MID,    fontName=n, leading=16),
        "h2":       ParagraphStyle("mk_h2",       fontSize=12, textColor=DARK,   fontName=b, leading=18, spaceBefore=6, spaceAfter=3),
        "body":     ParagraphStyle("mk_body",     fontSize=9,  textColor=DARK,   fontName=n, leading=13),
        "small":    ParagraphStyle("mk_small",    fontSize=7,  textColor=MID,    fontName=n, leading=11),
        "footer_p": ParagraphStyle("mk_footer_p", fontSize=7,  textColor=WHITE,  fontName=n, leading=11),
        "bar_p":    ParagraphStyle("mk_bar_p",    fontSize=7,  textColor=DARK,   fontName=n, leading=9),
        "kpi_lbl":  ParagraphStyle("mk_kpilbl",  fontSize=7,  textColor=MID,    fontName=n, leading=10, alignment=1),
    }


def _kpi_val_style(color):
    n, b = _ensure_fonts()
    return ParagraphStyle(f"mk_kpiv_{id(color)}", fontSize=20, textColor=color,
                          fontName=b, leading=24, alignment=1)


def _tbl_font():
    _, b = _ensure_fonts()
    return b


def _tbl_font_normal():
    n, _ = _ensure_fonts()
    return n


# ── Данные из БД ──────────────────────────────────────────────────────────────

async def _fetch(db_path: str, channel_id: int) -> dict:
    now = datetime.utcnow()
    s7  = (now - timedelta(days=7)).isoformat()
    s30 = (now - timedelta(days=30)).isoformat()
    s90 = (now - timedelta(days=90)).isoformat()

    async with aiosqlite.connect(db_path) as db:
        async with db.execute(
            "SELECT members FROM snapshots WHERE channel_id=? ORDER BY taken_at DESC LIMIT 1",
            (channel_id,),
        ) as cur:
            row = await cur.fetchone()
            members_now = row[0] if row else 0

        async with db.execute(
            "SELECT members FROM snapshots WHERE channel_id=? AND taken_at>=? ORDER BY taken_at LIMIT 1",
            (channel_id, s30),
        ) as cur:
            row = await cur.fetchone()
            members_30d = row[0] if row else members_now

        async with db.execute(
            "SELECT COUNT(*), SUM(views), AVG(views), MAX(views), SUM(has_media) FROM posts WHERE channel_id=? AND posted_at>=?",
            (channel_id, s30),
        ) as cur:
            row = await cur.fetchone()
            posts_30 = row[0] or 0
            views_sum_30 = int(row[1] or 0)
            views_avg_30 = round(row[2] or 0)
            views_max_30 = int(row[3] or 0)
            media_30 = row[4] or 0

        async with db.execute(
            "SELECT COUNT(*), SUM(views), AVG(views) FROM posts WHERE channel_id=? AND posted_at>=?",
            (channel_id, s7),
        ) as cur:
            row = await cur.fetchone()
            posts_7 = row[0] or 0
            views_sum_7 = int(row[1] or 0)
            views_avg_7 = round(row[2] or 0)

        async with db.execute(
            "SELECT message_id, views, has_media, posted_at FROM posts WHERE channel_id=? AND posted_at>=? ORDER BY views DESC LIMIT 5",
            (channel_id, s30),
        ) as cur:
            top_posts = await cur.fetchall()

        async with db.execute(
            """SELECT DATE(taken_at), MAX(members)
               FROM snapshots WHERE channel_id=? AND taken_at>=?
               GROUP BY DATE(taken_at) ORDER BY DATE(taken_at)""",
            (channel_id, s30),
        ) as cur:
            daily = await cur.fetchall()

        async with db.execute(
            """SELECT CAST(strftime('%H', posted_at) AS INTEGER), COUNT(*)
               FROM posts WHERE channel_id=? AND posted_at>=?
               GROUP BY 1 ORDER BY 2 DESC LIMIT 1""",
            (channel_id, s30),
        ) as cur:
            best_hour = await cur.fetchone()

    growth_30  = members_now - members_30d
    growth_pct = round(growth_30 / members_30d * 100, 1) if members_30d else 0
    er         = round(views_avg_30 / members_now * 100, 2) if members_now else 0
    media_pct  = round(media_30 / posts_30 * 100) if posts_30 else 0

    return dict(
        members_now=members_now, members_30d=members_30d,
        growth_30=growth_30, growth_pct=growth_pct,
        posts_30=posts_30, posts_7=posts_7,
        views_sum_30=views_sum_30, views_avg_30=views_avg_30,
        views_max_30=views_max_30, views_sum_7=views_sum_7, views_avg_7=views_avg_7,
        er=er, media_pct=media_pct, best_hour=best_hour,
        top_posts=top_posts, daily=daily,
    )


# ── Генератор ─────────────────────────────────────────────────────────────────

async def generate_mediakit(
    db_path: str,
    channel_id: int,
    channel_name: str,
    channel_description: Optional[str],
) -> str:
    _ensure_fonts()
    d   = await _fetch(db_path, channel_id)
    st  = _st()
    n, b = _ensure_fonts()
    ts  = (datetime.utcnow() + timedelta(hours=3)).strftime("%d.%m.%Y")

    safe = "".join(c for c in channel_name if c.isalnum() or c in "-_@")[:28]
    path = os.path.join(
        tempfile.gettempdir(),
        f"mediakit_{safe}_{datetime.utcnow().strftime('%Y%m%d')}.pdf"
    )

    doc = SimpleDocTemplate(
        path, pagesize=A4,
        leftMargin=15*mm, rightMargin=15*mm,
        topMargin=15*mm, bottomMargin=15*mm,
    )
    story = []

    # ── ШАПКА ─────────────────────────────────────────────────────────────────
    header_data = [[
        Paragraph(channel_name, st["title"]),
        Paragraph(f"Медиакит · {ts}", st["subtitle"]),
    ]]
    header_tbl = Table(header_data, colWidths=[110*mm, 65*mm])
    header_tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1,-1), DARK),
        ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
        ("TOPPADDING",    (0,0), (-1,-1), 10),
        ("BOTTOMPADDING", (0,0), (-1,-1), 10),
        ("LEFTPADDING",   (0,0), (0,-1),  10),
        ("RIGHTPADDING",  (-1,0),(-1,-1), 10),
        ("ALIGN",         (1,0), (1,-1),  "RIGHT"),
    ]))
    story.append(header_tbl)
    story.append(Spacer(1, 4*mm))

    if channel_description:
        desc = channel_description[:280] + ("…" if len(channel_description) > 280 else "")
        story.append(Paragraph(desc, st["body"]))
        story.append(Spacer(1, 3*mm))

    story.append(HRFlowable(width="100%", thickness=1, color=BORDER))
    story.append(Spacer(1, 4*mm))

    # ── KPI КАРТОЧКИ ──────────────────────────────────────────────────────────
    story.append(Paragraph("КЛЮЧЕВЫЕ ПОКАЗАТЕЛИ", st["h2"]))
    story.append(Spacer(1, 3*mm))

    sign = "+" if d["growth_30"] >= 0 else ""
    g_color = GREEN if d["growth_30"] >= 0 else RED
    er_color = GREEN if d["er"] >= 20 else (GOLD if d["er"] >= 10 else RED)

    kpis = [
        (f"{d['members_now']:,}",           "Подписчиков",        ACCENT),
        (f"{sign}{d['growth_30']:,}",        "Прирост за 30 дней", g_color),
        (f"{sign}{d['growth_pct']}%",        "Рост за 30 дней",    g_color),
        (f"{d['views_avg_30']:,}",           "Среднее просмотров", ACCENT2),
        (f"{d['er']}%",                      "Охват (ER)",         er_color),
        (f"{d['posts_30']}",                 "Постов за 30 дней",  GOLD),
    ]

    rows_kpi = [kpis[:3], kpis[3:]]
    for row_items in rows_kpi:
        tbl_data = [
            [Paragraph(v, _kpi_val_style(c)) for v, _, c in row_items],
            [Paragraph(l, st["kpi_lbl"])     for _, l, _ in row_items],
        ]
        tbl = Table(tbl_data, colWidths=[58*mm]*3)
        tbl.setStyle(TableStyle([
            ("BACKGROUND",    (0,0), (-1,-1), LIGHT),
            ("BOX",           (0,0), (0,-1),  0.5, BORDER),
            ("BOX",           (1,0), (1,-1),  0.5, BORDER),
            ("BOX",           (2,0), (2,-1),  0.5, BORDER),
            ("ALIGN",         (0,0), (-1,-1), "CENTER"),
            ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
            ("TOPPADDING",    (0,0), (-1,-1), 7),
            ("BOTTOMPADDING", (0,0), (-1,-1), 7),
        ]))
        story.append(tbl)
        story.append(Spacer(1, 2*mm))

    story.append(Spacer(1, 3*mm))
    story.append(HRFlowable(width="100%", thickness=1, color=BORDER))
    story.append(Spacer(1, 4*mm))

    # ── ТАБЛИЦА АКТИВНОСТИ ────────────────────────────────────────────────────
    story.append(Paragraph("АУДИТОРИЯ И АКТИВНОСТЬ", st["h2"]))
    story.append(Spacer(1, 3*mm))

    best_h_str = f"{(d['best_hour'][0]+3)%24:02d}:00 МСК" if d["best_hour"] else "нет данных"

    aud_data = [
        ["Показатель", "7 дней", "30 дней"],
        ["Постов опубликовано",    str(d["posts_7"]),        str(d["posts_30"])],
        ["Суммарно просмотров",    f"{d['views_sum_7']:,}",  f"{d['views_sum_30']:,}"],
        ["Среднее просмотров/пост",f"{d['views_avg_7']:,}",  f"{d['views_avg_30']:,}"],
        ["Максимум (один пост)",   "—",                      f"{d['views_max_30']:,}"],
        ["Постов с медиа",         "—",                      f"{d['media_pct']}%"],
        ["Лучшее время постов",    "—",                      best_h_str],
    ]

    aud_tbl = Table(aud_data, colWidths=[90*mm, 36*mm, 49*mm])
    aud_tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),  (-1,0),  DARK),
        ("TEXTCOLOR",     (0,0),  (-1,0),  WHITE),
        ("FONTNAME",      (0,0),  (-1,0),  b),
        ("FONTNAME",      (0,1),  (-1,-1), n),
        ("FONTSIZE",      (0,0),  (-1,-1), 9),
        ("ALIGN",         (1,0),  (-1,-1), "CENTER"),
        ("ALIGN",         (0,0),  (0,-1),  "LEFT"),
        ("ROWBACKGROUNDS",(0,1),  (-1,-1), [WHITE, LIGHT]),
        ("GRID",          (0,0),  (-1,-1), 0.5, BORDER),
        ("TOPPADDING",    (0,0),  (-1,-1), 5),
        ("BOTTOMPADDING", (0,0),  (-1,-1), 5),
        ("LEFTPADDING",   (0,0),  (0,-1),  6),
    ]))
    story.append(aud_tbl)
    story.append(Spacer(1, 4*mm))
    story.append(HRFlowable(width="100%", thickness=1, color=BORDER))
    story.append(Spacer(1, 4*mm))

    # ── ТОП ПОСТОВ ────────────────────────────────────────────────────────────
    if d["top_posts"]:
        story.append(Paragraph("ТОП ПОСТОВ ЗА 30 ДНЕЙ", st["h2"]))
        story.append(Spacer(1, 3*mm))

        top_data = [["#", "Дата", "Просмотров", "Медиа"]]
        for i, (mid, views, has_media, posted_at) in enumerate(d["top_posts"], 1):
            top_data.append([
                str(i),
                (posted_at or "")[:10],
                f"{views:,}",
                "Да" if has_media else "Текст",
            ])

        top_tbl = Table(top_data, colWidths=[12*mm, 40*mm, 55*mm, 68*mm])
        top_tbl.setStyle(TableStyle([
            ("BACKGROUND",    (0,0),  (-1,0),  ACCENT),
            ("TEXTCOLOR",     (0,0),  (-1,0),  WHITE),
            ("FONTNAME",      (0,0),  (-1,0),  b),
            ("FONTNAME",      (0,1),  (-1,-1), n),
            ("FONTSIZE",      (0,0),  (-1,-1), 9),
            ("ALIGN",         (0,0),  (-1,-1), "CENTER"),
            ("ROWBACKGROUNDS",(0,1),  (-1,-1), [WHITE, LIGHT]),
            ("GRID",          (0,0),  (-1,-1), 0.5, BORDER),
            ("TOPPADDING",    (0,0),  (-1,-1), 5),
            ("BOTTOMPADDING", (0,0),  (-1,-1), 5),
        ]))
        story.append(top_tbl)
        story.append(Spacer(1, 4*mm))
        story.append(HRFlowable(width="100%", thickness=1, color=BORDER))
        story.append(Spacer(1, 4*mm))

    # ── ТЕКСТОВЫЙ ГРАФИК РОСТА ────────────────────────────────────────────────
    if d["daily"] and len(d["daily"]) > 3:
        story.append(Paragraph("ДИНАМИКА ПОДПИСЧИКОВ (30 ДНЕЙ)", st["h2"]))
        story.append(Spacer(1, 2*mm))

        vals = [row[1] for row in d["daily"]]
        mn, mx = min(vals), max(vals)
        step = max(1, len(vals) // 12)

        for i, (date, members) in enumerate(d["daily"]):
            if i % step != 0:
                continue
            bar_len = round(((members - mn) / (mx - mn + 1)) * 28) if mx > mn else 14
            bar_str = "█" * bar_len + "░" * (28 - bar_len)
            line    = f"{date[5:]}  {bar_str}  {members:,}"
            story.append(Paragraph(line, st["bar_p"]))

        story.append(Spacer(1, 4*mm))
        story.append(HRFlowable(width="100%", thickness=1, color=BORDER))
        story.append(Spacer(1, 4*mm))

    # ── ПОДВАЛ ────────────────────────────────────────────────────────────────
    footer_tbl = Table(
        [[Paragraph(f"Аналитика: WH Analytics Bot · @whanalyticbot · {ts}", st["footer_p"])]],
        colWidths=[175*mm]
    )
    footer_tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1,-1), DARK),
        ("TOPPADDING",    (0,0), (-1,-1), 7),
        ("BOTTOMPADDING", (0,0), (-1,-1), 7),
        ("LEFTPADDING",   (0,0), (-1,-1), 10),
    ]))
    story.append(footer_tbl)

    doc.build(story)
    return path
