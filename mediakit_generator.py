# версия v1.4
"""
mediakit_generator.py
Генерирует PDF медиакит канала для отправки рекламодателям.
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

# ── Палитра ───────────────────────────────────────────────────────────────────
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


# ── Стили ────────────────────────────────────────────────────────────────────

def _styles():
    base = {"fontName": "Helvetica", "leading": 14}
    return {
        "title":    ParagraphStyle("title",    fontSize=28, textColor=WHITE,  fontName="Helvetica-Bold", leading=34, spaceAfter=4),
        "subtitle": ParagraphStyle("sub",      fontSize=13, textColor=MID,    fontName="Helvetica",      leading=18),
        "h2":       ParagraphStyle("h2",       fontSize=14, textColor=DARK,   fontName="Helvetica-Bold", leading=20, spaceBefore=8, spaceAfter=4),
        "body":     ParagraphStyle("body",     fontSize=10, textColor=DARK,   fontName="Helvetica",      leading=15),
        "small":    ParagraphStyle("small",    fontSize=8,  textColor=MID,    fontName="Helvetica",      leading=12),
        "kpi_val":  ParagraphStyle("kpiv",     fontSize=26, textColor=ACCENT, fontName="Helvetica-Bold", leading=30, alignment=1),
        "kpi_lbl":  ParagraphStyle("kpil",     fontSize=8,  textColor=MID,    fontName="Helvetica",      leading=11, alignment=1),
        "white_h2": ParagraphStyle("wh2",      fontSize=13, textColor=WHITE,  fontName="Helvetica-Bold", leading=18),
        "white_b":  ParagraphStyle("wb",       fontSize=10, textColor=WHITE,  fontName="Helvetica",      leading=14),
    }


# ── Данные из БД ──────────────────────────────────────────────────────────────

async def _fetch_mediakit_data(db_path: str, channel_id: int) -> dict:
    now    = datetime.utcnow()
    s30    = (now - timedelta(days=30)).isoformat()
    s7     = (now - timedelta(days=7)).isoformat()
    s90    = (now - timedelta(days=90)).isoformat()

    async with aiosqlite.connect(db_path) as db:
        # Последний снимок
        async with db.execute(
            "SELECT members FROM snapshots WHERE channel_id=? ORDER BY taken_at DESC LIMIT 1",
            (channel_id,),
        ) as cur:
            row = await cur.fetchone()
            members_now = row[0] if row else 0

        # 30 дней назад
        async with db.execute(
            "SELECT members FROM snapshots WHERE channel_id=? AND taken_at>=? ORDER BY taken_at LIMIT 1",
            (channel_id, s30),
        ) as cur:
            row = await cur.fetchone()
            members_30d = row[0] if row else members_now

        # 90 дней назад
        async with db.execute(
            "SELECT members FROM snapshots WHERE channel_id=? AND taken_at>=? ORDER BY taken_at LIMIT 1",
            (channel_id, s90),
        ) as cur:
            row = await cur.fetchone()
            members_90d = row[0] if row else members_now

        # Посты за 30 дней
        async with db.execute(
            "SELECT COUNT(*), SUM(views), AVG(views), MAX(views), SUM(has_media) FROM posts WHERE channel_id=? AND posted_at>=?",
            (channel_id, s30),
        ) as cur:
            row = await cur.fetchone()
            posts_30      = row[0] or 0
            views_sum_30  = int(row[1] or 0)
            views_avg_30  = round(row[2] or 0)
            views_max_30  = int(row[3] or 0)
            media_30      = row[4] or 0

        # Посты за 7 дней
        async with db.execute(
            "SELECT COUNT(*), SUM(views), AVG(views) FROM posts WHERE channel_id=? AND posted_at>=?",
            (channel_id, s7),
        ) as cur:
            row = await cur.fetchone()
            posts_7     = row[0] or 0
            views_sum_7 = int(row[1] or 0)
            views_avg_7 = round(row[2] or 0)

        # Топ-5 постов за 30 дней
        async with db.execute(
            "SELECT message_id, views, has_media, posted_at FROM posts WHERE channel_id=? AND posted_at>=? ORDER BY views DESC LIMIT 5",
            (channel_id, s30),
        ) as cur:
            top_posts = await cur.fetchall()

        # Динамика по дням за 30 дней (для мини-графика)
        async with db.execute(
            """SELECT DATE(taken_at), MAX(members)
               FROM snapshots WHERE channel_id=? AND taken_at>=?
               GROUP BY DATE(taken_at) ORDER BY DATE(taken_at)""",
            (channel_id, s30),
        ) as cur:
            daily_members = await cur.fetchall()

        # Лучший час
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
        members_now=members_now, members_30d=members_30d, members_90d=members_90d,
        growth_30=growth_30, growth_pct=growth_pct,
        posts_30=posts_30, posts_7=posts_7,
        views_sum_30=views_sum_30, views_avg_30=views_avg_30,
        views_max_30=views_max_30, views_sum_7=views_sum_7, views_avg_7=views_avg_7,
        er=er, media_pct=media_pct, best_hour=best_hour,
        top_posts=top_posts, daily_members=daily_members,
    )


# ── Генератор PDF ─────────────────────────────────────────────────────────────

async def generate_mediakit(
    db_path: str,
    channel_id: int,
    channel_name: str,
    channel_description: Optional[str],
) -> str:
    d  = await _fetch_mediakit_data(db_path, channel_id)
    st = _styles()
    ts = (datetime.utcnow() + timedelta(hours=3)).strftime("%d.%m.%Y")

    safe = "".join(c for c in channel_name if c.isalnum() or c in "-_@")[:28]
    path = os.path.join(tempfile.gettempdir(), f"mediakit_{safe}_{datetime.utcnow().strftime('%Y%m%d')}.pdf")

    doc = SimpleDocTemplate(
        path, pagesize=A4,
        leftMargin=15*mm, rightMargin=15*mm,
        topMargin=15*mm, bottomMargin=15*mm,
    )

    story = []
    pad   = 5 * mm

    # ── ШАПКА ─────────────────────────────────────────────────────────────────
    header_data = [[
        Paragraph(channel_name, st["title"]),
        Paragraph(f"Медиакит · {ts}", st["subtitle"]),
    ]]
    header_tbl = Table(header_data, colWidths=[110*mm, 65*mm])
    header_tbl.setStyle(TableStyle([
        ("BACKGROUND",   (0,0), (-1,-1), DARK),
        ("VALIGN",       (0,0), (-1,-1), "MIDDLE"),
        ("TOPPADDING",   (0,0), (-1,-1), 10),
        ("BOTTOMPADDING",(0,0), (-1,-1), 10),
        ("LEFTPADDING",  (0,0), (0,-1),  10),
        ("RIGHTPADDING", (-1,0),(-1,-1), 10),
        ("ALIGN",        (1,0), (1,-1),  "RIGHT"),
        ("ROUNDEDCORNERS", [4]),
    ]))
    story.append(header_tbl)
    story.append(Spacer(1, 5*mm))

    # Описание канала
    if channel_description:
        story.append(Paragraph(channel_description[:300], st["body"]))
        story.append(Spacer(1, 4*mm))

    story.append(HRFlowable(width="100%", thickness=1, color=BORDER))
    story.append(Spacer(1, 4*mm))

    # ── KPI КАРТОЧКИ ──────────────────────────────────────────────────────────
    story.append(Paragraph("КЛЮЧЕВЫЕ ПОКАЗАТЕЛИ", st["h2"]))
    story.append(Spacer(1, 3*mm))

    sign = "+" if d["growth_30"] >= 0 else ""
    er_color = GREEN if d["er"] >= 20 else (GOLD if d["er"] >= 10 else RED)

    kpis = [
        (f"{d['members_now']:,}", "Подписчиков", ACCENT),
        (f"{sign}{d['growth_30']:,}", "Прирост за 30 дней", GREEN if d["growth_30"] >= 0 else RED),
        (f"{sign}{d['growth_pct']}%", "Рост за 30 дней", GREEN if d["growth_30"] >= 0 else RED),
        (f"{d['views_avg_30']:,}", "Среднее просмотров", ACCENT2),
        (f"{d['er']}%", "Охват (ER)", er_color),
        (f"{d['posts_30']}", "Постов за 30 дней", GOLD),
    ]

    kpi_cells = []
    for val, lbl, color in kpis:
        kpi_cells.append([
            Paragraph(val, ParagraphStyle("kv", fontSize=22, textColor=color,
                                           fontName="Helvetica-Bold", leading=26, alignment=1)),
            Paragraph(lbl, st["kpi_lbl"]),
        ])

    rows = [kpi_cells[:3], kpi_cells[3:]]
    for row_items in rows:
        tbl_data = [[item[0] for item in row_items], [item[1] for item in row_items]]
        tbl = Table(tbl_data, colWidths=[58*mm]*3)
        tbl.setStyle(TableStyle([
            ("BACKGROUND",    (0,0), (-1,-1), LIGHT),
            ("BOX",           (0,0), (0,-1),  0.5, BORDER),
            ("BOX",           (1,0), (1,-1),  0.5, BORDER),
            ("BOX",           (2,0), (2,-1),  0.5, BORDER),
            ("ALIGN",         (0,0), (-1,-1), "CENTER"),
            ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
            ("TOPPADDING",    (0,0), (-1,-1), 8),
            ("BOTTOMPADDING", (0,0), (-1,-1), 8),
        ]))
        story.append(tbl)
        story.append(Spacer(1, 2*mm))

    story.append(Spacer(1, 3*mm))
    story.append(HRFlowable(width="100%", thickness=1, color=BORDER))
    story.append(Spacer(1, 4*mm))

    # ── АУДИТОРИЯ И АКТИВНОСТЬ ────────────────────────────────────────────────
    story.append(Paragraph("АУДИТОРИЯ И АКТИВНОСТЬ", st["h2"]))
    story.append(Spacer(1, 3*mm))

    best_h_str = f"{(d['best_hour'][0]+3)%24:02d}:00 МСК" if d["best_hour"] else "нет данных"

    aud_data = [
        ["Показатель", "За 7 дней", "За 30 дней"],
        ["Постов опубликовано", str(d["posts_7"]), str(d["posts_30"])],
        ["Суммарно просмотров", f"{d['views_sum_7']:,}", f"{d['views_sum_30']:,}"],
        ["Среднее просмотров на пост", f"{d['views_avg_7']:,}", f"{d['views_avg_30']:,}"],
        ["Максимум просмотров (пост)", "—", f"{d['views_max_30']:,}"],
        ["Постов с медиа", "—", f"{d['media_pct']}%"],
        ["Лучшее время публикаций", "—", best_h_str],
    ]

    aud_tbl = Table(aud_data, colWidths=[88*mm, 37*mm, 37*mm * 1.08])
    aud_tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1,0),   DARK),
        ("TEXTCOLOR",     (0,0), (-1,0),   WHITE),
        ("FONTNAME",      (0,0), (-1,0),   "Helvetica-Bold"),
        ("FONTSIZE",      (0,0), (-1,-1),  9),
        ("ALIGN",         (1,0), (-1,-1),  "CENTER"),
        ("ALIGN",         (0,0), (0,-1),   "LEFT"),
        ("ROWBACKGROUNDS",(0,1), (-1,-1),  [WHITE, LIGHT]),
        ("GRID",          (0,0), (-1,-1),  0.5, BORDER),
        ("TOPPADDING",    (0,0), (-1,-1),  5),
        ("BOTTOMPADDING", (0,0), (-1,-1),  5),
        ("LEFTPADDING",   (0,0), (0,-1),   6),
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
            date = (posted_at or "")[:10]
            top_data.append([
                str(i),
                date,
                f"{views:,}",
                "🖼 Да" if has_media else "Текст",
            ])

        top_tbl = Table(top_data, colWidths=[10*mm, 35*mm, 50*mm, 25*mm * 2.2])
        top_tbl.setStyle(TableStyle([
            ("BACKGROUND",    (0,0), (-1,0),   ACCENT),
            ("TEXTCOLOR",     (0,0), (-1,0),   WHITE),
            ("FONTNAME",      (0,0), (-1,0),   "Helvetica-Bold"),
            ("FONTSIZE",      (0,0), (-1,-1),  9),
            ("ALIGN",         (0,0), (-1,-1),  "CENTER"),
            ("ROWBACKGROUNDS",(0,1), (-1,-1),  [WHITE, LIGHT]),
            ("GRID",          (0,0), (-1,-1),  0.5, BORDER),
            ("TOPPADDING",    (0,0), (-1,-1),  5),
            ("BOTTOMPADDING", (0,0), (-1,-1),  5),
        ]))
        story.append(top_tbl)
        story.append(Spacer(1, 4*mm))
        story.append(HRFlowable(width="100%", thickness=1, color=BORDER))
        story.append(Spacer(1, 4*mm))

    # ── ДИНАМИКА РОСТА ────────────────────────────────────────────────────────
    if d["daily_members"] and len(d["daily_members"]) > 3:
        story.append(Paragraph("ДИНАМИКА ПОДПИСЧИКОВ (30 ДНЕЙ)", st["h2"]))
        story.append(Spacer(1, 3*mm))

        vals = [row[1] for row in d["daily_members"]]
        mn, mx = min(vals), max(vals)
        bar_w = 3.5 * mm
        chart_h = 25 * mm
        total_w = len(vals) * (bar_w + 1*mm)
        usable_w = 175 * mm
        bar_w = min(bar_w, usable_w / len(vals) - 1*mm)

        bar_data = []
        for date, members in d["daily_members"]:
            height = ((members - mn) / (mx - mn + 1)) * chart_h if mx > mn else chart_h / 2
            bar_data.append([
                Paragraph(
                    f'<font color="#{ACCENT.hexval()[2:]}">▌</font>',
                    ParagraphStyle("bar", fontSize=max(4, int(height/mm*2)), leading=height+2)
                )
            ])

        # Упрощённый текстовый барчарт
        lines = []
        step = max(1, len(vals) // 10)
        for i, (date, members) in enumerate(d["daily_members"]):
            if i % step == 0:
                bar_len = round(((members - mn) / (mx - mn + 1)) * 30) if mx > mn else 15
                bar = "█" * bar_len + "░" * (30 - bar_len)
                lines.append(
                    Paragraph(
                        f'<font name="Helvetica" size="7" color="#94A3B8">{date[5:]} </font>'
                        f'<font name="Helvetica" size="7" color="#5B6EF5">{bar}</font>'
                        f'<font name="Helvetica" size="7" color="#94A3B8"> {members:,}</font>',
                        ParagraphStyle("bar", leading=10)
                    )
                )

        for line in lines:
            story.append(line)

        story.append(Spacer(1, 4*mm))
        story.append(HRFlowable(width="100%", thickness=1, color=BORDER))
        story.append(Spacer(1, 4*mm))

    # ── ПОДВАЛ ────────────────────────────────────────────────────────────────
    footer_data = [[
        Paragraph(
            f"Аналитика предоставлена <b>WH Analytics Bot</b> · @whanalyticbot · {ts}",
            ParagraphStyle("footer", fontSize=8, textColor=WHITE, fontName="Helvetica", leading=12)
        )
    ]]
    footer_tbl = Table(footer_data, colWidths=[175*mm])
    footer_tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1,-1), DARK),
        ("TOPPADDING",    (0,0), (-1,-1), 8),
        ("BOTTOMPADDING", (0,0), (-1,-1), 8),
        ("LEFTPADDING",   (0,0), (-1,-1), 10),
    ]))
    story.append(footer_tbl)

    doc.build(story)
    return path
