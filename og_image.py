"""
Dynamic social-preview image generator: renders a PNG on the fly for
/e/<event_id>/card.png, with a real incrementing hit counter, plus the
/e/<event_id> share page that a link in a text message actually points to.

Add to review_dashboard.py:

    from og_image import event_card_bp
    app.register_blueprint(event_card_bp)

Requires:
    pip install Pillow

Font files expected in static/fonts/ (bundle these in the repo):
    ComicNeue-Bold.ttf       -- https://fonts.google.com/specimen/Comic+Neue
    ArchivoBlack-Regular.ttf -- https://fonts.google.com/specimen/Archivo+Black
    VT323-Regular.ttf        -- https://fonts.google.com/specimen/VT323
All three are open (OFL) licensed.
"""

import io
import os
import math
import sqlite3
from datetime import datetime, timedelta
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from flask import Blueprint, Response, request, send_file, render_template, url_for, redirect
from icalendar import Calendar, Event as ICalEvent

from PIL import Image, ImageDraw, ImageFont

event_card_bp = Blueprint("event_card", __name__)

CENTRAL = ZoneInfo("America/Chicago")

# Default duration assumed for an event when the source page never gave us an
# end_time -- long enough to cover a typical show without claiming precision we
# don't have.
DEFAULT_DURATION = timedelta(hours=3)


def _event_datetimes(event):
    """(start, end) as tz-aware America/Chicago datetimes, or (None, None) if
    start_date is missing/unparseable. When start_time is missing too, this is
    an all-day event -- returned as naive dates (icalendar treats a `date`
    value, not `datetime`, as an all-day VEVENT) rather than guessing a time."""
    start_date = event.get("start_date")
    if not start_date:
        return None, None
    try:
        date_part = datetime.strptime(start_date, "%Y-%m-%d").date()
    except ValueError:
        return None, None

    start_time = event.get("start_time")
    if not start_time:
        return date_part, date_part + timedelta(days=1)

    try:
        hh, mm = start_time.split(":")[:2]
        start_dt = datetime(date_part.year, date_part.month, date_part.day,
                             int(hh), int(mm), tzinfo=CENTRAL)
    except (ValueError, TypeError):
        return date_part, date_part + timedelta(days=1)

    end_time = event.get("end_time")
    end_dt = None
    if end_time:
        try:
            eh, em = end_time.split(":")[:2]
            end_dt = datetime(date_part.year, date_part.month, date_part.day,
                               int(eh), int(em), tzinfo=CENTRAL)
            if end_dt <= start_dt:
                end_dt += timedelta(days=1)  # e.g. 11pm - 1am
        except (ValueError, TypeError):
            end_dt = None
    if end_dt is None:
        end_dt = start_dt + DEFAULT_DURATION

    return start_dt, end_dt


def _google_calendar_url(event, start, end):
    if start is None:
        return None
    fmt = "%Y%m%d" if not isinstance(start, datetime) else "%Y%m%dT%H%M%S"
    dates = f"{start.strftime(fmt)}/{end.strftime(fmt)}"
    location = event.get("venue") or ""
    if event.get("location"):
        location = f"{location} ({event['location']})"
    params = {
        "action": "TEMPLATE",
        "text": event.get("name") or "Event",
        "dates": dates,
        "location": location,
        "details": event.get("description") or "",
    }
    return "https://calendar.google.com/calendar/render?" + urlencode(params)


def _format_share_date(start_date):
    """start_date is stored as 'YYYY-MM-DD' -- render as 'M/DD' for previews."""
    if not start_date:
        return ""
    try:
        return datetime.strptime(start_date, "%Y-%m-%d").strftime("%-m/%d")
    except ValueError:
        return start_date

# Card is rendered at 2x the 680x357 design canvas -> 1200x630 (twitter/OG standard-ish)
W, H = 1200, 630
SCALE = W / 680.0

FONT_DIR = os.path.join(os.path.dirname(__file__), "static", "fonts")


def _font(name, size):
    path = os.path.join(FONT_DIR, name)
    try:
        return ImageFont.truetype(path, size)
    except OSError:
        return ImageFont.load_default()


def _s(v):
    """Scale a design-canvas coordinate/size up to the real image size."""
    return v * SCALE


# ---------------------------------------------------------------------------
# Hit counter -- one row in site_stats, works with sqlite3 (local) or
# psycopg2 (Railway Postgres).
#
# Rather than importing back from review_dashboard.py (which causes a
# circular-import loop when review_dashboard.py is run directly as __main__),
# review_dashboard.py hands us its existing get_db_connection, get_event_by_id,
# and get_city_branding functions via init(). Call init() once, right after
# all three are defined, before registering the blueprint.
# ---------------------------------------------------------------------------

_get_db_connection = None
_get_event_by_id = None
_get_city_branding = None


def init(get_db_connection_fn, get_event_by_id_fn, get_city_branding_fn):
    global _get_db_connection, _get_event_by_id, _get_city_branding
    _get_db_connection = get_db_connection_fn
    _get_event_by_id = get_event_by_id_fn
    _get_city_branding = get_city_branding_fn


def _city_branding():
    """Card abbreviation + display city name for the current request's domain --
    this is one shared deployment serving multiple domains, resolved per-request
    (see get_city_branding in review_dashboard.py), not a fixed constant."""
    if _get_city_branding is None:
        raise RuntimeError("og_image.init() must be called before use -- "
                            "see review_dashboard.py wiring")
    return _get_city_branding()


def _get_conn():
    if _get_db_connection is None:
        raise RuntimeError("og_image.init() must be called before use -- "
                            "see review_dashboard.py wiring")
    return _get_db_connection()


_stats_table_ready = False


def ensure_stats_table():
    global _stats_table_ready
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS site_stats (
            stat_key TEXT PRIMARY KEY,
            count INTEGER NOT NULL DEFAULT 0
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    _stats_table_ready = True


def increment_and_get_hits(stat_key='total_hits'):
    """stat_key lets each event (f'event_{id}_hits') and the homepage ('homepage_hits')
    keep their own independent tally in the same table, instead of one site-wide total."""
    if not _stats_table_ready:
        ensure_stats_table()

    conn = _get_conn()
    cur = conn.cursor()
    is_sqlite = isinstance(conn, sqlite3.Connection)

    if is_sqlite:
        cur.execute(
            "INSERT INTO site_stats (stat_key, count) VALUES (?, 1) "
            "ON CONFLICT(stat_key) DO UPDATE SET count = count + 1",
            (stat_key,)
        )
        cur.execute("SELECT count FROM site_stats WHERE stat_key = ?", (stat_key,))
        hits = cur.fetchone()[0]
    else:
        cur.execute(
            "INSERT INTO site_stats (stat_key, count) VALUES (%s, 1) "
            "ON CONFLICT (stat_key) DO UPDATE SET count = site_stats.count + 1 "
            "RETURNING count",
            (stat_key,)
        )
        hits = cur.fetchone()[0]

    conn.commit()
    cur.close()
    conn.close()
    return hits


# ---------------------------------------------------------------------------
# Drawing helpers
# ---------------------------------------------------------------------------

def _vertical_gradient(w, h, stops):
    """stops: list of (position 0..1, (r,g,b)). Returns an RGB Image w x h."""
    grad = Image.new("RGB", (1, h))
    px = grad.load()
    for y in range(h):
        t = y / max(h - 1, 1)
        for i in range(len(stops) - 1):
            p0, c0 = stops[i]
            p1, c1 = stops[i + 1]
            if p0 <= t <= p1:
                local_t = (t - p0) / max(p1 - p0, 1e-6)
                r = int(c0[0] + (c1[0] - c0[0]) * local_t)
                g = int(c0[1] + (c1[1] - c0[1]) * local_t)
                b = int(c0[2] + (c1[2] - c0[2]) * local_t)
                px[0, y] = (r, g, b)
                break
    return grad.resize((w, h))


def _gradient_text(draw_size, text, font, stops_vertical, stroke_width, stroke_fill):
    """Renders text filled with a vertical gradient plus a solid stroke outline.
    Returns an RGBA image sized to fit the text, to be pasted at the desired position."""
    # Measure text (with stroke) first
    tmp = Image.new("RGBA", (10, 10))
    tdraw = ImageDraw.Draw(tmp)
    bbox = tdraw.textbbox((0, 0), text, font=font, stroke_width=stroke_width)
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    pad = stroke_width + 4
    canvas = Image.new("RGBA", (tw + pad * 2, th + pad * 2), (0, 0, 0, 0))
    cdraw = ImageDraw.Draw(canvas)

    # Solid stroke pass (this is the outline color showing at the edges)
    cdraw.text((pad - bbox[0], pad - bbox[1]), text, font=font,
               fill=stroke_fill, stroke_width=stroke_width, stroke_fill=stroke_fill)

    # Build alpha mask of the fill (no stroke) to punch the gradient through
    mask = Image.new("L", canvas.size, 0)
    mdraw = ImageDraw.Draw(mask)
    mdraw.text((pad - bbox[0], pad - bbox[1]), text, font=font, fill=255)

    gradient = _vertical_gradient(canvas.width, canvas.height, stops_vertical)
    gradient_rgba = gradient.convert("RGBA")
    canvas.paste(gradient_rgba, (0, 0), mask)

    return canvas


def _star4(cx, cy, r_outer, r_inner=None):
    if r_inner is None:
        r_inner = r_outer * 0.4
    pts = []
    for i in range(8):
        ang = math.pi / 4 * i - math.pi / 2
        rad = r_outer if i % 2 == 0 else r_inner
        pts.append((cx + rad * math.cos(ang), cy + rad * math.sin(ang)))
    return pts


def _tape_stripes(draw, y_top, strip_h, fill_bg, fill_stripe):
    """Yellow strip with black skewed 'construction tape' stripes, matching
    the approved design (25 degree skew, stripes spaced every 28 design-px)."""
    draw.rectangle([0, y_top, W, y_top + strip_h], fill=fill_bg)
    shift = strip_h * math.tan(math.radians(25))  # bottom edge shifts left by this much
    x = 0
    step = _s(28)
    stripe_w = _s(14)
    while x < W + shift:
        top_l = (x, y_top)
        top_r = (x + stripe_w, y_top)
        bot_r = (x + stripe_w - shift, y_top + strip_h)
        bot_l = (x - shift, y_top + strip_h)
        draw.polygon([top_l, top_r, bot_r, bot_l], fill=fill_stripe)
        x += step


def _bevel_button(draw, x, y, w, h, label, font):
    draw.rounded_rectangle([x, y, x + w, y + h], radius=_s(4),
                            fill=(192, 192, 192), outline=(0, 0, 0), width=int(_s(1.5)))
    draw.rectangle([x + _s(3), y + _s(3), x + w - _s(3), y + _s(17)],
                   fill=(255, 255, 255, 128))
    tw = draw.textlength(label, font=font)
    draw.text((x + w / 2 - tw / 2, y + h / 2 - _s(9)), label, font=font, fill=(0, 0, 204))


def _tag_badge(draw, x, y, w, h, border_color, lines, font):
    draw.rectangle([x, y, x + w, y + h], fill=(0, 0, 0), outline=border_color, width=int(_s(1)))
    ly = y + _s(6)
    for text, color, size_font in lines:
        draw.text((x + _s(8), ly), text, font=size_font, fill=color)
        ly += _s(13)


# ---------------------------------------------------------------------------
# Main renderer
# ---------------------------------------------------------------------------

def render_geocities(event, hits, city_abbr="HOU", city_name="Houston"):
    img = Image.new("RGB", (W, H))
    draw = ImageDraw.Draw(img)

    # Background vertical gradient (deep purple -> violet)
    bg = _vertical_gradient(W, H, [
        (0.0, (26, 10, 61)),
        (0.55, (45, 10, 94)),
        (1.0, (74, 14, 110)),
    ])
    img.paste(bg, (0, 0))
    draw = ImageDraw.Draw(img)

    # Scattered stars
    star_positions = [
        (20, 30, 1.2), (60, 70, 1), (110, 20, 1.4), (150, 90, 1), (200, 35, 1.2),
        (250, 60, 1), (480, 25, 1.3), (520, 80, 1), (560, 40, 1.2), (610, 65, 1),
        (650, 30, 1.3), (40, 140, 1), (620, 150, 1.2), (30, 300, 1), (640, 290, 1.2),
        (90, 320, 1), (590, 320, 1),
    ]
    for cx, cy, r in star_positions:
        rr = _s(r)
        draw.ellipse([_s(cx) - rr, _s(cy) - rr, _s(cx) + rr, _s(cy) + rr], fill=(255, 255, 255, 230))

    for cx, cy in [(100, 100), (580, 110), (50, 250), (610, 250)]:
        pts = _star4(_s(cx), _s(cy), _s(10), _s(4))
        draw.polygon(pts, fill=(255, 228, 0))

    # Top / bottom construction tape borders
    _tape_stripes(draw, 0, _s(14), (255, 228, 0), (17, 17, 17))
    _tape_stripes(draw, H - _s(14), _s(14), (255, 228, 0), (17, 17, 17))

    # HOU bevel text: drop shadow, then gradient fill with stroke
    hou_font = _font("ArchivoBlack-Regular.ttf", int(_s(120)))
    shadow_layer = _gradient_text((0, 0), city_abbr, hou_font,
                                   [(0, (0, 0, 0)), (1, (0, 0, 0))], int(_s(3)), (0, 0, 0))
    shadow_layer.putalpha(shadow_layer.getchannel("A").point(lambda a: int(a * 0.55)))
    hou_layer = _gradient_text((0, 0), city_abbr, hou_font,
                                [(0, (255, 238, 0)), (0.45, (255, 106, 0)), (1, (224, 0, 26))],
                                int(_s(3)), (92, 0, 0))

    hx = W / 2 - hou_layer.width / 2
    hy = _s(185) - hou_layer.height * 0.62
    img.paste(shadow_layer, (int(hx + _s(6)), int(hy + _s(6))), shadow_layer)
    img.paste(hou_layer, (int(hx), int(hy)), hou_layer)
    draw = ImageDraw.Draw(img)

    # Subtitle
    sub_font = _font("ComicNeue-Bold.ttf", int(_s(16)))
    sub_text = "e v e n t l i s t . c o m"
    sw = draw.textlength(sub_text, font=sub_font)
    draw.text((W / 2 - sw / 2, _s(200)), sub_text, font=sub_font, fill=(0, 255, 234))

    # Tagline (rainbow-ish gradient, left to right approximated as vertical band per line -
    # rendered as horizontal gradient via rotated approach)
    tag_font = _font("ComicNeue-Bold.ttf", int(_s(17)))
    tag_text = f"*~* {city_name} Music & Comedy Events *~*"
    tmp = Image.new("RGBA", (10, 10))
    tbbox = ImageDraw.Draw(tmp).textbbox((0, 0), tag_text, font=tag_font)
    tw, th = tbbox[2] - tbbox[0], tbbox[3] - tbbox[1]
    tag_grad = _vertical_gradient(th, tw, [
        (0.0, (255, 46, 196)), (0.25, (255, 212, 0)), (0.5, (57, 255, 20)),
        (0.75, (0, 229, 255)), (1.0, (255, 46, 196)),
    ]).rotate(90, expand=True).resize((tw + 8, th + 8))
    tag_mask = Image.new("L", tag_grad.size, 0)
    ImageDraw.Draw(tag_mask).text((4 - tbbox[0], 4 - tbbox[1]), tag_text, font=tag_font, fill=255)
    tag_layer = Image.new("RGBA", tag_grad.size, (0, 0, 0, 0))
    tag_layer.paste(tag_grad, (0, 0), tag_mask)
    img.paste(tag_layer, (int(W / 2 - tag_layer.width / 2), int(_s(228))), tag_layer)
    draw = ImageDraw.Draw(img)

    # Get tickets button
    btn_font = _font("ComicNeue-Bold.ttf", int(_s(15)))
    _bevel_button(draw, _s(190), _s(262), _s(300), _s(34), ">> GET TICKETS NOW <<", btn_font)

    # Best viewed badge
    badge_font_small = _font("ComicNeue-Bold.ttf", int(_s(9)))
    badge_font_med = _font("ComicNeue-Bold.ttf", int(_s(10)))
    _tag_badge(draw, _s(24), _s(300), _s(150), _s(34), (57, 255, 20), [
        ("best viewed in", (57, 255, 20), badge_font_small),
        ("Netscape Navigator 4.0", (57, 255, 20), badge_font_med),
    ], badge_font_small)

    # Hit counter badge -- the one live, real element on this whole card
    counter_label_font = _font("ComicNeue-Bold.ttf", int(_s(9)))
    counter_num_font = _font("VT323-Regular.ttf", int(_s(20)))
    cx0, cy0, cw, ch = _s(500), _s(300), _s(156), _s(34)
    draw.rectangle([cx0, cy0, cx0 + cw, cy0 + ch], fill=(17, 17, 17), outline=(255, 228, 0), width=int(_s(1)))
    draw.text((cx0 + _s(8), cy0 + _s(6)), "you are visitor number", font=counter_label_font, fill=(255, 228, 0))
    draw.text((cx0 + _s(8), cy0 + _s(15)), f"{hits:06d}", font=counter_num_font, fill=(255, 46, 196))

    return img


RENDERERS = {
    "geocities": render_geocities,
}

TEMPLATES = list(RENDERERS.keys())


def pick_template(event_id):
    return TEMPLATES[event_id % len(TEMPLATES)]


@event_card_bp.route("/e/<int:event_id>/card.png")
def event_card_image(event_id):
    event = _get_event_by_id(event_id) or {}
    hits = increment_and_get_hits(f"event_{event_id}_hits")
    template = pick_template(event_id)
    branding = _city_branding()
    img = RENDERERS[template](event, hits, branding["abbr"], branding["name"])

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return send_file(buf, mimetype="image/png")


@event_card_bp.route("/card.png")
def homepage_card_image():
    """OG image for the homepage/calendar share preview -- same card design (it never
    reads event fields anyway, see render_geocities), but its own independent hit
    counter rather than any single event's."""
    hits = increment_and_get_hits("homepage_hits")
    branding = _city_branding()
    img = RENDERERS[TEMPLATES[0]]({}, hits, branding["abbr"], branding["name"])

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return send_file(buf, mimetype="image/png")


def _preview_image_url(event, event_id):
    """Real event photo if one's stored on the row (e.g. a private-event invite
    flyer), otherwise the generated retro share card. `image_url` is stored as a
    root-relative path (e.g. /static/uploads/foo.jpeg), not an absolute URL, so
    it's resolved against the current request's host here rather than at
    insert time -- keeps it correct across both houeventlist.com/atxeventlist.com."""
    if event.get("image_url"):
        return request.url_root.rstrip("/") + event["image_url"]
    return url_for("event_card.event_card_image", event_id=event_id, _external=True)


@event_card_bp.route("/e/<int:event_id>")
def share_redirect(event_id):
    event = _get_event_by_id(event_id)
    # Same fallback chain the calendar itself uses to decide whether an event's title
    # is clickable at all (ticket_url || event_url || venue_url) -- this route only
    # checked ticket_url, so any event carrying just an event_url (McGonigel's never
    # populates ticket_url at all; Dan Electro's, Continental Club/Big Top mostly don't
    # either) silently redirected to /calendar instead of the real event page.
    redirect_url = event and (event.get("ticket_url") or event.get("event_url") or event.get("venue_url"))
    if not redirect_url:
        return redirect("/calendar")

    name = event.get("name") or f"{_city_branding()['name']} Event"
    date_str = _format_share_date(event.get("start_date"))
    venue = event.get("venue", "")
    title = f"{name} - {date_str} @ {venue}" if (date_str or venue) else name

    return render_template(
        "share_redirect.html",
        title=title,
        description=f"{event.get('venue', '')} -- {event.get('start_date', '')}",
        image_url=_preview_image_url(event, event_id),
        canonical_url=url_for("event_card.share_redirect", event_id=event_id, _external=True),
        redirect_url=redirect_url,
    )


def _format_full_date(start_date, start_time):
    """'Friday, August 21' (+ ' at 7:00 PM' if a time is known) for the detail page."""
    if not start_date:
        return ""
    try:
        d = datetime.strptime(start_date, "%Y-%m-%d")
    except ValueError:
        return start_date
    text = d.strftime("%A, %B %-d")
    if start_time:
        try:
            hh, mm = start_time.split(":")[:2]
            t = datetime(2000, 1, 1, int(hh), int(mm))
            text += t.strftime(" at %-I:%M %p")
        except (ValueError, TypeError):
            pass
    return text


@event_card_bp.route("/e/<int:event_id>/details")
def event_details(event_id):
    """Lingering event-detail page: unlike /e/<event_id> (which previews then
    auto-redirects straight to tickets), this page stays put and shows the full
    details plus add-to-calendar options -- for sharing an event itself, not
    just routing someone to buy a ticket."""
    event = _get_event_by_id(event_id)
    if not event:
        return redirect("/calendar")

    branding = _city_branding()
    name = event.get("name") or f"{branding['name']} Event"
    venue = event.get("venue", "")
    title = f"{name} - {_format_share_date(event.get('start_date'))} @ {venue}" if venue else name
    ticket_url = event.get("ticket_url") or event.get("event_url") or event.get("venue_url")

    start, end = _event_datetimes(event)

    return render_template(
        "event_details.html",
        event=event,
        title=title,
        display_date=_format_full_date(event.get("start_date"), event.get("start_time")),
        doors_time=event.get("doors_time"),
        ticket_url=ticket_url,
        image_url=_preview_image_url(event, event_id),
        canonical_url=url_for("event_card.event_details", event_id=event_id, _external=True),
        ics_url=url_for("event_card.event_ics", event_id=event_id, _external=True) if start else None,
        google_calendar_url=_google_calendar_url(event, start, end) if start else None,
    )


@event_card_bp.route("/e/<int:event_id>/event.ics")
def event_ics(event_id):
    event = _get_event_by_id(event_id)
    if not event:
        return redirect("/calendar")

    start, end = _event_datetimes(event)
    if start is None:
        return redirect(url_for("event_card.event_details", event_id=event_id))

    cal = Calendar()
    cal.add("prodid", "-//Group Events//houeventlist.com//")
    cal.add("version", "2.0")

    vevent = ICalEvent()
    vevent.add("summary", event.get("name") or "Event")
    vevent.add("dtstart", start)
    vevent.add("dtend", end)
    vevent.add("uid", f"event-{event_id}@houeventlist.com")
    vevent.add("dtstamp", datetime.now(CENTRAL))
    location = event.get("venue") or ""
    if event.get("location"):
        location = f"{location} ({event['location']})"
    if location:
        vevent.add("location", location)
    description_parts = [p for p in [
        event.get("description"),
        event.get("ticket_url") or event.get("event_url"),
    ] if p]
    if description_parts:
        vevent.add("description", "\n\n".join(description_parts))
    cal.add_component(vevent)

    return Response(
        cal.to_ical(),
        mimetype="text/calendar",
        headers={"Content-Disposition": f'inline; filename="event-{event_id}.ics"'},
    )
