import os
import re
import json
import time
from datetime import datetime, timezone
from html import escape as html_escape

import requests


# ========== CONFIG (ENV) ==========
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

GROUP_ID = int(os.getenv("GROUP_ID", "-1002977868330"))
TOPIC_ID = int(os.getenv("TOPIC_ID", "65114"))

KICK_SLUG = os.getenv("KICK_SLUG", "gladvalakaspwnz").strip()
VK_SLUG = os.getenv("VK_SLUG", "gladvalakas").strip()

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))
STATE_FILE = os.getenv("STATE_FILE", "state.json")

ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "").strip()

START_DEDUP_SEC = int(os.getenv("START_DEDUP_SEC", "120"))
CHANGE_DEDUP_SEC = int(os.getenv("CHANGE_DEDUP_SEC", "20"))


# ========== URLS ==========
KICK_API_URL = f"https://kick.com/api/v2/channels/{KICK_SLUG}"
KICK_PUBLIC_URL = f"https://kick.com/{KICK_SLUG}"
VK_PUBLIC_URL = f"https://live.vkvideo.ru/{VK_SLUG}"


UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
HEADERS_JSON = {"User-Agent": UA, "Accept": "application/json,text/plain,*/*"}
HEADERS_HTML = {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}


# ========== HELPERS ==========
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def ts() -> int:
    return int(time.time())


def bust(url: str | None) -> str | None:
    """Cache-busting для URL картинок."""
    if not url:
        return None
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}t={ts()}"


def esc(s: str | None) -> str:
    return html_escape(s or "—", quote=False)


def fmt_viewers(v) -> str:
    return str(v) if isinstance(v, int) else "—"


def fmt_duration(seconds: int) -> str:
    seconds = max(0, int(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    return f"{h:02d} ч. {m:02d} мин."


def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {
            "any_live": False,
            "kick_live": False,
            "vk_live": False,
            "started_at": None,
            "startup_ping_sent": False,

            "kick_title": None,
            "kick_cat": None,
            "vk_title": None,
            "vk_cat": None,

            "kick_viewers": None,
            "vk_viewers": None,

            "last_start_sent_ts": 0,
            "last_change_sent_ts": 0,
        }
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state: dict) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def tg_call(method: str, payload: dict) -> dict:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is empty. Set BOT_TOKEN env var on host.")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    r = requests.post(url, json=payload, timeout=25)
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")
    return data["result"]


def notify_admin(text: str) -> None:
    if not ADMIN_CHAT_ID:
        return
    try:
        tg_call("sendMessage", {"chat_id": int(ADMIN_CHAT_ID), "text": text[:3500]})
    except Exception:
        pass


def tg_send(text: str) -> int:
    payload = {
        "chat_id": GROUP_ID,
        "message_thread_id": TOPIC_ID,
        "text": text,
        "disable_web_page_preview": True,
        "parse_mode": "HTML",
    }
    res = tg_call("sendMessage", payload)
    return int(res["message_id"])


def tg_send_photo_url(photo_url: str, caption: str) -> int:
    payload = {
        "chat_id": GROUP_ID,
        "message_thread_id": TOPIC_ID,
        "photo": bust(photo_url),
        "caption": caption[:1024],
        "parse_mode": "HTML",
    }
    res = tg_call("sendPhoto", payload)
    return int(res["message_id"])


def tg_send_photo_upload(image_bytes: bytes, caption: str, filename: str = "shot.jpg") -> int:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is empty. Set BOT_TOKEN env var on host.")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    data = {
        "chat_id": str(GROUP_ID),
        "message_thread_id": str(TOPIC_ID),
        "caption": caption[:1024],
        "parse_mode": "HTML",
    }
    files = {"photo": (filename, image_bytes)}
    r = requests.post(url, data=data, files=files, timeout=35)
    r.raise_for_status()
    out = r.json()
    if not out.get("ok"):
        raise RuntimeError(f"Telegram API error: {out}")
    return int(out["result"]["message_id"])


def download_image(url: str) -> bytes:
    u = bust(url) or url
    headers = {
        "User-Agent": UA,
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    r = requests.get(u, headers=headers, timeout=25)
    r.raise_for_status()
    return r.content


def tg_send_photo_best(photo_url: str, caption: str) -> int:
    """
    1) Скачиваем картинку сами (с cache-busting) и отправляем как upload (максимум свежести).
    2) Если не получилось — fallback на URL.
    """
    try:
        img = download_image(photo_url)
        return tg_send_photo_upload(img, caption, filename=f"shot_{ts()}.jpg")
    except Exception as e:
        notify_admin(f"Photo upload fallback to URL. Reason: {e}")
        return tg_send_photo_url(photo_url, caption)


# ========== KICK ==========
def kick_fetch() -> dict:
    r = requests.get(KICK_API_URL, headers=HEADERS_JSON, timeout=25)
    r.raise_for_status()
    data = r.json()

    ls = data.get("livestream") or {}
    is_live = bool(ls.get("is_live"))

    title = ls.get("session_title") or ls.get("stream_title") or None
    viewers = ls.get("viewer_count") or ls.get("viewers") or None

    cat = None
    cats = ls.get("categories") or []
    if isinstance(cats, list) and cats:
        cat = (cats[0] or {}).get("name") or None

    thumb = None
    th = ls.get("thumbnail") or {}
    if isinstance(th, dict):
        thumb = th.get("url") or th.get("src") or None
    if not thumb:
        thumb = ls.get("thumbnail_url") or None

    return {"live": is_live, "title": title, "category": cat, "viewers": viewers, "thumb": thumb}


# ========== VK (best-effort HTML parse) ==========
def _find_container_with_streaminfo(obj):
    if isinstance(obj, dict):
        if "streamInfo" in obj and isinstance(obj.get("streamInfo"), dict):
            return obj
        for v in obj.values():
            found = _find_container_with_streaminfo(v)
            if found:
                return found
    elif isinstance(obj, list):
        for v in obj:
            found = _find_container_with_streaminfo(v)
            if found:
                return found
    return None


def vk_fetch_best_effort() -> dict:
    r = requests.get(VK_PUBLIC_URL, headers=HEADERS_HTML, timeout=25, allow_redirects=True)
    r.raise_for_status()
    html = r.text

    title = None
    category = None
    viewers = None
    thumb = None
    live = False

    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL | re.IGNORECASE)
    if m:
        try:
            data = json.loads(m.group(1))
            container = _find_container_with_streaminfo(data)
            if container:
                ch = container.get("channelInfo") or {}
                si = container.get("streamInfo") or {}

                status = (ch.get("status") or "").upper()
                live = status in {"ONLINE", "LIVE", "STREAMING"}

                title = si.get("title") or title
                cat_obj = si.get("category") or {}
                category = cat_obj.get("title") or category
                cnt = si.get("counters") or {}
                viewers = cnt.get("viewers") or viewers

                if isinstance(viewers, int) and viewers > 0:
                    live = True
        except Exception:
            pass

    m_img = re.search(r'<meta[^>]+property="og:image"[^>]+content="([^"]+)"', html, re.IGNORECASE)
    if m_img:
        thumb = m_img.group(1).strip()

    m_title = re.search(r'<meta[^>]+property="og:title"[^>]+content="([^"]+)"', html, re.IGNORECASE)
    if not title and m_title:
        title = m_title.group(1).strip()

    return {"live": bool(live), "title": title, "category": category, "viewers": viewers, "thumb": thumb}


# ========== MESSAGE BUILDERS ==========
def choose_best_thumb(kick: dict, vk: dict) -> str | None:
    if kick.get("live") and kick.get("thumb"):
        return kick["thumb"]
    if vk.get("live") and vk.get("thumb"):
        return vk["thumb"]
    return None


def build_caption(prefix: str, kick: dict, vk: dict) -> str:
    if kick.get("live"):
        kick_block = (
            f"<b>Kick:</b> {esc(kick.get('category'))} — {esc(kick.get('title'))}\n"
            f"<b>Зрителей (Kick):</b> {fmt_viewers(kick.get('viewers'))}"
        )
    else:
        kick_block = "<b>Kick:</b> OFF\n<b>Зрителей (Kick):</b> —"

    if vk.get("live"):
        vk_block = (
            f"<b>VK:</b> {esc(vk.get('category'))} — {esc(vk.get('title'))}\n"
            f"<b>Зрителей (VK):</b> {fmt_viewers(vk.get('viewers'))}"
        )
    else:
        vk_block = "<b>VK:</b> OFF\n<b>Зрителей (VK):</b> —"

    return (
        f"{prefix}\n\n"
        f"{kick_block}\n\n"
        f"{vk_block}\n\n"
        f"Kick: {KICK_PUBLIC_URL}\n"
        f"VK: {VK_PUBLIC_URL}"
    )


def build_end_text(st: dict) -> str:
    started_at = st.get("started_at")
    dur = "—"
    if started_at:
        try:
            start_dt = datetime.fromisoformat(started_at)
            dur = fmt_duration(int((now_utc() - start_dt).total_seconds()))
        except Exception:
            pass

    viewers = st.get("kick_viewers") or st.get("vk_viewers") or "—"
    return (
        "Стрим Глад Валакаса закончился\n"
        f"Зрителей на стриме: {viewers}\n"
        f"Длительность: {dur}\n\n"
        f"Kick: {KICK_PUBLIC_URL}\n"
        f"VK: {VK_PUBLIC_URL}"
    )


# ========== MAIN LOOP ==========
def main():
    st = load_state()

    if not st.get("startup_ping_sent"):
        try:
            tg_send("✅ StreamAlertValakas запущен (ping).")
            st["startup_ping_sent"] = True
            save_state(st)
        except Exception as e:
            notify_admin(f"Startup ping failed: {e}")

    while True:
        try:
            kick = kick_fetch()
        except Exception as e:
            kick = {"live": False, "title": None, "category": None, "viewers": None, "thumb": None}
            notify_admin(f"Kick fetch error: {e}")

        try:
            vk = vk_fetch_best_effort()
        except Exception as e:
            vk = {"live": False, "title": None, "category": None, "viewers": None, "thumb": None}
            notify_admin(f"VK fetch error: {e}")

        prev_any = bool(st.get("any_live"))
        prev_kick = bool(st.get("kick_live"))
        prev_vk = bool(st.get("vk_live"))

        any_live = bool(kick["live"] or vk["live"])

        # partial end notifications (text only)
        try:
            if prev_kick and (not kick["live"]) and vk["live"]:
                tg_send(f"Kick-стрим закончился, на VK продолжается:\n{VK_PUBLIC_URL}")
            if prev_vk and (not vk["live"]) and kick["live"]:
                tg_send(f"VK-стрим закончился, на Kick продолжается:\n{KICK_PUBLIC_URL}")
        except Exception as e:
            notify_admin(f"Partial end notify error: {e}")

        # START
        if (not prev_any) and any_live:
            if ts() - int(st.get("last_start_sent_ts") or 0) >= START_DEDUP_SEC:
                st["started_at"] = now_utc().isoformat()
                caption = build_caption("🧩 Глад Валакас завёл стрим!", kick, vk)
                thumb = choose_best_thumb(kick, vk)
                try:
                    if thumb:
                        tg_send_photo_best(thumb, caption)
                    else:
                        tg_send(caption)
                    st["last_start_sent_ts"] = ts()
                except Exception as e:
                    notify_admin(f"Start send error: {e}")

        # CHANGE (only title/category changed)
        changed = False
        if kick["live"] and ((kick.get("title") != st.get("kick_title")) or (kick.get("category") != st.get("kick_cat"))):
            changed = True
        if vk["live"] and ((vk.get("title") != st.get("vk_title")) or (vk.get("category") != st.get("vk_cat"))):
            changed = True

        if any_live and prev_any and changed:
            if ts() - int(st.get("last_change_sent_ts") or 0) >= CHANGE_DEDUP_SEC:
                caption = build_caption("🔁 Обновление стрима (название/категория)", kick, vk)
                thumb = choose_best_thumb(kick, vk)
                try:
                    if thumb:
                        tg_send_photo_best(thumb, caption)
                    else:
                        tg_send(caption)
                    st["last_change_sent_ts"] = ts()
                except Exception as e:
                    notify_admin(f"Change send error: {e}")

        # END (text only)
        if prev_any and (not any_live):
            try:
                st["kick_viewers"] = st.get("kick_viewers") or kick.get("viewers")
                st["vk_viewers"] = st.get("vk_viewers") or vk.get("viewers")
                tg_send(build_end_text(st))
            except Exception as e:
                notify_admin(f"End send error: {e}")
            st["started_at"] = None

        # update state
        st["any_live"] = any_live
        st["kick_live"] = bool(kick["live"])
        st["vk_live"] = bool(vk["live"])

        st["kick_title"] = kick.get("title")
        st["kick_cat"] = kick.get("category")
        st["vk_title"] = vk.get("title")
        st["vk_cat"] = vk.get("category")

        st["kick_viewers"] = kick.get("viewers")
        st["vk_viewers"] = vk.get("viewers")

        save_state(st)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
