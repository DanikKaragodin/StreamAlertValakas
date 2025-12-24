import os
import re
import json
import time
from datetime import datetime, timezone

import requests


# ========== CONFIG (ENV) ==========
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

GROUP_ID = int(os.getenv("GROUP_ID", "-1002977868330"))
TOPIC_ID = int(os.getenv("TOPIC_ID", "65114"))

KICK_SLUG = os.getenv("KICK_SLUG", "gladvalakaspwnz").strip()
VK_SLUG = os.getenv("VK_SLUG", "gladvalakas").strip()

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))
STATE_FILE = os.getenv("STATE_FILE", "state.json")

# optional: send errors to your private chat
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "").strip()


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

            # last known fields (for change detection)
            "kick_title": None,
            "kick_cat": None,
            "vk_title": None,
            "vk_cat": None,

            # last known viewers (not for triggering, just for display)
            "kick_viewers": None,
            "vk_viewers": None,
        }
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state: dict) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def tg_call(method: str, payload: dict) -> dict:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is empty. Set BOT_TOKEN env var on Bothost.")
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
    }
    res = tg_call("sendMessage", payload)
    return int(res["message_id"])


def tg_send_photo(photo_url: str, caption: str) -> int:
    # photo can be an HTTP URL; message_thread_id sends to the specific topic
    payload = {
        "chat_id": GROUP_ID,
        "message_thread_id": TOPIC_ID,
        "photo": photo_url,
        "caption": caption[:1024],  # Telegram caption limit for photos
    }
    res = tg_call("sendPhoto", payload)
    return int(res["message_id"])


# ========== KICK ==========
def kick_fetch() -> dict:
    """
    Expected in /api/v2/channels/{slug}:
      livestream.is_live
      livestream.session_title
      livestream.viewer_count
      livestream.categories[].name
      livestream.thumbnail.url or livestream.thumbnail.src
    """
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

    # 1) try __NEXT_DATA__
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

                # sometimes "online" is easiest to detect by viewers
                if isinstance(viewers, int) and viewers > 0:
                    live = True
        except Exception:
            pass

    # 2) fallback: og:image / og:title
    m_img = re.search(r'<meta[^>]+property="og:image"[^>]+content="([^"]+)"', html, re.IGNORECASE)
    if m_img:
        thumb = m_img.group(1).strip()

    m_title = re.search(r'<meta[^>]+property="og:title"[^>]+content="([^"]+)"', html, re.IGNORECASE)
    if not title and m_title:
        title = m_title.group(1).strip()

    return {"live": bool(live), "title": title, "category": category, "viewers": viewers, "thumb": thumb}


# ========== TEXT BUILDERS ==========
def fmt_viewers(v) -> str:
    return str(v) if isinstance(v, int) else "—"


def build_caption(prefix: str, kick: dict, vk: dict) -> str:
    # always show both lines
    kick_line = "Kick: OFF"
    if kick.get("live"):
        kick_line = f"Kick: {kick.get('category') or '—'} — {kick.get('title') or '—'}\nЗрителей (Kick): {fmt_viewers(kick.get('viewers'))}"

    vk_line = "VK: OFF"
    if vk.get("live"):
        vk_line = f"VK: {vk.get('category') or '—'} — {vk.get('title') or '—'}\nЗрителей (VK): {fmt_viewers(vk.get('viewers'))}"

    return (
        f"{prefix}\n\n"
        f"{kick_line}\n\n"
        f"{vk_line}\n\n"
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


def choose_best_thumb(kick: dict, vk: dict) -> str | None:
    # prefer Kick thumb (your rule), else VK thumb
    if kick.get("live") and kick.get("thumb"):
        return kick["thumb"]
    if vk.get("live") and vk.get("thumb"):
        return vk["thumb"]
    return None


# ========== MAIN LOOP ==========
def main():
    st = load_state()

    # startup ping once
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

        # START: send photo + caption
        if (not prev_any) and any_live:
            st["started_at"] = now_utc().isoformat()
            caption = build_caption("🧩 Глад Валакас завёл стрим!", kick, vk)
            thumb = choose_best_thumb(kick, vk)
            try:
                if thumb:
                    tg_send_photo(thumb, caption)
                else:
                    tg_send(caption)
            except Exception as e:
                notify_admin(f"Start send error: {e}")

        # CHANGE: send photo + caption only when title/category changed
        # (viewer count changes do NOT trigger)
        changed = False
        if kick["live"]:
            if (kick.get("title") != st.get("kick_title")) or (kick.get("category") != st.get("kick_cat")):
                changed = True
        if vk["live"]:
            if (vk.get("title") != st.get("vk_title")) or (vk.get("category") != st.get("vk_cat")):
                changed = True

        if any_live and prev_any and changed:
            caption = build_caption("🔁 Обновление стрима (название/категория)", kick, vk)
            thumb = choose_best_thumb(kick, vk)
            try:
                if thumb:
                    tg_send_photo(thumb, caption)
                else:
                    tg_send(caption)
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

        # update state for next iteration (save last title/category/viewers)
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
