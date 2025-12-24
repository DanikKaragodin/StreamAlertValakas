import os
import re
import json
import time
import subprocess
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

# Anti-duplicate if host briefly runs 2 instances
START_DEDUP_SEC = int(os.getenv("START_DEDUP_SEC", "120"))
CHANGE_DEDUP_SEC = int(os.getenv("CHANGE_DEDUP_SEC", "20"))

# ffmpeg
FFMPEG_ENABLED = os.getenv("FFMPEG_ENABLED", "1").strip() not in {"0", "false", "False"}
FFMPEG_BIN = os.getenv("FFMPEG_BIN", "ffmpeg").strip()
FFMPEG_TIMEOUT_SEC = int(os.getenv("FFMPEG_TIMEOUT_SEC", "18"))
FFMPEG_SEEK_SEC = float(os.getenv("FFMPEG_SEEK_SEC", "3"))  # small seek into live
FFMPEG_SCALE = os.getenv("FFMPEG_SCALE", "1280:-1").strip()  # reduce load

# Text truncation (Telegram caption limit is 1024 for photos) [page:0]
MAX_TITLE_LEN = int(os.getenv("MAX_TITLE_LEN", "180"))
MAX_GAME_LEN = int(os.getenv("MAX_GAME_LEN", "120"))


# ========== URLS ==========
KICK_API_URL = f"https://kick.com/api/v1/channels/{KICK_SLUG}"
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
    if not url:
        return None
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}t={ts()}"


def esc(s: str | None) -> str:
    return html_escape(s or "—", quote=False)


def trim(s: str | None, n: int) -> str | None:
    if not s:
        return s
    s = str(s).strip()
    return s if len(s) <= n else (s[: n - 1] + "…")


def fmt_viewers(v) -> str:
    return str(v) if isinstance(v, int) else "—"


def fmt_duration(seconds: int) -> str:
    seconds = max(0, int(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    return f"{h:02d} ч. {m:02d} мин."


def parse_kick_created_at(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def seconds_since_started(st: dict) -> int | None:
    started_at = st.get("started_at")
    if not started_at:
        return None
    try:
        start_dt = datetime.fromisoformat(started_at)
        return int((now_utc() - start_dt).total_seconds())
    except Exception:
        return None


def fmt_running_line(st: dict) -> str:
    sec = seconds_since_started(st)
    if sec is None:
        return "<b>Идёт:</b> —"
    return f"<b>Идёт:</b> {fmt_duration(sec)}"


# ========== STATE ==========
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


# ========== TELEGRAM ==========
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
    try:
        img = download_image(photo_url)
        return tg_send_photo_upload(img, caption, filename=f"thumb_{ts()}.jpg")
    except Exception as e:
        notify_admin(f"Photo upload fallback to URL. Reason: {e}")
        return tg_send_photo_url(photo_url, caption)


# ========== FFMPEG SCREENSHOT ==========
def ffmpeg_available() -> bool:
    try:
        r = subprocess.run([FFMPEG_BIN, "-version"], capture_output=True, text=True, timeout=5)
        return r.returncode == 0
    except Exception:
        return False


def screenshot_from_m3u8(playback_url: str) -> bytes | None:
    if not FFMPEG_ENABLED or not playback_url or not ffmpeg_available():
        return None

    cmd = [
        FFMPEG_BIN,
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-ss",
        str(FFMPEG_SEEK_SEC),
        "-i",
        playback_url,
        "-vframes",
        "1",
        "-vf",
        f"scale={FFMPEG_SCALE}",
        "-f",
        "image2pipe",
        "-vcodec",
        "mjpeg",
        "pipe:1",
    ]
    try:
        p = subprocess.run(cmd, capture_output=True, timeout=FFMPEG_TIMEOUT_SEC)
        if p.returncode != 0 or not p.stdout:
            return None
        return p.stdout
    except Exception:
        return None


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

    created_at = ls.get("created_at")

    thumb = None
    th = ls.get("thumbnail") or {}
    if isinstance(th, dict):
        thumb = th.get("url") or th.get("src") or None
    if not thumb:
        thumb = ls.get("thumbnail_url") or None

    playback_url = None
    sc = data.get("streamer_channel") or {}
    if isinstance(sc, dict):
        playback_url = sc.get("playback_url") or None

    return {
        "live": is_live,
        "title": trim(title, MAX_TITLE_LEN),
        "category": trim(cat, MAX_GAME_LEN),
        "viewers": viewers,
        "thumb": thumb,
        "created_at": created_at,
        "playback_url": playback_url,
    }


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

    return {
        "live": bool(live),
        "title": trim(title, MAX_TITLE_LEN),
        "category": trim(category, MAX_GAME_LEN),
        "viewers": viewers,
        "thumb": thumb,
    }


# ========== MESSAGE BUILDERS ==========
def build_caption(prefix: str, st: dict, kick: dict, vk: dict) -> str:
    running = fmt_running_line(st)

    if kick.get("live"):
        kick_block = (
            f"<b>Kick:</b> Игра - {esc(kick.get('category'))}\n"
            f"<b>Название стрима:</b> {esc(kick.get('title'))}\n"
            f"<b>Зрителей (Kick):</b> {fmt_viewers(kick.get('viewers'))}"
        )
    else:
        kick_block = "<b>Kick:</b> OFF\n<b>Зрителей (Kick):</b> —"

    if vk.get("live"):
        vk_block = (
            f"<b>VK:</b> Игра - {esc(vk.get('category'))}\n"
            f"<b>Название стрима:</b> {esc(vk.get('title'))}\n"
            f"<b>Зрителей (VK):</b> {fmt_viewers(vk.get('viewers'))}"
        )
    else:
        vk_block = "<b>VK:</b> OFF\n<b>Зрителей (VK):</b> —"

    return (
        f"{prefix}\n"
        f"{running}\n\n"
        f"{kick_block}\n\n"
        f"{vk_block}\n\n"
        f"Kick: {KICK_PUBLIC_URL}\n"
        f"VK: {VK_PUBLIC_URL}"
    )


def build_end_text(st: dict) -> str:
    sec = seconds_since_started(st)
    dur = fmt_duration(sec) if sec is not None else "—"
    viewers = st.get("kick_viewers") or st.get("vk_viewers") or "—"
    return (
        "Стрим Глад Валакаса закончился\n"
        f"Длительность: {dur}\n"
        f"Зрителей на стриме: {viewers}\n\n"
        f"Kick: {KICK_PUBLIC_URL}\n"
        f"VK: {VK_PUBLIC_URL}"
    )


def set_started_at_from_kick(st: dict, kick: dict) -> None:
    if not kick.get("live"):
        return
    kdt = parse_kick_created_at(kick.get("created_at"))
    if kdt:
        st["started_at"] = kdt.isoformat()


# ========== MAIN LOOP ==========
def main():
    st = load_state()

    # ---- Initial fetch BEFORE loop (so duration is correct even after restart)
    try:
        kick0 = kick_fetch()
    except Exception as e:
        kick0 = {"live": False, "title": None, "category": None, "viewers": None, "thumb": None, "created_at": None, "playback_url": None}
        notify_admin(f"Kick init fetch error: {e}")

    try:
        vk0 = vk_fetch_best_effort()
    except Exception as e:
        vk0 = {"live": False, "title": None, "category": None, "viewers": None, "thumb": None}
        notify_admin(f"VK init fetch error: {e}")

    any_live0 = bool(kick0.get("live") or vk0.get("live"))
    st["any_live"] = any_live0
    st["kick_live"] = bool(kick0.get("live"))
    st["vk_live"] = bool(vk0.get("live"))
    set_started_at_from_kick(st, kick0)

    # ---- IMPORTANT FIX: initialize "last known" fields to prevent "Обновление" on boot
    st["kick_title"] = kick0.get("title")
    st["kick_cat"] = kick0.get("category")
    st["vk_title"] = vk0.get("title")
    st["vk_cat"] = vk0.get("category")
    st["kick_viewers"] = kick0.get("viewers")
    st["vk_viewers"] = vk0.get("viewers")
    save_state(st)

    # ---- ping once
    if not st.get("startup_ping_sent"):
        try:
            msg = "✅ StreamAlertValakas запущен (ping).\n" + fmt_running_line(st)
            tg_send(msg)
            st["startup_ping_sent"] = True
            save_state(st)
        except Exception as e:
            notify_admin(f"Startup ping failed: {e}")

    while True:
        try:
            kick = kick_fetch()
        except Exception as e:
            kick = {"live": False, "title": None, "category": None, "viewers": None, "thumb": None, "created_at": None, "playback_url": None}
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

        # Keep started_at aligned with real Kick start whenever Kick is live
        set_started_at_from_kick(st, kick)

        # START
        if (not prev_any) and any_live:
            if ts() - int(st.get("last_start_sent_ts") or 0) >= START_DEDUP_SEC:
                if not st.get("started_at"):
                    st["started_at"] = now_utc().isoformat()

                caption = build_caption("🧩 Глад Валакас завёл стрим!", st, kick, vk)
                try:
                    shot = screenshot_from_m3u8(kick.get("playback_url")) if kick.get("live") else None
                    if shot:
                        tg_send_photo_upload(shot, caption, filename=f"kick_live_{ts()}.jpg")
                    else:
                        if kick.get("live") and kick.get("thumb"):
                            tg_send_photo_best(kick["thumb"], caption)
                        elif vk.get("live") and vk.get("thumb"):
                            tg_send_photo_best(vk["thumb"], caption)
                        else:
                            tg_send(caption)

                    st["last_start_sent_ts"] = ts()
                except Exception as e:
                    notify_admin(f"Start send error: {e}")

        # CHANGE only when title/category changed
        changed = False
        if kick["live"] and ((kick.get("title") != st.get("kick_title")) or (kick.get("category") != st.get("kick_cat"))):
            changed = True
        if vk["live"] and ((vk.get("title") != st.get("vk_title")) or (vk.get("category") != st.get("vk_cat"))):
            changed = True

        if any_live and prev_any and changed:
            if ts() - int(st.get("last_change_sent_ts") or 0) >= CHANGE_DEDUP_SEC:
                caption = build_caption("🔁 Обновление стрима (название/категория)", st, kick, vk)
                try:
                    shot = screenshot_from_m3u8(kick.get("playback_url")) if kick.get("live") else None
                    if shot:
                        tg_send_photo_upload(shot, caption, filename=f"kick_live_{ts()}.jpg")
                    else:
                        if kick.get("live") and kick.get("thumb"):
                            tg_send_photo_best(kick["thumb"], caption)
                        elif vk.get("live") and vk.get("thumb"):
                            tg_send_photo_best(vk["thumb"], caption)
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
