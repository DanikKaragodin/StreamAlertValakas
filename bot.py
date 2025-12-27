import os
import re
import json
import time
import random
import subprocess
import threading
import tempfile
import traceback
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

START_DEDUP_SEC = int(os.getenv("START_DEDUP_SEC", "120"))
CHANGE_DEDUP_SEC = int(os.getenv("CHANGE_DEDUP_SEC", "20"))

BOOT_STATUS_ENABLED = os.getenv("BOOT_STATUS_ENABLED", "1").strip() not in {"0", "false", "False"}
BOOT_STATUS_DEDUP_SEC = int(os.getenv("BOOT_STATUS_DEDUP_SEC", "300"))

# Commands
COMMANDS_ENABLED = os.getenv("COMMANDS_ENABLED", "1").strip() not in {"0", "false", "False"}
COMMAND_POLL_TIMEOUT = int(os.getenv("COMMAND_POLL_TIMEOUT", "20"))
COMMAND_HTTP_TIMEOUT = int(os.getenv("COMMAND_HTTP_TIMEOUT", "30"))
STATUS_COMMANDS = {"/status", "/stream", "/patok", "/state", "/стрим", "/паток"}

# Admin
ADMIN_ID = 417850992
ADMIN_COMMANDS = {"/admin", "/admin_reset_offset"}

# Auto-recovery (commands watchdog)
COMMANDS_WATCHDOG_ENABLED = os.getenv("COMMANDS_WATCHDOG_ENABLED", "1").strip() not in {"0", "false", "False"}
COMMANDS_WATCHDOG_SILENCE_SEC = int(os.getenv("COMMANDS_WATCHDOG_SILENCE_SEC", "240"))
COMMANDS_WATCHDOG_COOLDOWN_SEC = int(os.getenv("COMMANDS_WATCHDOG_COOLDOWN_SEC", "900"))
COMMANDS_WATCHDOG_PING_ENABLED = os.getenv("COMMANDS_WATCHDOG_PING_ENABLED", "1").strip() not in {"0", "false", "False"}

# If NO stream anywhere: message on start + message on command
NO_STREAM_ON_START_MESSAGE = os.getenv("NO_STREAM_ON_START_MESSAGE", "1").strip() not in {"0", "false", "False"}
NO_STREAM_START_DEDUP_SEC = int(os.getenv("NO_STREAM_START_DEDUP_SEC", "3600"))

# HTTP retry strategy
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "4"))
HTTP_BACKOFF_BASE = float(os.getenv("HTTP_BACKOFF_BASE", "1.6"))
HTTP_BACKOFF_MAX = float(os.getenv("HTTP_BACKOFF_MAX", "15"))
HTTP_JITTER = os.getenv("HTTP_JITTER", "1").strip() not in {"0", "false", "False"}

LOOP_CRASH_SLEEP = int(os.getenv("LOOP_CRASH_SLEEP", "5"))

# ffmpeg
FFMPEG_ENABLED = os.getenv("FFMPEG_ENABLED", "1").strip() not in {"0", "false", "False"}
FFMPEG_BIN = os.getenv("FFMPEG_BIN", "ffmpeg").strip()
FFMPEG_TIMEOUT_SEC = int(os.getenv("FFMPEG_TIMEOUT_SEC", "18"))
FFMPEG_SEEK_SEC = float(os.getenv("FFMPEG_SEEK_SEC", "3"))
FFMPEG_SCALE = os.getenv("FFMPEG_SCALE", "1280:-1").strip()

MAX_TITLE_LEN = int(os.getenv("MAX_TITLE_LEN", "180"))
MAX_GAME_LEN = int(os.getenv("MAX_GAME_LEN", "120"))

END_CONFIRM_STREAK = int(os.getenv("END_CONFIRM_STREAK", "2"))

# 409 notify dedup
NOTIFY_409_EVERY_SEC = 6 * 60 * 60  # 6 hours


# ========== URLS ==========
KICK_API_URL = f"https://kick.com/api/v1/channels/{KICK_SLUG}"
KICK_PUBLIC_URL = f"https://kick.com/{KICK_SLUG}"
VK_PUBLIC_URL = f"https://live.vkvideo.ru/{VK_SLUG}"


UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
HEADERS_JSON = {"User-Agent": UA, "Accept": "application/json,text/plain,*/*"}
HEADERS_HTML = {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}

STATE_LOCK = threading.Lock()
SESSION = requests.Session()


# ========== COMMON HELPERS ==========
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


def backoff_sleep(attempt: int) -> None:
    delay = min((HTTP_BACKOFF_BASE ** attempt), HTTP_BACKOFF_MAX)
    if HTTP_JITTER:
        delay *= random.uniform(0.85, 1.35)
    time.sleep(delay)


def http_request(method: str, url: str, *, headers=None, json_body=None, data=None, files=None, timeout=25, allow_redirects=True) -> requests.Response:
    last_exc = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            r = SESSION.request(
                method,
                url,
                headers=headers,
                json=json_body,
                data=data,
                files=files,
                timeout=timeout,
                allow_redirects=allow_redirects,
            )

            if r.status_code in (429, 500, 502, 503, 504):
                if attempt == HTTP_RETRIES:
                    r.raise_for_status()
                backoff_sleep(attempt)
                continue

            r.raise_for_status()
            return r

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError) as e:
            last_exc = e
            if attempt == HTTP_RETRIES:
                raise
            backoff_sleep(attempt)
        except requests.exceptions.HTTPError as e:
            last_exc = e
            if attempt == HTTP_RETRIES:
                raise
            backoff_sleep(attempt)

    raise last_exc


def is_telegram_conflict_409(exc: Exception) -> bool:
    return (
        isinstance(exc, requests.exceptions.HTTPError)
        and getattr(exc, "response", None) is not None
        and int(getattr(exc.response, "status_code", 0) or 0) == 409
    )


# ========== STATE (SAFE + ATOMIC) ==========
def default_state() -> dict:
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
        "last_boot_status_ts": 0,

        "last_no_stream_start_ts": 0,

        "updates_offset": 0,

        # commands watchdog
        "last_command_seen_ts": 0,
        "last_commands_recover_ts": 0,
        "last_updates_poll_ts": 0,

        # end confirmation
        "end_streak": 0,

        # anti-spam for 409
        "last_409_notify_ts": 0,

        # remember your private chat id once seen
        "admin_private_chat_id": 0,
    }


def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return default_state()

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            raw = f.read()
        if not raw.strip():
            return default_state()
        st = json.loads(raw)
        if not isinstance(st, dict):
            return default_state()
    except Exception:
        return default_state()

    st.setdefault("last_boot_status_ts", 0)
    st.setdefault("updates_offset", 0)
    st.setdefault("last_no_stream_start_ts", 0)

    st.setdefault("last_command_seen_ts", 0)
    st.setdefault("last_commands_recover_ts", 0)
    st.setdefault("last_updates_poll_ts", 0)

    st.setdefault("end_streak", 0)
    st.setdefault("last_409_notify_ts", 0)
    st.setdefault("admin_private_chat_id", 0)
    return st


def save_state(state: dict) -> None:
    d = os.path.dirname(STATE_FILE) or "."
    os.makedirs(d, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(prefix="state_", suffix=".json", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, STATE_FILE)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


# ========== TELEGRAM ==========
def tg_api_url(method: str) -> str:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is empty. Set BOT_TOKEN env var on host.")
    return f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"


def tg_call(method: str, payload: dict) -> dict:
    url = tg_api_url(method)
    r = http_request("POST", url, json_body=payload, timeout=25)
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")
    return data["result"]


def notify_admin(text: str) -> None:
    # only your private chat if known; otherwise fallback to your user id
    try:
        with STATE_LOCK:
            st = load_state()
            chat_id = int(st.get("admin_private_chat_id") or 0)

        target = chat_id if chat_id != 0 else ADMIN_ID
        tg_call("sendMessage", {"chat_id": target, "text": text[:3500]})
    except Exception:
        pass


def notify_409_dedup(text: str) -> None:
    now = ts()
    with STATE_LOCK:
        st = load_state()
        last = int(st.get("last_409_notify_ts") or 0)
        if now - last < NOTIFY_409_EVERY_SEC:
            return
        st["last_409_notify_ts"] = now
        save_state(st)
    notify_admin(text)


def tg_drop_pending_updates_safe() -> None:
    try:
        tg_call("deleteWebhook", {"drop_pending_updates": True})
    except Exception as e:
        notify_admin(f"tg_drop_pending_updates_safe failed: {e}")


def tg_get_webhook_info() -> dict:
    return tg_call("getWebhookInfo", {})


def tg_set_my_commands(commands: list, scope: dict | None = None) -> None:
    payload = {"commands": commands}
    if scope is not None:
        payload["scope"] = scope
    tg_call("setMyCommands", payload)


def setup_commands_visibility() -> None:
    """
    Делает так:
    - в группах: только обычные команды
    - в твоей личке: обычные + админские
    """
    public_cmds = [
        {"command": "stream", "description": "Текущий статус патока"},
        {"command": "status", "description": "Текущий статус патока"},
        {"command": "patok", "description": "Текущий статус патока"},
        {"command": "state", "description": "Состояние бота"},
    ]
    admin_cmds = [
        {"command": "admin", "description": "Диагностика (только админ)"},
        {"command": "admin_reset_offset", "description": "Сброс offset polling (только админ)"},
    ]

    # 1) Для групп/супергрупп
    tg_set_my_commands(public_cmds, scope={"type": "all_group_chats"})

    # 2) Для твоей лички — только если бот уже знает chat_id
    with STATE_LOCK:
        st = load_state()
        admin_chat = int(st.get("admin_private_chat_id") or 0)

    if admin_chat != 0:
        tg_set_my_commands(public_cmds + admin_cmds, scope={"type": "chat", "chat_id": admin_chat})


def tg_get_updates(offset: int, timeout: int) -> list:
    url = tg_api_url("getUpdates")
    payload = {"offset": int(offset), "timeout": int(timeout), "allowed_updates": ["message"]}
    r = http_request("POST", url, json_body=payload, timeout=COMMAND_HTTP_TIMEOUT)
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram getUpdates error: {data}")
    return data.get("result", [])


def tg_send_to(chat_id: int, thread_id: int | None, text: str, reply_to: int | None = None) -> int:
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True, "parse_mode": "HTML"}
    if thread_id is not None:
        payload["message_thread_id"] = int(thread_id)
    if reply_to is not None:
        payload["reply_to_message_id"] = int(reply_to)
    res = tg_call("sendMessage", payload)
    return int(res["message_id"])


def tg_send(text: str) -> int:
    return tg_send_to(GROUP_ID, TOPIC_ID, text, reply_to=None)


def tg_send_photo_url_to(chat_id: int, thread_id: int | None, photo_url: str, caption: str, reply_to: int | None = None) -> int:
    payload = {"chat_id": chat_id, "photo": bust(photo_url), "caption": caption[:1024], "parse_mode": "HTML"}
    if thread_id is not None:
        payload["message_thread_id"] = int(thread_id)
    if reply_to is not None:
        payload["reply_to_message_id"] = int(reply_to)
    res = tg_call("sendPhoto", payload)
    return int(res["message_id"])


def tg_send_photo_upload_to(chat_id: int, thread_id: int | None, image_bytes: bytes, caption: str, filename: str, reply_to: int | None = None) -> int:
    url = tg_api_url("sendPhoto")
    data = {"chat_id": str(chat_id), "caption": caption[:1024], "parse_mode": "HTML"}
    if thread_id is not None:
        data["message_thread_id"] = str(thread_id)
    if reply_to is not None:
        data["reply_to_message_id"] = str(reply_to)

    files = {"photo": (filename, image_bytes)}
    r = http_request("POST", url, data=data, files=files, timeout=35)
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
    r = http_request("GET", u, headers=headers, timeout=25)
    return r.content


def tg_send_photo_best_to(chat_id: int, thread_id: int | None, photo_url: str, caption: str, reply_to: int | None = None) -> int:
    try:
        img = download_image(photo_url)
        return tg_send_photo_upload_to(chat_id, thread_id, img, caption, filename=f"thumb_{ts()}.jpg", reply_to=reply_to)
    except Exception as e:
        notify_admin(f"Photo upload fallback to URL. Reason: {e}")
        return tg_send_photo_url_to(chat_id, thread_id, photo_url, caption, reply_to=reply_to)


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
        FFMPEG_BIN, "-hide_banner", "-loglevel", "error", "-nostdin",
        "-ss", str(FFMPEG_SEEK_SEC),
        "-i", playback_url,
        "-vframes", "1",
        "-vf", f"scale={FFMPEG_SCALE}",
        "-f", "image2pipe",
        "-vcodec", "mjpeg",
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
    r = http_request("GET", KICK_API_URL, headers=HEADERS_JSON, timeout=25)
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
    r = http_request("GET", VK_PUBLIC_URL, headers=HEADERS_HTML, timeout=25, allow_redirects=True)
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

    return {"live": bool(live), "title": trim(title, MAX_TITLE_LEN), "category": trim(category, MAX_GAME_LEN), "viewers": viewers, "thumb": thumb}


# ========== MESSAGES ==========
def build_caption(prefix: str, st: dict, kick: dict, vk: dict) -> str:
    running = fmt_running_line(st)

    if kick.get("live"):
        kick_block = (
            f"<b>Kick:</b> Игра - {esc(kick.get('category'))}\n"
            f"<b>Название патока:</b> {esc(kick.get('title'))}\n"
            f"<b>Зрителей (Kick):</b> {fmt_viewers(kick.get('viewers'))}"
        )
    else:
        kick_block = "<b>Kick:</b> OFF\n<b>Зрителей (Kick):</b> —"

    if vk.get("live"):
        vk_block = (
            f"<b>VK:</b> Игра - {esc(vk.get('category'))}\n"
            f"<b>Название патока:</b> {esc(vk.get('title'))}\n"
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
        "Паток Глад Валакаса закончился\n"
        f"Длительность: {dur}\n"
        f"Зрителей на патоке: {viewers}\n\n"
        f"Kick: {KICK_PUBLIC_URL}\n"
        f"VK: {VK_PUBLIC_URL}"
    )


def build_no_stream_text(prefix: str = "Сейчас на канале Глад Валакас патока нет!") -> str:
    return f"{prefix}\n\nKick: {KICK_PUBLIC_URL}\nVK: {VK_PUBLIC_URL}"


def set_started_at_from_kick(st: dict, kick: dict) -> None:
    if not kick.get("live"):
        return
    kdt = parse_kick_created_at(kick.get("created_at"))
    if kdt:
        st["started_at"] = kdt.isoformat()


def send_status_with_screen_to(prefix: str, st: dict, kick: dict, vk: dict, chat_id: int, thread_id: int | None, reply_to: int | None) -> None:
    caption = build_caption(prefix, st, kick, vk)

    shot = screenshot_from_m3u8(kick.get("playback_url")) if kick.get("live") else None
    if shot:
        tg_send_photo_upload_to(chat_id, thread_id, shot, caption, filename=f"kick_live_{ts()}.jpg", reply_to=reply_to)
        return

    if kick.get("live") and kick.get("thumb"):
        tg_send_photo_best_to(chat_id, thread_id, kick["thumb"], caption, reply_to=reply_to)
        return
    if vk.get("live") and vk.get("thumb"):
        tg_send_photo_best_to(chat_id, thread_id, vk["thumb"], caption, reply_to=reply_to)
        return

    tg_send_to(chat_id, thread_id, caption, reply_to=reply_to)


def send_status_with_screen(prefix: str, st: dict, kick: dict, vk: dict) -> None:
    send_status_with_screen_to(prefix, st, kick, vk, GROUP_ID, TOPIC_ID, reply_to=None)


# ========== COMMANDS ==========
def is_status_command(text: str) -> bool:
    if not text:
        return False
    t = text.strip().split()[0].split("@")[0]
    return t in STATUS_COMMANDS


def is_private_chat(msg: dict) -> bool:
    ch = msg.get("chat") or {}
    return (ch.get("type") == "private")


def is_admin_msg(msg: dict) -> bool:
    fr = msg.get("from") or {}
    uid = fr.get("id")
    return isinstance(uid, int) and uid == ADMIN_ID


def build_admin_diag_text(st: dict, webhook_info: dict) -> str:
    url = ""
    pend = ""
    try:
        url = webhook_info.get("url", "")
        pend = str(webhook_info.get("pending_update_count", ""))
    except Exception:
        url = str(webhook_info)

    return (
        "<b>Admin diag</b>\n"
        f"any_live: {st.get('any_live')}\n"
        f"kick_live: {st.get('kick_live')}\n"
        f"vk_live: {st.get('vk_live')}\n"
        f"started_at: {esc(st.get('started_at'))}\n\n"
        f"last_updates_poll_ts: {st.get('last_updates_poll_ts')}\n"
        f"last_command_seen_ts: {st.get('last_command_seen_ts')}\n"
        f"last_commands_recover_ts: {st.get('last_commands_recover_ts')}\n"
        f"updates_offset: {st.get('updates_offset')}\n\n"
        f"webhook_url: {esc(url)}\n"
        f"pending_update_count: {esc(pend)}\n"
    )


def commands_loop_forever():
    while True:
        try:
            commands_loop_once()
        except Exception as e:
            if is_telegram_conflict_409(e):
                notify_409_dedup("⚠️ Telegram 409 Conflict (getUpdates): есть другой polling на этом токене. Проверь, не запущено ли где-то ещё.")
                time.sleep(60)
                continue
            notify_admin(f"commands_loop crashed: {e}\n{traceback.format_exc()[:3000]}")
            time.sleep(LOOP_CRASH_SLEEP)


def commands_loop_once():
    if not COMMANDS_ENABLED:
        time.sleep(5)
        return

    with STATE_LOCK:
        st = load_state()
        offset = int(st.get("updates_offset") or 0)

    updates = tg_get_updates(offset=offset, timeout=COMMAND_POLL_TIMEOUT)

    with STATE_LOCK:
        st = load_state()
        st["last_updates_poll_ts"] = ts()
        save_state(st)

    max_update_id = None
    for upd in updates:
        uid = upd.get("update_id")
        if isinstance(uid, int):
            max_update_id = uid if (max_update_id is None or uid > max_update_id) else max_update_id

        msg = upd.get("message") or {}
        text = msg.get("text") or ""
        if not text:
            continue

        # remember your private chat id once you write the bot in private
        if is_private_chat(msg) and is_admin_msg(msg):
            with STATE_LOCK:
                st = load_state()
                st["admin_private_chat_id"] = int((msg.get("chat") or {}).get("id") or 0)
                save_state(st)
            # after we know chat id, we can configure commands visibility
            try:
                setup_commands_visibility()
            except Exception:
                pass

        chat = msg.get("chat") or {}
        chat_id = chat.get("id")
        if not isinstance(chat_id, int):
            continue

        thread_id = msg.get("message_thread_id")
        thread_id = int(thread_id) if isinstance(thread_id, int) else None

        reply_to = msg.get("message_id")
        reply_to = int(reply_to) if isinstance(reply_to, int) else None

        # ---- ADMIN (private only + admin id only) ----
        cmd = text.strip().split()[0].split("@")[0]
        if cmd in ADMIN_COMMANDS:
            if not (is_private_chat(msg) and is_admin_msg(msg)):
                continue

            if cmd == "/admin_reset_offset":
                with STATE_LOCK:
                    st = load_state()
                    st["updates_offset"] = 0
                    save_state(st)
                tg_send_to(chat_id, None, "OK: updates_offset сброшен в 0.", reply_to=reply_to)
                continue

            # /admin
            with STATE_LOCK:
                st = load_state()
            try:
                wh = tg_get_webhook_info()
            except Exception as e:
                wh = {"error": str(e)}
            tg_send_to(chat_id, None, build_admin_diag_text(st, wh), reply_to=reply_to)
            continue

        # ---- STATUS ----
        if not is_status_command(text):
            continue

        with STATE_LOCK:
            st = load_state()
            st["last_command_seen_ts"] = ts()
            save_state(st)

        try:
            kick = kick_fetch()
        except Exception as e:
            kick = {"live": False, "title": None, "category": None, "viewers": None, "thumb": None, "created_at": None, "playback_url": None}
            notify_admin(f"Kick fetch (command) error: {e}")

        try:
            vk = vk_fetch_best_effort()
        except Exception as e:
            vk = {"live": False, "title": None, "category": None, "viewers": None, "thumb": None}
            notify_admin(f"VK fetch (command) error: {e}")

        with STATE_LOCK:
            st2 = load_state()
            set_started_at_from_kick(st2, kick)
            st2["kick_title"] = kick.get("title")
            st2["kick_cat"] = kick.get("category")
            st2["vk_title"] = vk.get("title")
            st2["vk_cat"] = vk.get("category")
            st2["kick_viewers"] = kick.get("viewers")
            st2["vk_viewers"] = vk.get("viewers")
            save_state(st2)

        if not (kick.get("live") or vk.get("live")):
            tg_send_to(chat_id, thread_id, build_no_stream_text("Сейчас на канале Глад Валакас патока нет!"), reply_to=reply_to)
        else:
            send_status_with_screen_to("📌 Текущее состояние патока", st2, kick, vk, chat_id, thread_id, reply_to)

    if max_update_id is not None:
        with STATE_LOCK:
            st = load_state()
            st["updates_offset"] = int(max_update_id) + 1
            save_state(st)


def commands_watchdog_forever():
    while True:
        try:
            if not (COMMANDS_ENABLED and COMMANDS_WATCHDOG_ENABLED):
                time.sleep(10)
                continue

            with STATE_LOCK:
                st = load_state()
                last_poll = int(st.get("last_updates_poll_ts") or 0)
                last_recover = int(st.get("last_commands_recover_ts") or 0)

            now = ts()
            if last_poll == 0:
                time.sleep(10)
                continue

            silent = (now - last_poll) >= COMMANDS_WATCHDOG_SILENCE_SEC
            cooldown_ok = (now - last_recover) >= COMMANDS_WATCHDOG_COOLDOWN_SEC

            if silent and cooldown_ok:
                notify_admin("⚠️ Watchdog: getUpdates давно не отрабатывал, делаю восстановление...")
                tg_drop_pending_updates_safe()
                with STATE_LOCK:
                    st = load_state()
                    st["updates_offset"] = 0
                    st["last_commands_recover_ts"] = now
                    save_state(st)

                if COMMANDS_WATCHDOG_PING_ENABLED:
                    try:
                        notify_admin("✅ Watchdog: восстановил polling команд.")
                    except Exception:
                        pass

        except Exception as e:
            notify_admin(f"commands_watchdog crashed: {e}\n{traceback.format_exc()[:3000]}")

        time.sleep(10)


# ========== MAIN LOOP ==========
def main_loop_forever():
    while True:
        try:
            main_loop()
        except Exception as e:
            notify_admin(f"main_loop crashed: {e}\n{traceback.format_exc()[:3000]}")
            time.sleep(LOOP_CRASH_SLEEP)


def main_loop():
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

    with STATE_LOCK:
        prev_st = load_state()
        prev_any_before_init = bool(prev_st.get("any_live"))

    if prev_any_before_init and (not any_live0):
        try:
            with STATE_LOCK:
                st_end = load_state()
            tg_send(build_end_text(st_end))
        except Exception as e:
            notify_admin(f"End-after-restart send error: {e}")

        with STATE_LOCK:
            st_end = load_state()
            st_end["started_at"] = None
            st_end["end_streak"] = 0
            save_state(st_end)

    with STATE_LOCK:
        st = load_state()
        st["any_live"] = any_live0
        st["kick_live"] = bool(kick0.get("live"))
        st["vk_live"] = bool(vk0.get("live"))
        set_started_at_from_kick(st, kick0)

        st["kick_title"] = kick0.get("title")
        st["kick_cat"] = kick0.get("category")
        st["vk_title"] = vk0.get("title")
        st["vk_cat"] = vk0.get("category")
        st["kick_viewers"] = kick0.get("viewers")
        st["vk_viewers"] = vk0.get("viewers")
        save_state(st)

    with STATE_LOCK:
        st = load_state()
        ping_sent = bool(st.get("startup_ping_sent"))

    if not ping_sent:
        try:
            with STATE_LOCK:
                st = load_state()
            tg_send("✅ StreamAlertValakas запущен (ping).\n" + fmt_running_line(st))
            with STATE_LOCK:
                st = load_state()
                st["startup_ping_sent"] = True
                save_state(st)
        except Exception as e:
            notify_admin(f"Startup ping failed: {e}")

    if NO_STREAM_ON_START_MESSAGE and (not any_live0):
        with STATE_LOCK:
            st = load_state()
            last_ts = int(st.get("last_no_stream_start_ts") or 0)

        if ts() - last_ts >= NO_STREAM_START_DEDUP_SEC:
            try:
                tg_send(build_no_stream_text("Сейчас на канале Глад Валакас патока нет!"))
            except Exception as e:
                notify_admin(f"No-stream-on-start send error: {e}")

            with STATE_LOCK:
                st = load_state()
                st["last_no_stream_start_ts"] = ts()
                save_state(st)

    if BOOT_STATUS_ENABLED and any_live0:
        try:
            with STATE_LOCK:
                st = load_state()
                can_send = ts() - int(st.get("last_boot_status_ts") or 0) >= BOOT_STATUS_DEDUP_SEC

            if can_send:
                with STATE_LOCK:
                    st = load_state()
                send_status_with_screen("ℹ️ Паток уже идёт (после рестарта)", st, kick0, vk0)
                with STATE_LOCK:
                    st = load_state()
                    st["last_boot_status_ts"] = ts()
                    save_state(st)
        except Exception as e:
            notify_admin(f"Boot status send error: {e}")

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

        with STATE_LOCK:
            st = load_state()
        prev_any = bool(st.get("any_live"))
        any_live = bool(kick.get("live") or vk.get("live"))

        with STATE_LOCK:
            st = load_state()
            set_started_at_from_kick(st, kick)
            if not any_live:
                st["end_streak"] = int(st.get("end_streak") or 0) + 1
            else:
                st["end_streak"] = 0
            save_state(st)

        if (not prev_any) and any_live:
            if ts() - int(st.get("last_start_sent_ts") or 0) >= START_DEDUP_SEC:
                with STATE_LOCK:
                    st = load_state()
                    if not st.get("started_at"):
                        st["started_at"] = now_utc().isoformat()
                    save_state(st)

                try:
                    with STATE_LOCK:
                        st = load_state()
                    # CHANGED: add rotating light emoji on both sides
                    send_status_with_screen("🚨🚨 🧩 Глад Валакас запустил паток! 🚨🚨", st, kick, vk)
                    with STATE_LOCK:
                        st = load_state()
                        st["last_start_sent_ts"] = ts()
                        save_state(st)
                except Exception as e:
                    notify_admin(f"Start send error: {e}")

        with STATE_LOCK:
            st = load_state()
        changed = False
        if kick.get("live") and ((kick.get("title") != st.get("kick_title")) or (kick.get("category") != st.get("kick_cat"))):
            changed = True
        if vk.get("live") and ((vk.get("title") != st.get("vk_title")) or (vk.get("category") != st.get("vk_cat"))):
            changed = True

        if any_live and prev_any and changed:
            if ts() - int(st.get("last_change_sent_ts") or 0) >= CHANGE_DEDUP_SEC:
                try:
                    with STATE_LOCK:
                        st = load_state()
                    send_status_with_screen("🔁 Обновление патока (название/категория)", st, kick, vk)
                    with STATE_LOCK:
                        st = load_state()
                        st["last_change_sent_ts"] = ts()
                        save_state(st)
                except Exception as e:
                    notify_admin(f"Change send error: {e}")

        with STATE_LOCK:
            st = load_state()
            end_streak = int(st.get("end_streak") or 0)

        if prev_any and (not any_live) and end_streak >= END_CONFIRM_STREAK:
            try:
                with STATE_LOCK:
                    st = load_state()
                    st["kick_viewers"] = st.get("kick_viewers") or kick.get("viewers")
                    st["vk_viewers"] = st.get("vk_viewers") or vk.get("viewers")
                    save_state(st)
                with STATE_LOCK:
                    st = load_state()
                tg_send(build_end_text(st))
            except Exception as e:
                notify_admin(f"End send error: {e}")

            with STATE_LOCK:
                st = load_state()
                st["started_at"] = None
                st["end_streak"] = 0
                save_state(st)

        with STATE_LOCK:
            st = load_state()
            st["any_live"] = any_live
            st["kick_live"] = bool(kick.get("live"))
            st["vk_live"] = bool(vk.get("live"))
            st["kick_title"] = kick.get("title")
            st["kick_cat"] = kick.get("category")
            st["vk_title"] = vk.get("title")
            st["vk_cat"] = vk.get("category")
            st["kick_viewers"] = kick.get("viewers")
            st["vk_viewers"] = vk.get("viewers")
            save_state(st)

        time.sleep(POLL_INTERVAL)


def main():
    tg_drop_pending_updates_safe()

    # try to set command visibility (works after bot knows your private chat_id)
    try:
        setup_commands_visibility()
    except Exception:
        pass

    if COMMANDS_ENABLED:
        threading.Thread(target=commands_loop_forever, daemon=True).start()
        threading.Thread(target=commands_watchdog_forever, daemon=True).start()

    main_loop_forever()


if __name__ == "__main__":
    main()
