import os
import re
import json
import time
import random
import subprocess
import threading
import tempfile
import traceback
import shutil
import glob
from datetime import datetime, timezone, timedelta
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

# Disk cleanup
DISK_CHECK_INTERVAL = int(os.getenv("DISK_CHECK_INTERVAL", "100"))  # Check every 100 iterations
MAX_STATE_SIZE = 1024 * 50  # 50KB max for state file
TEMP_CLEANUP_AGE_SEC = 3600  # Clean temp files older than 1 hour
ERROR_DEDUP_SEC = 300  # 5 minutes between duplicate error notifications


# ✅ Bothost quota monitor (monitor project folder size, not host filesystem)
BOT_QUOTA_MB = int(os.getenv("BOT_QUOTA_MB", "500"))
BOT_WARN_PERCENT = float(os.getenv("BOT_WARN_PERCENT", "90"))
BOT_NOTIFY_COOLDOWN_SEC = int(os.getenv("BOT_NOTIFY_COOLDOWN_SEC", str(6 * 60 * 60)))  # 6h
BOT_TOP_FILES = int(os.getenv("BOT_TOP_FILES", "5"))


# ========== URLS ==========
KICK_API_URL = f"https://kick.com/api/v1/channels/{KICK_SLUG}"
KICK_PUBLIC_URL = f"https://kick.com/{KICK_SLUG}"
VK_PUBLIC_URL = f"https://live.vkvideo.ru/{VK_SLUG}"


UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
HEADERS_JSON = {"User-Agent": UA, "Accept": "application/json,text/plain,*/*"}
HEADERS_HTML = {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}

STATE_LOCK = threading.Lock()
SESSION = requests.Session()

# Error deduplication cache
last_error_notify = {}


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


# ========== DISK CLEANUP FUNCTIONS ==========
def cleanup_temp_files():
    """Очистка временных файлов"""
    try:
        # Очистка временных файлов ffmpeg
        temp_dirs = ["/tmp", "/var/tmp", "/dev/shm"]
        for temp_dir in temp_dirs:
            if os.path.exists(temp_dir):
                for pattern in ["ffmpeg-*", "tmp*", "*.mp4", "*.ts", "*.m3u8", "*.jpg", "*.jpeg", "*.png"]:
                    for file in glob.glob(os.path.join(temp_dir, pattern)):
                        try:
                            if os.path.isfile(file):
                                file_age = time.time() - os.path.getmtime(file)
                                if file_age > TEMP_CLEANUP_AGE_SEC:
                                    os.remove(file)
                        except Exception:
                            pass
    except Exception:
        pass



def cleanup_pycache():
    '''Удаляет __pycache__ и *.pyc в папке проекта.'''
    try:
        base = os.getcwd()
        for root, dirs, files in os.walk(base):
            if root.startswith('/proc') or root.startswith('/sys') or root.startswith('/dev'):
                continue
            if '__pycache__' in dirs:
                try:
                    shutil.rmtree(os.path.join(root, '__pycache__'), ignore_errors=True)
                except Exception:
                    pass
            for fn in files:
                if fn.endswith('.pyc') or fn.endswith('.pyo'):
                    try:
                        os.remove(os.path.join(root, fn))
                    except Exception:
                        pass
    except Exception:
        pass


def cleanup_old_state_backups():
    """Очистка старых backup файлов состояния"""
    try:
        dir_name = os.path.dirname(STATE_FILE) or "."
        for filename in os.listdir(dir_name):
            if filename.startswith("state_") and filename.endswith(".json"):
                filepath = os.path.join(dir_name, filename)
                try:
                    if os.path.isfile(filepath):
                        file_age = time.time() - os.path.getmtime(filepath)
                        if file_age > TEMP_CLEANUP_AGE_SEC:
                            os.remove(filepath)
                except Exception:
                    pass
    except Exception:
        pass


def check_disk_usage():
    """УСТАРЕЛО: в контейнере показывает диск хоста, а не квоту тарифа.
    Оставлено для совместимости, но больше не используется."""
    return 0



def fmt_bytes(n: int) -> str:
    n = int(n or 0)
    if n < 1024:
        return f"{n} B"
    if n < 1024**2:
        return f"{n/1024:.1f} KB"
    if n < 1024**3:
        return f"{n/1024**2:.1f} MB"
    return f"{n/1024**3:.2f} GB"


def dir_size_bytes(root: str) -> int:
    total = 0
    exclude_dirs = {"__pycache__", ".git", ".venv", "venv", "env", "node_modules"}
    for base, dirs, files in os.walk(root):
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        for fn in files:
            try:
                fp = os.path.join(base, fn)
                if os.path.islink(fp):
                    continue
                total += os.path.getsize(fp)
            except Exception:
                pass
    return total


def list_largest_files(root: str, topn: int = 5):
    items = []
    exclude_dirs = {"__pycache__", ".git", ".venv", "venv", "env", "node_modules"}
    for base, dirs, files in os.walk(root):
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        for fn in files:
            try:
                fp = os.path.join(base, fn)
                if os.path.islink(fp):
                    continue
                size = int(os.path.getsize(fp))
                rel = os.path.relpath(fp, root)
                items.append((size, rel))
            except Exception:
                pass
    items.sort(key=lambda x: x[0], reverse=True)
    return items[: max(0, int(topn))]


def quota_usage_for_bot():
    quota_bytes = int(BOT_QUOTA_MB) * 1024 * 1024
    used = dir_size_bytes(os.getcwd())
    percent = (used * 100.0 / quota_bytes) if quota_bytes else 0.0
    return percent, used, quota_bytes


def notify_admin_dedup(key: str, text: str):
    """Уведомление админа с дедупликацией"""
    now = ts()
    last = last_error_notify.get(key, 0)
    if now - last < ERROR_DEDUP_SEC:
        return
    last_error_notify[key] = now
    notify_admin(text)


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

        # end confirmation ✅ ИСПРАВЛЕНО: всегда сбрасывается при любом live
        "end_streak": 0,

        # anti-spam for 409
        "last_409_notify_ts": 0,

        # remember your private chat id once seen
        "admin_private_chat_id": 0,
        
        # disk cleanup tracking
        "last_disk_check_ts": 0,
        "last_temp_cleanup_ts": 0,

        # quota alert anti-spam
        "last_quota_notify_ts": 0,
        "end_sent_for_started_at": None,
        "end_sent_ts": 0,
        "stream_stats": None,
    }


def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return default_state()

    try:
        # Проверка размера файла
        if os.path.getsize(STATE_FILE) > MAX_STATE_SIZE:
            notify_admin_dedup("state_file_large", f"⚠️ Файл состояния слишком большой: {os.path.getsize(STATE_FILE)} байт")
            # Оставляем только основные поля
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                raw = f.read()
            if not raw.strip():
                return default_state()
            st = json.loads(raw)
            # Фильтруем только важные поля
            important_fields = ["any_live", "kick_live", "vk_live", "started_at", "updates_offset", 
                               "last_command_seen_ts", "last_updates_poll_ts", "end_streak"]
            filtered_st = {k: v for k, v in st.items() if k in important_fields}
            # Добавляем отсутствующие поля
            for k, v in default_state().items():
                if k not in filtered_st:
                    filtered_st[k] = v
            st = filtered_st
        else:
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
    
    st.setdefault("last_disk_check_ts", 0)
    st.setdefault("last_temp_cleanup_ts", 0)
    st.setdefault("last_quota_notify_ts", 0)
    st.setdefault("end_sent_for_started_at", None)
    st.setdefault("end_sent_ts", 0)
    st.setdefault("stream_stats", None)
    return st


def save_state(state: dict) -> None:
    d = os.path.dirname(STATE_FILE) or "."
    os.makedirs(d, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(prefix="state_", suffix=".json", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, separators=(",", ":"))
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
        notify_admin_dedup("drop_webhook_error", f"tg_drop_pending_updates_safe failed: {e}")


def tg_get_webhook_info() -> dict:
    return tg_call("getWebhookInfo", {})


def tg_set_my_commands(commands: list, scope: dict | None = None) -> None:
    payload = {"commands": commands}
    if scope is not None:
        payload["scope"] = scope
    tg_call("setMyCommands", payload)


def setup_commands_visibility() -> None:
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

    tg_set_my_commands(public_cmds, scope={"type": "all_group_chats"})

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
        notify_admin_dedup("photo_upload_error", f"Photo upload fallback to URL. Reason: {e}")
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



# ========== MSK TIME + FINAL REPORT ==========
MSK_TZ = timezone(timedelta(hours=3))

def dt_from_iso(iso_s: str | None) -> datetime | None:
    if not iso_s:
        return None
    try:
        return datetime.fromisoformat(iso_s)
    except Exception:
        return None

def fmt_msk(dt: datetime | None) -> str:
    if not dt:
        return "—"
    try:
        return dt.astimezone(MSK_TZ).strftime("%d.%m.%Y %H:%M:%S")
    except Exception:
        return "—"

def fmt_msk_hm_from_ts(ts_int: int) -> str:
    try:
        dt = datetime.fromtimestamp(int(ts_int), tz=timezone.utc).astimezone(MSK_TZ)
        return dt.strftime("%H:%M")
    except Exception:
        return "--:--"

def fmt_duration_full(seconds: int) -> str:
    seconds = max(0, int(seconds or 0))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    return f"{h:02d} ч. {m:02d} мин."

def fmt_hhmm(seconds: int) -> str:
    seconds = max(0, int(seconds or 0))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    return f"{h:02d}:{m:02d}"

def norm_key(x: str | None) -> str:
    s = (x or "").strip()
    return s if s else "—"

def seg_add(segments: list, start_ts: int, end_ts: int, value: str) -> None:
    if int(end_ts) <= int(start_ts):
        return
    value = norm_key(value)
    if segments and isinstance(segments[-1], dict):
        last = segments[-1]
        if last.get("value") == value and int(last.get("end_ts") or 0) == int(start_ts):
            last["end_ts"] = int(end_ts)
            return
    segments.append({"start_ts": int(start_ts), "end_ts": int(end_ts), "value": value})

def add_dur(d: dict, key: str, delta: int, max_keys: int = 20) -> None:
    key = norm_key(key)
    if key not in d and len(d) >= max_keys:
        return
    d[key] = int(d.get(key, 0)) + int(delta)

def plat_init() -> dict:
    return {
        "min": None,
        "max": None,
        "sum": 0,
        "samples": 0,
        "peak_ts": 0,
        "min_ts": 0,
        "title_changes": 0,
        "cat_changes": 0,
        "ever_live": False,
    }

def stats_init(st: dict, kick: dict, vk: dict, now_ts: int) -> dict:
    if not st.get("started_at"):
        st["started_at"] = now_utc().isoformat()
    return {
        "session_started_at": st.get("started_at"),
        "start_ts": int(now_ts),
        "end_ts": None,
        "last_tick_ts": int(now_ts),
        "kick": plat_init(),
        "vk": plat_init(),
        "kick_cat_dur": {},
        "kick_title_dur": {},
        "vk_cat_dur": {},
        "vk_title_dur": {},
        "kick_cat_timeline": [],
        "kick_title_timeline": [],
        "vk_cat_timeline": [],
        "vk_title_timeline": [],
        "kick_last_live": bool(kick.get("live")),
        "vk_last_live": bool(vk.get("live")),
        "kick_last_cat": norm_key(kick.get("category")),
        "kick_last_title": norm_key(kick.get("title")),
        "vk_last_cat": norm_key(vk.get("category")),
        "vk_last_title": norm_key(vk.get("title")),
        "both_live_sec": 0,
    }

def plat_sample(p: dict, viewers, now_ts: int) -> None:
    if not isinstance(viewers, int):
        return
    v = int(viewers)
    p["sum"] = int(p.get("sum", 0)) + v
    p["samples"] = int(p.get("samples", 0)) + 1
    cur_min = p.get("min")
    cur_max = p.get("max")
    if cur_min is None or v < int(cur_min):
        p["min"] = v
        p["min_ts"] = int(now_ts)
    if cur_max is None or v > int(cur_max):
        p["max"] = v
        p["peak_ts"] = int(now_ts)

def stats_tick(st: dict, kick: dict, vk: dict, any_live: bool, now_ts: int | None = None) -> None:
    now_ts = int(now_ts or ts())
    stats = st.get("stream_stats")

    if any_live and (not isinstance(stats, dict) or stats.get("session_started_at") != st.get("started_at")):
        st["stream_stats"] = stats_init(st, kick, vk, now_ts)
        return

    if not isinstance(stats, dict):
        return

    last_tick = int(stats.get("last_tick_ts") or now_ts)
    delta = max(0, now_ts - last_tick)
    delta = min(delta, int(POLLINTERVAL) + 5)

    if delta > 0:
        if stats.get("kick_last_live"):
            seg_add(stats.setdefault("kick_cat_timeline", []), last_tick, now_ts, stats.get("kick_last_cat"))
            seg_add(stats.setdefault("kick_title_timeline", []), last_tick, now_ts, stats.get("kick_last_title"))
            add_dur(stats.setdefault("kick_cat_dur", {}), stats.get("kick_last_cat"), delta)
            add_dur(stats.setdefault("kick_title_dur", {}), stats.get("kick_last_title"), delta)
        if stats.get("vk_last_live"):
            seg_add(stats.setdefault("vk_cat_timeline", []), last_tick, now_ts, stats.get("vk_last_cat"))
            seg_add(stats.setdefault("vk_title_timeline", []), last_tick, now_ts, stats.get("vk_last_title"))
            add_dur(stats.setdefault("vk_cat_dur", {}), stats.get("vk_last_cat"), delta)
            add_dur(stats.setdefault("vk_title_dur", {}), stats.get("vk_last_title"), delta)
        if stats.get("kick_last_live") and stats.get("vk_last_live"):
            stats["both_live_sec"] = int(stats.get("both_live_sec", 0)) + delta

    if bool(kick.get("live")) and stats.get("kick_last_live"):
        if norm_key(kick.get("title")) != norm_key(stats.get("kick_last_title")):
            stats["kick"]["title_changes"] = int(stats["kick"].get("title_changes", 0)) + 1
        if norm_key(kick.get("category")) != norm_key(stats.get("kick_last_cat")):
            stats["kick"]["cat_changes"] = int(stats["kick"]["cat_changes"] or 0) + 1

    if bool(vk.get("live")) and stats.get("vk_last_live"):
        if norm_key(vk.get("title")) != norm_key(stats.get("vk_last_title")):
            stats["vk"]["title_changes"] = int(stats["vk"].get("title_changes", 0)) + 1
        if norm_key(vk.get("category")) != norm_key(stats.get("vk_last_cat")):
            stats["vk"]["cat_changes"] = int(stats["vk"]["cat_changes"] or 0) + 1

    if kick.get("live"):
        stats["kick"]["ever_live"] = True
        plat_sample(stats["kick"], kick.get("viewers"), now_ts)

    if vk.get("live"):
        stats["vk"]["ever_live"] = True
        plat_sample(stats["vk"], vk.get("viewers"), now_ts)

    stats["last_tick_ts"] = int(now_ts)
    stats["kick_last_live"] = bool(kick.get("live"))
    stats["vk_last_live"] = bool(vk.get("live"))
    stats["kick_last_cat"] = norm_key(kick.get("category"))
    stats["kick_last_title"] = norm_key(kick.get("title"))
    stats["vk_last_cat"] = norm_key(vk.get("category"))
    stats["vk_last_title"] = norm_key(vk.get("title"))

    st["stream_stats"] = stats

def stats_finalize_end(st: dict, now_ts: int | None = None) -> None:
    now_ts = int(now_ts or ts())
    stats = st.get("stream_stats")
    if not isinstance(stats, dict):
        return
    stats["end_ts"] = int(now_ts)
    st["stream_stats"] = stats

def _fmt_avg(p: dict) -> str:
    samples = int(p.get("samples", 0) or 0)
    if samples <= 0:
        return "—"
    s = int(p.get("sum", 0) or 0)
    return str(int(round(s / samples)))

def _top_durations(d: dict) -> list:
    items = [(k, int(v)) for k, v in (d or {}).items() if int(v) > 0]
    items.sort(key=lambda x: x[1], reverse=True)
    return items

def _render_timeline(segments: list, bold_value: bool, max_items: int = 10) -> list[str]:
    out = []
    for seg in (segments or [])[:max_items]:
        if not isinstance(seg, dict):
            continue
        s = int(seg.get("start_ts") or 0)
        e = int(seg.get("end_ts") or 0)
        if e <= s:
            continue
        hms = fmt_msk_hm_from_ts(s)
        hme = fmt_msk_hm_from_ts(e)
        val = esc(seg.get("value"))
        dur = fmt_hhmm(e - s)
        if bold_value:
            out.append(f"{hms}–{hme} <b>{val}</b> {dur}")
        else:
            out.append(f"{hms}–{hme} <i>{val}</i> {dur}")
    return out

def _platform_block(label: str, key: str, url: str, stats: dict) -> list[str]:
    out = [f"<b>{label}</b>"]
    p = (stats or {}).get(key) if isinstance(stats, dict) else None
    if not isinstance(p, dict) or not bool(p.get("ever_live")):
        out.append("<i>Стрима не было на этой площадке.</i>")
        out.append(f"<b>Ссылка:</b> {url}")
        return out

    out.append(f"<b>Min/Avg/Max зрителей:</b> {fmt_viewers(p.get('min'))}/{_fmt_avg(p)}/{fmt_viewers(p.get('max'))}")
    out.append(f"<b>Смен названия/категории:</b> {int(p.get('title_changes',0) or 0)}/{int(p.get('cat_changes',0) or 0)}")

    cat_dur = (stats or {}).get(f"{key}_cat_dur") or {}
    title_dur = (stats or {}).get(f"{key}_title_dur") or {}
    if cat_dur:
        out.append("")
        out.append("<b>Топ игр/категорий (по длительности):</b>")
        for k, v in _top_durations(cat_dur)[:5]:
            out.append(f"• {esc(k)} — {fmt_duration_full(v)}")
    if title_dur:
        out.append("")
        out.append("<b>Топ названий (по длительности):</b>")
        for k, v in _top_durations(title_dur)[:3]:
            out.append(f"• {esc(k)} — {fmt_duration_full(v)}")

    cat_tl = (stats or {}).get(f"{key}_cat_timeline") or []
    title_tl = (stats or {}).get(f"{key}_title_timeline") or []
    if cat_tl:
        out.append("")
        out.append("<b>Хронология категорий:</b>")
        out.extend(_render_timeline(cat_tl, bold_value=True, max_items=10))
    if title_tl:
        out.append("")
        out.append("<b>Хронология названий:</b>")
        out.extend(_render_timeline(title_tl, bold_value=False, max_items=10))

    out.append("")
    out.append(f"<b>Ссылка:</b> {url}")
    return out

def build_end_report(st: dict) -> str:
    start_dt = dt_from_iso(st.get("started_at"))
    stats = st.get("stream_stats") if isinstance(st.get("stream_stats"), dict) else {}

    end_ts = int((stats or {}).get("end_ts") or st.get("end_sent_ts") or ts())
    try:
        end_dt = datetime.fromtimestamp(int(end_ts), tz=timezone.utc)
    except Exception:
        end_dt = None

    dur = "—"
    try:
        if start_dt and end_dt:
            dur = fmt_duration_full(int((end_dt - start_dt).total_seconds()))
    except Exception:
        pass

    lines = []
    lines.append("<b>📊 Финальный отчёт</b>")
    lines.append("")
    lines.append(f"<b>Старт (МСК):</b> {fmt_msk(start_dt)}")
    lines.append(f"<b>Финиш (МСК):</b> {fmt_msk(end_dt)}")
    lines.append(f"<b>Длительность:</b> {dur}")

    both = int((stats or {}).get("both_live_sec", 0) or 0)
    if both > 0:
        lines.append(f"<b>Kick + VK одновременно:</b> {fmt_duration_full(both)}")

    lines.append("")
    lines.extend(_platform_block("Kick", "kick", KICK_PUBLIC_URL, stats))
    lines.append("")
    lines.extend(_platform_block("VK Play", "vk", VK_PUBLIC_URL, stats))

    out = "
".join(lines)
    return out[:3900]

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
    return "\n".join([
        prefix,
        "",
        f"🔗 Kick: {KICK_PUBLIC_URL}",
        f"🔗 VK Play: {VK_PUBLIC_URL}",
    ])
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


# ========== ADMIN DIAG (УЛУЧШЕНО) ==========
def _age_str(sec: int) -> str:
    sec = int(sec or 0)
    if sec <= 0:
        return "никогда"
    if sec < 60:
        return f"{sec} сек"
    if sec < 3600:
        return f"{sec//60} мин"
    h = sec // 3600
    m = (sec % 3600) // 60
    return f"{h} ч {m} мин"


def _yes_no(v: bool) -> str:
    return "ДА" if v else "НЕТ"


def build_admin_diag_text(st: dict, webhook_info: dict) -> str:
    now = ts()

    any_live = bool(st.get("any_live"))
    kick_live = bool(st.get("kick_live"))
    vk_live = bool(st.get("vk_live"))
    end_streak = int(st.get("end_streak") or 0)  # ✅ НОВОЕ

    started_at = esc(st.get("started_at"))

    last_poll = int(st.get("last_updates_poll_ts") or 0)
    last_cmd = int(st.get("last_command_seen_ts") or 0)
    last_rec = int(st.get("last_commands_recover_ts") or 0)

    poll_age = (now - last_poll) if last_poll else 0
    cmd_age = (now - last_cmd) if last_cmd else 0
    rec_age = (now - last_rec) if last_rec else 0

    on_air = (last_poll != 0 and poll_age <= 120)
    on_air_icon = "✅" if on_air else "⚠️"
    on_air_text = "Да" if on_air else "Похоже, нет (давно не опрашивал Telegram)"

    offset = int(st.get("updates_offset") or 0)

    url = ""
    pend = ""
    try:
        url = webhook_info.get("url", "")
        pend = str(webhook_info.get("pending_update_count", ""))
    except Exception:
        url = str(webhook_info)
        pend = "—"

    webhook_state = "выключен (это нормально: бот работает через polling getUpdates)" if not url else "включен"

    actions = []
    if on_air:
        actions.append("✅ Всё хорошо: бот получает обновления Telegram.")
    else:
        actions.append("⚠️ Бот давно не ‘слушал’ Telegram.")
        actions.append("1) Подожди 1–2 минуты и снова введи /admin.")
        actions.append("2) Если всё так же — перезапусти бота/контейнер.")
        actions.append("3) Если часто так бывает — смотри, не запущен ли второй экземпляр бота (может быть 409 Conflict).")

    if last_rec:
        actions.append("ℹ️ Watchdog уже срабатывал — значит бот сам пытался починиться.")

    return (
        "<b>Админ-проверка (простыми словами)</b>\n\n"
        "<b>Стрим сейчас:</b>\n"
        f"- Идёт ли стрим: {_yes_no(any_live)} (Kick: {_yes_no(kick_live)}, VK: {_yes_no(vk_live)})\n"
        f"- Время старта: {started_at}\n"
        f"- Подтверждений конца: {end_streak} (нужно {END_CONFIRM_STREAK}) ✅\n\n"
        "<b>Команды в Телеграм:</b>\n"
        f"- Бот “на связи”: {on_air_icon} {on_air_text} (последний опрос: {_age_str(poll_age)} назад)\n"
        f"- Последняя команда (/stream и т.п.): {_age_str(cmd_age)} назад\n"
        f"- Самовосстановление (watchdog): {_age_str(rec_age)} назад\n\n"
        "<b>Очередь сообщений Telegram:</b>\n"
        f"- Webhook: {webhook_state}\n"
        f"- В очереди Telegram: {esc(pend)} (сколько апдейтов ждут доставки)\n"
        f"- Указатель очереди (offset): {offset} (с какого update_id продолжаем)\n\n"
        "<b>Что делать:</b>\n"
        + "\n".join(actions)
        + "\n"
    )


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


def commands_loop_forever():
    while True:
        try:
            commands_loop_once()
        except Exception as e:
            if is_telegram_conflict_409(e):
                notify_409_dedup("⚠️ Telegram 409 Conflict (getUpdates): есть другой polling на этом токене. Проверь, не запущено ли где-то ещё.")
                time.sleep(60)
                continue
            notify_admin_dedup("commands_loop_crash", f"commands_loop crashed: {e}\n{traceback.format_exc()[:3000]}")
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

        if is_private_chat(msg) and is_admin_msg(msg):
            with STATE_LOCK:
                st = load_state()
                st["admin_private_chat_id"] = int((msg.get("chat") or {}).get("id") or 0)
                save_state(st)
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

            with STATE_LOCK:
                st = load_state()
            try:
                wh = tg_get_webhook_info()
            except Exception as e:
                wh = {"error": str(e)}
            tg_send_to(chat_id, None, build_admin_diag_text(st, wh), reply_to=reply_to)
            continue

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
            notify_admin_dedup("kick_fetch_error", f"Kick fetch (command) error: {e}")

        try:
            vk = vk_fetch_best_effort()
        except Exception as e:
            vk = {"live": False, "title": None, "category": None, "viewers": None, "thumb": None}
            notify_admin_dedup("vk_fetch_error", f"VK fetch (command) error: {e}")

        with STATE_LOCK:
            st2 = load_state()
            # ✅ ПРАВИЛЬНЫЙ ПОРЯДОК ДЛЯ КОМАНД
            st2["any_live"] = bool(kick.get("live") or vk.get("live"))
            st2["kick_live"] = bool(kick.get("live"))
            st2["vk_live"] = bool(vk.get("live"))
            if st2["any_live"]:
                set_started_at_from_kick(st2, kick)
                st2["end_streak"] = 0  # ✅ СБРОС при любом live
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
                notify_admin_dedup("watchdog_triggered", "⚠️ Watchdog: getUpdates давно не отрабатывал, делаю восстановление...")
                tg_drop_pending_updates_safe()
                with STATE_LOCK:
                    st = load_state()
                    st["updates_offset"] = 0
                    st["last_commands_recover_ts"] = now
                    save_state(st)

                if COMMANDS_WATCHDOG_PING_ENABLED:
                    try:
                        notify_admin_dedup("watchdog_recovered", "✅ Watchdog: восстановил polling команд.")
                    except Exception:
                        pass

        except Exception as e:
            notify_admin_dedup("watchdog_crash", f"commands_watchdog crashed: {e}\n{traceback.format_exc()[:3000]}")

        time.sleep(10)


# ========== MAIN LOOP (✅ ИСПРАВЛЕНО) ==========
def main_loop_forever():
    while True:
        try:
            main_loop()
        except Exception as e:
            notify_admin_dedup("main_loop_crash", f"main_loop crashed: {e}\n{traceback.format_exc()[:3000]}")
            time.sleep(LOOP_CRASH_SLEEP)


def main_loop():
    # Инициализация
    try:
        kick0 = kick_fetch()
    except Exception as e:
        kick0 = {"live": False, "title": None, "category": None, "viewers": None, "thumb": None, "created_at": None, "playback_url": None}
        notify_admin_dedup("kick_init_error", f"Kick init fetch error: {e}")

    try:
        vk0 = vk_fetch_best_effort()
    except Exception as e:
        vk0 = {"live": False, "title": None, "category": None, "viewers": None, "thumb": None}
        notify_admin_dedup("vk_init_error", f"VK init fetch error: {e}")

    any_live0 = bool(kick0.get("live") or vk0.get("live"))

    # ✅ ПРОВЕРКА END ПРИ СТАРТЕ (если стрим кончился пока бот был вниз)
    with STATE_LOCK:
        prev_st = load_state()
        prev_any_before_init = bool(prev_st.get("any_live"))
        prev_end_streak = int(prev_st.get("end_streak") or 0)

    if prev_any_before_init and (not any_live0) and prev_end_streak >= END_CONFIRM_STREAK:
        try:
            with STATE_LOCK:
                st_end = load_state()
            stats_tick(st_end, kick, vk, any_live=False, now_ts=ts())
                stats_finalize_end(st_end, now_ts=ts())
                st_end["end_sent_for_started_at"] = st_end.get("started_at")
                st_end["end_sent_ts"] = ts()
                save_state(st_end)
                tg_send(build_end_report(st_end))
            notify_admin_dedup("end_notification_sent", f"✅ End notification sent at boot (streak={prev_end_streak})")
        except Exception as e:
            notify_admin_dedup("end_restart_error", f"End-after-restart send error: {e}")

    # ✅ ИНИЦИАЛИЗАЦИЯ СОСТОЯНИЯ (ПРАВИЛЬНЫЙ ПОРЯДОК)
    with STATE_LOCK:
        st = load_state()
        st["any_live"] = any_live0
        st["kick_live"] = bool(kick0.get("live"))
        st["vk_live"] = bool(vk0.get("live"))
        if any_live0:
            set_started_at_from_kick(st, kick0)
            st["end_streak"] = 0  # ✅ СБРОС при любом live
        st["kick_title"] = kick0.get("title")
        st["kick_cat"] = kick0.get("category")
        st["vk_title"] = vk0.get("title")
        st["vk_cat"] = vk0.get("category")
        st["kick_viewers"] = kick0.get("viewers")
        st["vk_viewers"] = vk0.get("viewers")
        save_state(st)

    # Startup ping
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
            notify_admin_dedup("startup_ping_error", f"Startup ping failed: {e}")

    # No stream on start
    if NO_STREAM_ON_START_MESSAGE and (not any_live0):
        with STATE_LOCK:
            st = load_state()
            last_ts = int(st.get("last_no_stream_start_ts") or 0)

        if ts() - last_ts >= NO_STREAM_START_DEDUP_SEC:
            try:
                tg_send(build_no_stream_text("Сейчас на канале Глад Валакас патока нет!"))
            except Exception as e:
                notify_admin_dedup("no_stream_error", f"No-stream-on-start send error: {e}")

            with STATE_LOCK:
                st = load_state()
                st["last_no_stream_start_ts"] = ts()
                save_state(st)

    # Boot status
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
            notify_admin_dedup("boot_status_error", f"Boot status send error: {e}")

    # Счетчик для очистки
    cleanup_counter = 0
    last_disk_check = 0
    
    # ✅ ОСНОВНОЙ ЦИКЛ (ИСПРАВЛЕН)
    while True:
        try:
            kick = kick_fetch()
        except Exception as e:
            kick = {"live": False, "title": None, "category": None, "viewers": None, "thumb": None, "created_at": None, "playback_url": None}
            notify_admin_dedup("kick_fetch_main_error", f"Kick fetch error: {e}")

        try:
            vk = vk_fetch_best_effort()
        except Exception as e:
            vk = {"live": False, "title": None, "category": None, "viewers": None, "thumb": None}
            notify_admin_dedup("vk_fetch_main_error", f"VK fetch error: {e}")

        # ✅ 1. ЧИТАЕМ ПРЕДЫДУЩЕЕ СОСТОЯНИЕ
        with STATE_LOCK:
            st = load_state()
            prev_any = bool(st.get("any_live"))
            prev_end_streak = int(st.get("end_streak") or 0)

        # ✅ 2. НОВОЕ СОСТОЯНИЕ
        any_live = bool(kick.get("live") or vk.get("live"))

        # ✅ 3. START NOTIFICATION
        if (not prev_any) and any_live:
            if ts() - int(st.get("last_start_sent_ts") or 0) >= START_DEDUP_SEC:
                with STATE_LOCK:
                    st_start = load_state()
                    if not st_start.get("started_at"):
                        set_started_at_from_kick(st_start, kick)
                    save_state(st_start)

                try:
                    with STATE_LOCK:
                        st = load_state()
                    send_status_with_screen("🚨🚨 🧩 Глад Валакас запустил паток! 🚨🚨", st, kick, vk)
                    with STATE_LOCK:
                        st = load_state()
                        st["last_start_sent_ts"] = ts()
                        save_state(st)
                except Exception as e:
                    notify_admin_dedup("start_notify_error", f"Start send error: {e}")

        # ✅ 4. CHANGE NOTIFICATION
        changed = False
        with STATE_LOCK:
            st = load_state()
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
                    notify_admin_dedup("change_notify_error", f"Change send error: {e}")

        # ✅ 5. END NOTIFICATION (ИСПРАВЛЕНО!)
        if prev_any and (not any_live) and prev_end_streak + 1 >= END_CONFIRM_STREAK:
            try:
                with STATE_LOCK:
                    st_end = load_state()
                    # Запоминаем зрителей для end-сообщения
                    st_end["kick_viewers"] = st_end.get("kick_viewers") or kick.get("viewers")
                    st_end["vk_viewers"] = st_end.get("vk_viewers") or vk.get("viewers")
                    save_state(st_end)
                tg_send(build_end_text(st_end))
                notify_admin_dedup("end_notify_success", f"✅ End notification sent (streak={prev_end_streak + 1})")
            except Exception as e:
                notify_admin_dedup("end_notify_error", f"End send error: {e}")

        # ✅ 6. СОХРАНЯЕМ НОВОЕ СОСТОЯНИЕ (ПРАВИЛЬНЫЙ ПОРЯДОК)
        with STATE_LOCK:
            st = load_state()
            st["any_live"] = any_live
            st["kick_live"] = bool(kick.get("live"))
            st["vk_live"] = bool(vk.get("live"))
            if any_live:
                set_started_at_from_kick(st, kick)
                st["end_streak"] = 0  # ✅ ГЛАВНОЕ ИСПРАВЛЕНИЕ: сбрасываем при ЛЮБОМ live
            else:
                st["end_streak"] = prev_end_streak + 1  # ✅ Инкремент только при !any_live
            st["started_at"] = st.get("started_at")  # Сохраняем время старта
            st["kick_title"] = kick.get("title")
            st["kick_cat"] = kick.get("category")
            st["vk_title"] = vk.get("title")
            st["vk_cat"] = vk.get("category")
            st["kick_viewers"] = kick.get("viewers")
            st["vk_viewers"] = vk.get("viewers")
            save_state(st)

        # ✅ 7. ПЕРИОДИЧЕСКАЯ ОЧИСТКА ДИСКА
        cleanup_counter += 1
        current_time = ts()
        
        if cleanup_counter >= DISK_CHECK_INTERVAL:
            # ✅ Periodic cleanup + quota monitor (Bothost)
            cleanup_temp_files()
            cleanup_old_state_backups()

            q_percent, q_used, q_total = quota_usage_for_bot()

            with STATE_LOCK:
                stq = load_state()
                last_nt = int(stq.get("last_quota_notify_ts") or 0)

            cooldown_ok = (ts() - last_nt) >= BOT_NOTIFY_COOLDOWN_SEC

            if q_percent >= BOT_WARN_PERCENT and cooldown_ok:
                top = list_largest_files(os.getcwd(), BOT_TOP_FILES)
                if top:
                    top_lines = "\n".join([f"- {fmt_bytes(sz)} — {path}" for sz, path in top])
                    top_text = "\n\nТоп файлов по размеру:\n" + top_lines
                else:
                    top_text = ""

                notify_admin_dedup(
                    "quota_high",
                    "⚠️ Квота диска почти заполнена (по размеру папки бота).\n"
                    f"Занято ботом: {fmt_bytes(q_used)} из {fmt_bytes(q_total)} ({q_percent:.1f}%)."
                    + top_text
                    + "\n\nОчищаю temp/__pycache__…"
                )

                cleanup_pycache()
                cleanup_temp_files()
                cleanup_old_state_backups()

                with STATE_LOCK:
                    stq = load_state()
                    stq["last_quota_notify_ts"] = ts()
                    save_state(stq)

            cleanup_counter = 0

        time.sleep(POLL_INTERVAL)


def main():
    # Очистка старых файлов при старте
    cleanup_temp_files()
    cleanup_old_state_backups()
    
    tg_drop_pending_updates_safe()

    try:
        setup_commands_visibility()
    except Exception as e:
        notify_admin_dedup("setup_commands_error", f"Setup commands visibility failed: {e}")

    if COMMANDS_ENABLED:
        threading.Thread(target=commands_loop_forever, daemon=True).start()
        threading.Thread(target=commands_watchdog_forever, daemon=True).start()

    main_loop_forever()


if __name__ == "__main__":
    main()
