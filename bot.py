import os
import re
import json
import time
import random
import subprocess
import threading
import traceback
import shutil
import glob
from datetime import datetime, timezone, timedelta
from html import escape as html_escape

import requests

# ========== CONFIG (ENV) ==========
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()


def _mask_secrets(text: str) -> str:
    """Hide bot token and similar secrets in logs/messages."""
    try:
        s = str(text)
    except Exception:
        return '<unprintable>'

    # Replace exact token if present
    try:
        if BOT_TOKEN:
            s = s.replace(BOT_TOKEN, '***')
    except Exception:
        pass

    # Replace '/bot<TOKEN>/' fragments that appear in requests exceptions
    try:
        s = re.sub(r'/bot[^/]+/', '/bot***/', s)
    except Exception:
        pass

    # Replace 'bot<TOKEN>' fragments
    try:
        s = re.sub(r'bot\d+:[A-Za-z0-9_\-]+', 'bot***', s)
    except Exception:
        pass

    return s

GROUP_ID = int(os.getenv("GROUP_ID", "-1002977868330"))
TOPIC_ID = int(os.getenv("TOPIC_ID", "65114"))

# Special cross-post: if Kick category matches, duplicate notifications to another topic
PUBG_DUPLICATE_CHAT_ID = int(os.getenv("PUBG_DUPLICATE_CHAT_ID", "-1002977868330"))
PUBG_DUPLICATE_TOPIC_ID = int(os.getenv("PUBG_DUPLICATE_TOPIC_ID", "2"))
PUBG_CATEGORY_MATCH = os.getenv("PUBG_CATEGORY_MATCH", "PUBG: Battlegrounds").strip()

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
COMMAND_POLL_TIMEOUT = int(os.getenv("COMMAND_POLL_TIMEOUT", "5"))
# IMPORTANT: HTTP timeout must be > long-poll timeout, otherwise you'll see ReadTimeout on getUpdates.
COMMAND_HTTP_TIMEOUT = int(os.getenv("COMMAND_HTTP_TIMEOUT", "20"))
COMMAND_STATE_SAVE_SEC = int(os.getenv("COMMAND_STATE_SAVE_SEC", "60"))
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

# HTTP retry strategy (external services: Kick/VK + images)
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "4"))
HTTP_BACKOFF_BASE = float(os.getenv("HTTP_BACKOFF_BASE", "1.6"))
HTTP_BACKOFF_MAX = float(os.getenv("HTTP_BACKOFF_MAX", "15"))
HTTP_JITTER = os.getenv("HTTP_JITTER", "1").strip() not in {"0", "false", "False"}

# Telegram retry strategy (keep smaller to avoid command loop stalls)
TG_RETRIES = int(os.getenv("TG_RETRIES", "2"))
TG_BACKOFF_BASE = float(os.getenv("TG_BACKOFF_BASE", "1.3"))
TG_BACKOFF_MAX = float(os.getenv("TG_BACKOFF_MAX", "4"))

LOOP_CRASH_SLEEP = int(os.getenv("LOOP_CRASH_SLEEP", "2"))

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
NOTIFY_409_EVERY_SEC = 6 * 60 * 60

# Disk cleanup
DISK_CHECK_INTERVAL = int(os.getenv("DISK_CHECK_INTERVAL", "100"))
MAX_STATE_SIZE = 1024 * 50
TEMP_CLEANUP_AGE_SEC = 3600
ERROR_DEDUP_SEC = 300

# Bothost quota monitor (project folder size)
BOT_QUOTA_MB = int(os.getenv("BOT_QUOTA_MB", "500"))
BOT_WARN_PERCENT = float(os.getenv("BOT_WARN_PERCENT", "90"))
BOT_NOTIFY_COOLDOWN_SEC = int(os.getenv("BOT_NOTIFY_COOLDOWN_SEC", str(6 * 60 * 60)))
BOT_TOP_FILES = int(os.getenv("BOT_TOP_FILES", "5"))

# ========== URLS ==========
KICK_API_URL = f"https://kick.com/api/v1/channels/{KICK_SLUG}"
KICK_PUBLIC_URL = f"https://kick.com/{KICK_SLUG}"
VK_PUBLIC_URL = f"https://live.vkvideo.ru/{VK_SLUG}"

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
HEADERS_JSON = {"User-Agent": UA, "Accept": "application/json,text/plain,*/*"}
HEADERS_HTML = {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}

STATE_LOCK = threading.Lock()

EXT_SESSION = requests.Session()  # Kick/VK/images
TG_SESSION = requests.Session()   # Telegram



# ========== FAST COMMANDS + FRESH SCREENSHOT (RAM cache) ==========
# Commands reuse the last snapshot collected by main loop to avoid slow Kick/VK/HTML fetch on demand.
CACHE_MAX_AGE_SEC = int(os.getenv("CACHE_MAX_AGE_SEC", "30"))
CACHED_AT_TS = 0
CACHED_KICK = None
CACHED_VK = None
CACHED_STATE = None

# Screenshot cache (bytes) stored in RAM.
SHOT_CACHE_MAX_AGE_SEC = int(os.getenv("SHOT_CACHE_MAX_AGE_SEC", "60"))
SHOT_REFRESH_SEC = int(os.getenv("SHOT_REFRESH_SEC", "20"))
CACHED_SHOT_AT_TS = 0
CACHED_SHOT_BYTES = None

# Shorter timeouts for command replies (so /stream won't hang 1-2 minutes).
TG_CMD_SEND_TIMEOUT_SEC = int(os.getenv("TG_CMD_SEND_TIMEOUT_SEC", "12"))
TG_CMD_PHOTO_URL_TIMEOUT_SEC = int(os.getenv("TG_CMD_PHOTO_URL_TIMEOUT_SEC", "15"))
TG_CMD_PHOTO_UPLOAD_TIMEOUT_SEC = int(os.getenv("TG_CMD_PHOTO_UPLOAD_TIMEOUT_SEC", "18"))
FFMPEG_CMD_TIMEOUT_SEC = int(os.getenv("FFMPEG_CMD_TIMEOUT_SEC", "8"))

# Local log file (works even if platform doesn't show stdout)
LOG_FILE = os.getenv("LOG_FILE", "bot_runtime.log")

# Error deduplication cache
last_error_notify = {}


def log_line(msg: str) -> None:
    msg = _mask_secrets(msg)
    ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts_str}] {msg}"
    try:
        print(line, flush=True)
    except Exception:
        pass
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def ts() -> int:
    return int(time.time())


def _cache_set_snapshot(st: dict, kick: dict, vk: dict) -> None:
    global CACHED_AT_TS, CACHED_KICK, CACHED_VK, CACHED_STATE
    CACHED_AT_TS = ts()
    CACHED_KICK = dict(kick or {})
    CACHED_VK = dict(vk or {})
    CACHED_STATE = dict(st or {})


def _cache_get_snapshot():
    age = ts() - int(CACHED_AT_TS or 0)
    if CACHED_STATE is None or CACHED_KICK is None or CACHED_VK is None:
        return None
    if age > int(CACHE_MAX_AGE_SEC):
        return None
    return dict(CACHED_STATE), dict(CACHED_KICK), dict(CACHED_VK), age


def _shot_cache_set(img: bytes) -> None:
    global CACHED_SHOT_AT_TS, CACHED_SHOT_BYTES
    CACHED_SHOT_AT_TS = ts()
    CACHED_SHOT_BYTES = img


def _shot_cache_get():
    if not CACHED_SHOT_BYTES:
        return None
    age = ts() - int(CACHED_SHOT_AT_TS or 0)
    if age > int(SHOT_CACHE_MAX_AGE_SEC):
        return None
    return CACHED_SHOT_BYTES, age


# ========== MSK TIME + STREAM STATS ==========
MSK_TZ = timezone(timedelta(hours=3))  # Moscow time (UTC+3)

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

def now_msk_str() -> str:
    return fmt_msk(now_utc())

# Keep stats small to avoid bloating state.json
STATS_MAX_KEYS = 20     # max unique titles/categories stored per platform
STATS_MAX_PRINT = 10    # max items printed per section in final report

def _norm_key(x: str | None) -> str:
    s = (x or "—")
    s = str(s).strip()
    return s if s else "—"

def _add_dur(d: dict, key: str, delta: int) -> None:
    key = _norm_key(key)
    if key not in d and len(d) >= STATS_MAX_KEYS:
        key = "Другое"
    d[key] = int(d.get(key, 0)) + int(delta)

def _plat_init() -> dict:
    return {
        "min": None,
        "max": None,
        "sum": 0,
        "samples": 0,
        "peak_ts": 0,
        "min_ts": 0,
        "title_changes": 0,
        "cat_changes": 0,
    }

def _stats_init(st: dict, kick: dict, vk: dict, now_ts: int) -> dict:
    if not st.get("started_at"):
        st["started_at"] = now_utc().isoformat()
    return {
        "session_started_at": st.get("started_at"),
        "start_ts": int(now_ts),
        "end_ts": None,
        "last_tick_ts": int(now_ts),
        "kick": _plat_init(),
        "vk": _plat_init(),
        "kick_cat_dur": {},
        "kick_title_dur": {},
        "vk_cat_dur": {},
        "vk_title_dur": {},
        "kick_last_live": bool(kick.get("live")),
        "vk_last_live": bool(vk.get("live")),
        "kick_last_cat": _norm_key(kick.get("category")),
        "kick_last_title": _norm_key(kick.get("title")),
        "vk_last_cat": _norm_key(vk.get("category")),
        "vk_last_title": _norm_key(vk.get("title")),
        "both_live_sec": 0,
    }

def _plat_sample(p: dict, viewers, now_ts: int) -> None:
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
        st["stream_stats"] = _stats_init(st, kick, vk, now_ts)
        return

    if not isinstance(stats, dict):
        return

    last_tick = int(stats.get("last_tick_ts") or now_ts)
    delta = now_ts - last_tick
    if delta < 0:
        delta = 0
    delta = min(delta, int(POLL_INTERVAL) * 5)

    if delta > 0:


        if stats.get("kick_last_live"):

            _seg_add(stats.setdefault("kick_cat_timeline", []), last_tick, now_ts, stats.get("kick_last_cat", "—"))

            _seg_add(stats.setdefault("kick_title_timeline", []), last_tick, now_ts, stats.get("kick_last_title", "—"))

            _add_dur(stats.setdefault("kick_cat_dur", {}), stats.get("kick_last_cat", "—"), delta)

            _add_dur(stats.setdefault("kick_title_dur", {}), stats.get("kick_last_title", "—"), delta)


        if stats.get("vk_last_live"):

            _seg_add(stats.setdefault("vk_cat_timeline", []), last_tick, now_ts, stats.get("vk_last_cat", "—"))

            _seg_add(stats.setdefault("vk_title_timeline", []), last_tick, now_ts, stats.get("vk_last_title", "—"))

            _add_dur(stats.setdefault("vk_cat_dur", {}), stats.get("vk_last_cat", "—"), delta)

            _add_dur(stats.setdefault("vk_title_dur", {}), stats.get("vk_last_title", "—"), delta)


        if stats.get("kick_last_live") and stats.get("vk_last_live"):

            stats["both_live_sec"] = int(stats.get("both_live_sec", 0)) + delta


    if bool(kick.get("live")) and stats.get("kick_last_live"):
        if _norm_key(kick.get("title")) != _norm_key(stats.get("kick_last_title")):
            stats["kick"]["title_changes"] = int(stats["kick"].get("title_changes", 0)) + 1
        if _norm_key(kick.get("category")) != _norm_key(stats.get("kick_last_cat")):
            stats["kick"]["cat_changes"] = int(stats["kick"].get("cat_changes", 0)) + 1

    if bool(vk.get("live")) and stats.get("vk_last_live"):
        if _norm_key(vk.get("title")) != _norm_key(stats.get("vk_last_title")):
            stats["vk"]["title_changes"] = int(stats["vk"].get("title_changes", 0)) + 1
        if _norm_key(vk.get("category")) != _norm_key(stats.get("vk_last_cat")):
            stats["vk"]["cat_changes"] = int(stats["vk"].get("cat_changes", 0)) + 1

    if kick.get("live"):
        stats["kick_ever_live"] = True
        _plat_sample(stats["kick"], kick.get("viewers"), now_ts)
    if vk.get("live"):
        stats["vk_ever_live"] = True
        _plat_sample(stats["vk"], vk.get("viewers"), now_ts)

    stats["last_tick_ts"] = int(now_ts)
    stats["kick_last_live"] = bool(kick.get("live"))
    stats["vk_last_live"] = bool(vk.get("live"))
    stats["kick_last_cat"] = _norm_key(kick.get("category"))
    stats["kick_last_title"] = _norm_key(kick.get("title"))
    stats["vk_last_cat"] = _norm_key(vk.get("category"))
    stats["vk_last_title"] = _norm_key(vk.get("title"))

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

def _top_durations(d: dict) -> list[tuple[str, int]]:
    items = [(k, int(v)) for k, v in (d or {}).items() if int(v) > 0]
    items.sort(key=lambda x: x[1], reverse=True)
    return items

def build_end_report(st: dict) -> str:
    start_dt = dt_from_iso(st.get("started_at"))
    stats = st.get("stream_stats") if isinstance(st.get("stream_stats"), dict) else {}

    end_ts = stats.get("end_ts") or st.get("end_sent_ts") or ts()
    try:
        end_dt = datetime.fromtimestamp(int(end_ts), tz=timezone.utc)
    except Exception:
        end_dt = None

    dur = "—"
    try:
        if start_dt and end_dt:
            dur_sec = int((end_dt - start_dt).total_seconds())
            dur = fmt_duration(dur_sec)
    except Exception:
        pass

    lines: list[str] = []
    lines.append("🏁 <b>Поток окончен</b> — Глад Валакас")
    lines.append("")
    lines.append(f"🕒 <b>Начало (МСК):</b> {fmt_msk(start_dt)}")
    lines.append(f"🕒 <b>Конец (МСК):</b> {fmt_msk(end_dt)}")
    lines.append(f"⏱ <b>Длительность:</b> {dur}")

    both_live_sec = int(stats.get("both_live_sec", 0) or 0)
    if both_live_sec > 0:
        lines.append(f"⏱ <b>Одновременно на Kick + VK Play:</b> {fmt_duration(both_live_sec)}")

    lines.append("")

    def _render_timeline(segments: list, value_style: str) -> list[str]:
        out: list[str] = []
        for seg in segments or []:
            if not isinstance(seg, dict):
                continue
            s = int(seg.get("start_ts") or 0)
            e = int(seg.get("end_ts") or 0)
            if e <= s:
                continue
            hm_s = fmt_msk_hm_from_ts(s)
            hm_e = fmt_msk_hm_from_ts(e)
            val = esc(seg.get("value") or "—")
            dur_hm = fmt_hhmm(e - s)
            if value_style == 'b':
                out.append(f"{hm_s}–{hm_e} — <b>{val}</b> ({dur_hm})")
            else:
                out.append(f"{hm_s}–{hm_e} — <i>{val}</i> ({dur_hm})")
        return out

    def plat_block(label: str, key: str, url: str) -> list[str]:
        out: list[str] = []
        out.append(label)

        ever_live = bool((stats or {}).get(f"{key}_ever_live", False))
        if not ever_live:
            out.append("⚪ <i>Патока на этой площадке не было.</i>")
            out.append(f"🔗 <b>Ссылка:</b> {url}")
            return out

        pstats = (stats.get(key) or {}) if isinstance(stats.get(key), dict) else {}
        out.append(f"👥 Зрители (min/avg/max): <b>{fmt_viewers(pstats.get('min'))} / {_fmt_avg(pstats)} / {fmt_viewers(pstats.get('max'))}</b>")
        out.append(f"🔁 Смен названия: <b>{int(pstats.get('title_changes',0) or 0)}</b> • Смен категории: <b>{int(pstats.get('cat_changes',0) or 0)}</b>")

        cat_tl = stats.get(f"{key}_cat_timeline") or []
        title_tl = stats.get(f"{key}_title_timeline") or []

        out.append("")
        out.append("🧭 <b>Категории (хронология)</b>")
        cats = _render_timeline(cat_tl, 'b')
        if cats:
            out += cats[:STATS_MAX_PRINT]
            if len(cats) > STATS_MAX_PRINT:
                out.append(f"… ещё {len(cats)-STATS_MAX_PRINT}")
        else:
            out.append("—")

        out.append("")
        out.append("🧭 <b>Названия (хронология)</b>")
        titles = _render_timeline(title_tl, 'i')
        if titles:
            out += titles[:STATS_MAX_PRINT]
            if len(titles) > STATS_MAX_PRINT:
                out.append(f"… ещё {len(titles)-STATS_MAX_PRINT}")
        else:
            out.append("—")

        out.append("")
        out.append(f"🔗 <b>Ссылка:</b> {url}")
        return out

    lines += plat_block("🎥 <b>Kick</b>", "kick", KICK_PUBLIC_URL)
    lines.append("")
    lines += plat_block("🎮 <b>VK Play</b>", "vk", VK_PUBLIC_URL)

    out = "\n".join(lines)
    return out[:3900] + ("…" if len(out) > 3900 else "")

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


def fmt_hhmm(seconds: int) -> str:
    seconds = max(0, int(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    return f"{h:02d}:{m:02d}"


def fmt_msk_hm_from_ts(ts_int: int) -> str:
    try:
        dt = datetime.fromtimestamp(int(ts_int), tz=timezone.utc).astimezone(MSK_TZ)
        return dt.strftime("%H:%M")
    except Exception:
        return "--:--"


def _seg_add(segments: list, start_ts: int, end_ts: int, value: str) -> None:
    # Append [start_ts, end_ts) segment, merging with previous if same value.
    if end_ts <= start_ts:
        return
    value = _norm_key(value)
    if segments and isinstance(segments[-1], dict):
        last = segments[-1]
        if last.get("value") == value and int(last.get("end_ts") or 0) == int(start_ts):
            last["end_ts"] = int(end_ts)
            return
    segments.append({"start_ts": int(start_ts), "end_ts": int(end_ts), "value": value})


def parse_kick_created_at(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

def reset_stream_session(st: dict) -> None:
    # Reset per-stream state so a new stream can't inherit timestamps/stats from a previous one.
    st["stream_stats"] = None
    st["end_streak"] = 0
    st["end_sent_for_started_at"] = None
    st["end_sent_ts"] = 0


def sync_kick_session(st: dict, kick: dict, force: bool = False) -> bool:
    """Return True if started_at was set/changed (new session detected)."""
    if not kick.get("live"):
        return False

    kdt = parse_kick_created_at(kick.get("created_at"))
    cur = dt_from_iso(st.get("started_at"))

    # If Kick provides created_at, it uniquely identifies the current stream session.
    if kdt is not None:
        if cur is None:
            st["started_at"] = kdt.isoformat()
            return True

        # If created_at changed notably, it's a new stream session.
        try:
            if abs(int((cur - kdt).total_seconds())) > 60:
                reset_stream_session(st)
                st["started_at"] = kdt.isoformat()
                return True
        except Exception:
            reset_stream_session(st)
            st["started_at"] = kdt.isoformat()
            return True

        # Same session.
        if force:
            st["started_at"] = kdt.isoformat()
        return False

    # Fallback: no created_at. If forcing, use current time.
    if force:
        reset_stream_session(st)
        st["started_at"] = now_utc().isoformat()
        return True

    return False



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
        return "Идёт: —"
    return f"Идёт: {fmt_duration(sec)}"


def _sleep_backoff(attempt: int, base: float, cap: float, jitter: bool) -> None:
    delay = min((base ** attempt), cap)
    if jitter:
        delay *= random.uniform(0.85, 1.35)
    time.sleep(delay)


def http_request_ext(method: str, url: str, *, headers=None, json_body=None, data=None, files=None, timeout=25, allow_redirects=True) -> requests.Response:
    last_exc = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            r = EXT_SESSION.request(
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
                _sleep_backoff(attempt, HTTP_BACKOFF_BASE, HTTP_BACKOFF_MAX, HTTP_JITTER)
                continue
            r.raise_for_status()
            return r
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError) as e:
            last_exc = e
            if attempt == HTTP_RETRIES:
                raise
            _sleep_backoff(attempt, HTTP_BACKOFF_BASE, HTTP_BACKOFF_MAX, HTTP_JITTER)
        except requests.exceptions.HTTPError as e:
            last_exc = e
            if attempt == HTTP_RETRIES:
                raise
            _sleep_backoff(attempt, HTTP_BACKOFF_BASE, HTTP_BACKOFF_MAX, HTTP_JITTER)
    raise last_exc


def http_request_tg(method: str, url: str, *, json_body=None, data=None, files=None, timeout=(5, 15)) -> requests.Response:
    """Telegram requests with smaller retry budget to avoid long stalls in command loop."""
    last_exc = None
    for attempt in range(1, TG_RETRIES + 1):
        try:
            r = TG_SESSION.request(method, url, json=json_body, data=data, files=files, timeout=timeout)
            # Telegram can rate limit; retry a bit
            if r.status_code in (429, 500, 502, 503, 504):
                if attempt == TG_RETRIES:
                    r.raise_for_status()
                _sleep_backoff(attempt, TG_BACKOFF_BASE, TG_BACKOFF_MAX, True)
                continue
            r.raise_for_status()
            return r
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError) as e:
            last_exc = e
            if attempt == TG_RETRIES:
                raise
            _sleep_backoff(attempt, TG_BACKOFF_BASE, TG_BACKOFF_MAX, True)
        except requests.exceptions.HTTPError as e:
            last_exc = e
            if attempt == TG_RETRIES:
                raise
            _sleep_backoff(attempt, TG_BACKOFF_BASE, TG_BACKOFF_MAX, True)
    raise last_exc


def is_telegram_conflict_409(exc: Exception) -> bool:
    return (
        isinstance(exc, requests.exceptions.HTTPError)
        and getattr(exc, "response", None) is not None
        and int(getattr(exc.response, "status_code", 0) or 0) == 409
    )


# ========== DISK CLEANUP FUNCTIONS ==========

def cleanup_temp_files() -> None:
    try:
        temp_dirs = ["/tmp", "/var/tmp", "/dev/shm"]
        for temp_dir in temp_dirs:
            if os.path.exists(temp_dir):
                for pattern in ["ffmpeg-*", "tmp*", "*.mp4", "*.ts", "*.m3u8", "*.jpg", "*.jpeg", "*.png"]:
                    for fp in glob.glob(os.path.join(temp_dir, pattern)):
                        try:
                            if os.path.isfile(fp):
                                file_age = time.time() - os.path.getmtime(fp)
                                if file_age > TEMP_CLEANUP_AGE_SEC:
                                    os.remove(fp)
                        except Exception:
                            pass
    except Exception:
        pass


def cleanup_pycache() -> None:
    try:
        base = os.getcwd()
        for root, dirs, files in os.walk(base):
            if root.startswith("/proc") or root.startswith("/sys") or root.startswith("/dev"):
                continue
            if "__pycache__" in dirs:
                try:
                    shutil.rmtree(os.path.join(root, "__pycache__"), ignore_errors=True)
                except Exception:
                    pass
            for fn in files:
                if fn.endswith(".pyc") or fn.endswith(".pyo"):
                    try:
                        os.remove(os.path.join(root, fn))
                    except Exception:
                        pass
    except Exception:
        pass


def cleanup_old_state_backups() -> None:
    try:
        dir_name = os.path.dirname(STATE_FILE) or "."
        for filename in os.listdir(dir_name):
            if filename.startswith("state_") and filename.endswith(".json"):
                fp = os.path.join(dir_name, filename)
                try:
                    if os.path.isfile(fp):
                        file_age = time.time() - os.path.getmtime(fp)
                        if file_age > TEMP_CLEANUP_AGE_SEC:
                            os.remove(fp)
                except Exception:
                    pass
    except Exception:
        pass


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


def notify_admin_dedup(key: str, text: str) -> None:
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
        # end confirmation
        "end_streak": 0,
        # end notification anti-loss
        "end_sent_for_started_at": None,
        "end_sent_ts": 0,
        # anti-spam for 409
        "last_409_notify_ts": 0,
        # remember your private chat id once seen
        "admin_private_chat_id": 0,
        # disk cleanup tracking
        "last_disk_check_ts": 0,
        "last_temp_cleanup_ts": 0,
        # quota alert anti-spam
        "last_quota_notify_ts": 0,

        # per-stream aggregated stats (lightweight)
        "stream_stats": None,
    }


def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return default_state()
    try:
        if os.path.getsize(STATE_FILE) > MAX_STATE_SIZE:
            notify_admin_dedup("state_file_large", f"⚠️ state.json слишком большой: {os.path.getsize(STATE_FILE)} bytes")
            # keep only important fields
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                raw = f.read()
            if not raw.strip():
                return default_state()
            st = json.loads(raw)
            important = {
                "any_live",
                "kick_live",
                "vk_live",
                "started_at",
                "updates_offset",
                "last_command_seen_ts",
                "last_updates_poll_ts",
                "end_streak",
                "end_sent_for_started_at",
        "stream_stats",
            }
            st = {k: v for k, v in (st or {}).items() if k in important}
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

    base = default_state()
    base.update(st)
    return base


def save_state(state: dict) -> None:
    d = os.path.dirname(STATE_FILE) or "."
    os.makedirs(d, exist_ok=True)
    tmp_path = os.path.join(d, ".state_tmp.json")

    def _write_once() -> None:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, separators=(",", ":"))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, STATE_FILE)

    try:
        _write_once()
    except OSError as e:
        if getattr(e, "errno", None) == 28:
            try:
                cleanup_pycache()
                cleanup_temp_files()
                cleanup_old_state_backups()
            except Exception:
                pass
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
            try:
                _write_once()
                return
            except OSError as e2:
                if getattr(e2, "errno", None) == 28:
                    notify_admin_dedup(
                        "no_space",
                        "❌ No space left: не могу сохранить state.json. Освободи место (state_*.json, __pycache__, /tmp ffmpeg-*).",
                    )
                    return
                raise
        raise
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


def tg_call(method: str, payload: dict, *, timeout=(5, 15)) -> dict:
    """Return Telegram 'result'. Raises on network/API errors."""
    url = tg_api_url(method)
    r = http_request_tg("POST", url, json_body=payload, timeout=timeout)
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")
    return data["result"]


def notify_admin(text: str) -> None:
    text = _mask_secrets(text)
    # Admin notify should never break the main logic.
    try:
        with STATE_LOCK:
            st = load_state()
        chat_id = int(st.get("admin_private_chat_id") or 0)
        target = chat_id if chat_id != 0 else ADMIN_ID
        tg_call("sendMessage", {"chat_id": target, "text": text[:3500]}, timeout=(5, 15))
    except Exception as e:
        log_line(f"notify_admin failed: {e}")


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
        tg_call("deleteWebhook", {"drop_pending_updates": True}, timeout=(5, 15))
    except Exception as e:
        log_line(f"tg_drop_pending_updates_safe failed: {e}")


def tg_get_webhook_info() -> dict:
    return tg_call("getWebhookInfo", {}, timeout=(5, 15))


def tg_set_my_commands(commands: list, scope: dict | None = None) -> None:
    payload = {"commands": commands}
    if scope is not None:
        payload["scope"] = scope
    tg_call("setMyCommands", payload, timeout=(5, 15))


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
    # timeout for HTTP read MUST be > longpoll timeout
    eff_read = max(int(COMMAND_HTTP_TIMEOUT), int(timeout) + 15)
    r = http_request_tg("POST", url, json_body=payload, timeout=(5, eff_read))
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram getUpdates error: {data}")
    return data.get("result", [])


def tg_send_chat_action(chat_id: int, thread_id: int | None, action: str) -> None:
    try:
        payload = {"chat_id": int(chat_id), "action": action}
        if thread_id is not None:
            payload["message_thread_id"] = int(thread_id)
        tg_call("sendChatAction", payload, timeout=(5, 10))
    except Exception:
        pass


def tg_send_to(chat_id: int, thread_id: int | None, text: str, reply_to: int | None = None) -> int:
    payload = {"chat_id": chat_id, "text": text[:4000], "disable_web_page_preview": True, "parse_mode": "HTML"}
    if thread_id is not None:
        payload["message_thread_id"] = int(thread_id)
    if reply_to is not None:
        payload["reply_to_message_id"] = int(reply_to)
    res = tg_call("sendMessage", payload, timeout=(5, 15))
    return int(res["message_id"])


def tg_send(text: str) -> int:
    return tg_send_to(GROUP_ID, TOPIC_ID, text, reply_to=None)


def maybe_send_to_pubg_topic(text: str, st: dict, kick: dict) -> None:
    try:
        cat = (kick or {}).get("category")
        if cat and cat.strip() == PUBG_CATEGORY_MATCH:
            tg_send_to(PUBG_DUPLICATE_CHAT_ID, PUBG_DUPLICATE_TOPIC_ID, text, reply_to=None)
    except Exception as e:
        log_line(f"PUBG duplicate send error: {e}")


def tg_send_main_and_maybe_pubg(text: str, st: dict, kick: dict) -> None:
    tg_send(text)
    maybe_send_to_pubg_topic(text, st, kick)


def tg_send_photo_url_to(chat_id: int, thread_id: int | None, photo_url: str, caption: str, reply_to: int | None = None) -> int:
    payload = {"chat_id": chat_id, "photo": bust(photo_url), "caption": caption[:1024], "parse_mode": "HTML"}
    if thread_id is not None:
        payload["message_thread_id"] = int(thread_id)
    if reply_to is not None:
        payload["reply_to_message_id"] = int(reply_to)
    res = tg_call("sendPhoto", payload, timeout=(5, 25))
    return int(res["message_id"])


def tg_send_photo_upload_to(chat_id: int, thread_id: int | None, image_bytes: bytes, caption: str, filename: str, reply_to: int | None = None) -> int:
    url = tg_api_url("sendPhoto")
    data = {"chat_id": str(chat_id), "caption": caption[:1024], "parse_mode": "HTML"}
    if thread_id is not None:
        data["message_thread_id"] = str(thread_id)
    if reply_to is not None:
        data["reply_to_message_id"] = str(reply_to)
    files = {"photo": (filename, image_bytes)}
    # Upload may take longer
    r = http_request_tg("POST", url, data=data, files=files, timeout=(10, 45))
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
    r = http_request_ext("GET", u, headers=headers, timeout=25)
    return r.content


def tg_send_photo_best_to(chat_id: int, thread_id: int | None, photo_url: str, caption: str, reply_to: int | None = None) -> int:
    try:
        img = download_image(photo_url)
        return tg_send_photo_upload_to(chat_id, thread_id, img, caption, filename=f"thumb_{ts()}.jpg", reply_to=reply_to)
    except Exception as e:
        log_line(f"Photo upload fallback to URL. Reason: {e}")
        return tg_send_photo_url_to(chat_id, thread_id, photo_url, caption, reply_to=reply_to)


# ---------- FAST send helpers for command replies ----------

def tg_send_to_cmd(chat_id: int, thread_id: int | None, text: str, reply_to: int | None = None) -> int:
    payload = {"chat_id": chat_id, "text": text[:4000], "disable_web_page_preview": True, "parse_mode": "HTML"}
    if thread_id is not None:
        payload["message_thread_id"] = int(thread_id)
    if reply_to is not None:
        payload["reply_to_message_id"] = int(reply_to)
    res = tg_call("sendMessage", payload, timeout=(4, TG_CMD_SEND_TIMEOUT_SEC))
    return int(res["message_id"])


def tg_send_photo_url_to_cmd(chat_id: int, thread_id: int | None, photo_url: str, caption: str, reply_to: int | None = None) -> int:
    payload = {"chat_id": chat_id, "photo": bust(photo_url), "caption": caption[:1024], "parse_mode": "HTML"}
    if thread_id is not None:
        payload["message_thread_id"] = int(thread_id)
    if reply_to is not None:
        payload["reply_to_message_id"] = int(reply_to)
    res = tg_call("sendPhoto", payload, timeout=(4, TG_CMD_PHOTO_URL_TIMEOUT_SEC))
    return int(res["message_id"])


def tg_send_photo_upload_to_cmd(chat_id: int, thread_id: int | None, image_bytes: bytes, caption: str, filename: str, reply_to: int | None = None) -> int:
    url = tg_api_url("sendPhoto")
    data = {"chat_id": str(chat_id), "caption": caption[:1024], "parse_mode": "HTML"}
    if thread_id is not None:
        data["message_thread_id"] = str(thread_id)
    if reply_to is not None:
        data["reply_to_message_id"] = str(reply_to)
    files = {"photo": (filename, image_bytes)}
    r = http_request_tg("POST", url, data=data, files=files, timeout=(6, TG_CMD_PHOTO_UPLOAD_TIMEOUT_SEC))
    out = r.json()
    if not out.get("ok"):
        raise RuntimeError(f"Telegram API error: {out}")
    return int(out["result"]["message_id"])



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


def screenshot_from_m3u8_fast(playback_url: str) -> bytes | None:
    # Same as screenshot_from_m3u8 but with shorter timeout for commands.
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
        p = subprocess.run(cmd, capture_output=True, timeout=min(int(FFMPEG_TIMEOUT_SEC), int(FFMPEG_CMD_TIMEOUT_SEC)))
        if p.returncode != 0 or not p.stdout:
            return None
        return p.stdout
    except Exception:
        return None



# ========== KICK ==========

def kick_fetch() -> dict:
    r = http_request_ext("GET", KICK_API_URL, headers=HEADERS_JSON, timeout=25)
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
    r = http_request_ext("GET", VK_PUBLIC_URL, headers=HEADERS_HTML, timeout=25, allow_redirects=True)
    html = r.text

    title = None
    category = None
    viewers = None
    thumb = None
    live = False

    # Parse __NEXT_DATA__ for live info (best-effort)
    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL | re.IGNORECASE)
    if m:
        try:
            data = json.loads(m.group(1))
            container = _find_container_with_streaminfo(data)
            if container:
                ch = container.get("channelInfo") or {}
                si = container.get("streamInfo") or {}

                status = str(ch.get("status") or "").upper()
                live = status in {"ONLINE", "LIVE", "STREAMING"}

                title = si.get("title") or title
                catobj = si.get("category") or {}
                if isinstance(catobj, dict):
                    category = catobj.get("title") or category

                cnt = si.get("counters") or {}
                if isinstance(cnt, dict):
                    viewers = cnt.get("viewers") or viewers
                if isinstance(viewers, int) and viewers > 0:
                    live = True
        except Exception:
            pass

    # Fallback: og tags
    m_img = re.search(r'property="og:image"[^>]+content="([^"]+)"', html, re.IGNORECASE)
    if m_img:
        thumb = m_img.group(1).strip()
    m_title = re.search(r'property="og:title"[^>]+content="([^"]+)"', html, re.IGNORECASE)
    if m_title and not title:
        title = m_title.group(1).strip()

    return {
        "live": bool(live),
        "title": trim(title, MAX_TITLE_LEN),
        "category": trim(category, MAX_GAME_LEN),
        "viewers": viewers,
        "thumb": thumb,
    }


# ========== MESSAGES ==========

def build_caption(prefix: str, st: dict, kick: dict, vk: dict) -> str:
    # Telegram parse_mode is HTML.
    running = fmt_running_line(st)

    lines: list[str] = []
    if prefix:
        lines.append(prefix)
        lines.append("")

    lines.append(f"🕒 <b>Сейчас (МСК):</b> {now_msk_str()}")
    if st.get("started_at"):
        lines.append(f"🕒 <b>Старт (МСК):</b> {fmt_msk(dt_from_iso(st.get('started_at')))}")
    lines.append(f"⏱ <b>{esc(running)}</b>")
    lines.append("")

    lines.append("🎥 <b>Kick</b>")
    if kick.get("live"):
        if kick.get("category"):
            lines.append(f"🏷 Категория: <b>{esc(kick.get('category'))}</b>")
        if kick.get("title"):
            lines.append(f"📝 Название: <i>{esc(kick.get('title'))}</i>")
        lines.append(f"👥 Зрители: <b>{fmt_viewers(kick.get('viewers'))}</b>")
    else:
        lines.append("⚫ OFF")
    lines.append("")

    lines.append("🎮 <b>VK Play</b>")
    if vk.get("live"):
        if vk.get("category"):
            lines.append(f"🏷 Категория: <b>{esc(vk.get('category'))}</b>")
        if vk.get("title"):
            lines.append(f"📝 Название: <i>{esc(vk.get('title'))}</i>")
        lines.append(f"👥 Зрители: <b>{fmt_viewers(vk.get('viewers'))}</b>")
    else:
        lines.append("⚫ OFF")

    lines.append("")
    lines.append(f"🔗 <b>Kick:</b> {KICK_PUBLIC_URL}")
    lines.append(f"🔗 <b>VK Play:</b> {VK_PUBLIC_URL}")

    return "\n".join(lines)

def build_end_text(st: dict) -> str:
    return build_end_report(st)



def build_no_stream_text(prefix: str = "⚫ <b>Патока сейчас нет</b>") -> str:
    return "\n".join([
        prefix,
        "",
        f"🔗 <b>Kick:</b> {KICK_PUBLIC_URL}",
        f"🔗 <b>VK Play:</b> {VK_PUBLIC_URL}",
    ])

def set_started_at_from_kick(st: dict, kick: dict, force: bool = False) -> None:
    # Use Kick created_at to keep stream start time accurate across restarts and between streams.
    sync_kick_session(st, kick, force=force)

def send_status_with_screen_to(prefix: str, st: dict, kick: dict, vk: dict, chat_id: int, thread_id: int | None, reply_to: int | None) -> None:
    caption = build_caption(prefix, st, kick, vk)

    # show user bot is working
    tg_send_chat_action(chat_id, thread_id, "upload_photo")

    # 1) real screenshot from m3u8 (main feature)
    shot = screenshot_from_m3u8(kick.get("playback_url")) if kick.get("live") else None
    if shot:
        tg_send_photo_upload_to(chat_id, thread_id, shot, caption, filename=f"kick_live_{ts()}.jpg", reply_to=reply_to)
        maybe_send_to_pubg_topic(caption, st, kick)
        return

    # 2) fallbacks
    if kick.get("live") and kick.get("thumb"):
        tg_send_photo_best_to(chat_id, thread_id, kick["thumb"], caption, reply_to=reply_to)
        maybe_send_to_pubg_topic(caption, st, kick)
        return

    if vk.get("live") and vk.get("thumb"):
        tg_send_photo_best_to(chat_id, thread_id, vk["thumb"], caption, reply_to=reply_to)
        maybe_send_to_pubg_topic(caption, st, kick)
        return

    tg_send_to(chat_id, thread_id, caption, reply_to=reply_to)
    maybe_send_to_pubg_topic(caption, st, kick)






def build_change_caption(st: dict, kick: dict, vk: dict,
                         kick_title_changed: bool, kick_cat_changed: bool,
                         vk_title_changed: bool, vk_cat_changed: bool) -> str:
    lines: list[str] = []
    lines.append("🟡 <b>Обновление патока</b>")
    lines.append("")

    if kick.get("live") and (kick_title_changed or kick_cat_changed):
        lines.append("🎥 <b>Kick</b>")
        if kick_cat_changed and kick.get("category"):
            lines.append(f"🏷 <b>Категория:</b> <b>{esc(kick.get('category'))}</b>")
        if kick_title_changed and kick.get("title"):
            lines.append(f"📝 <b>Название:</b> <i>{esc(kick.get('title'))}</i>")
        lines.append("")

    if vk.get("live") and (vk_title_changed or vk_cat_changed):
        lines.append("🎮 <b>VK Play</b>")
        if vk_cat_changed and vk.get("category"):
            lines.append(f"🏷 <b>Категория:</b> <b>{esc(vk.get('category'))}</b>")
        if vk_title_changed and vk.get("title"):
            lines.append(f"📝 <b>Название:</b> <i>{esc(vk.get('title'))}</i>")
        lines.append("")

    lines.append(f"🕒 {now_msk_str()} (МСК) • ⏱ {esc(fmt_running_line(st))}")
    lines.append(f"🔗 {KICK_PUBLIC_URL}")
    lines.append(f"🔗 {VK_PUBLIC_URL}")
    return "\n".join(lines)


def send_caption_with_screen(caption: str, st: dict, kick: dict, vk: dict) -> None:
    # Prefer platform thumbnails; fallback to text.
    try:
        if kick.get("live") and kick.get("thumb"):
            tg_send_photo_best_to(GROUP_ID, TOPIC_ID, kick.get("thumb"), caption, reply_to=None)
            maybe_send_to_pubg_topic(caption, st, kick)
            return
        if vk.get("live") and vk.get("thumb"):
            tg_send_photo_best_to(GROUP_ID, TOPIC_ID, vk.get("thumb"), caption, reply_to=None)
            maybe_send_to_pubg_topic(caption, st, kick)
            return
    except Exception:
        pass

    tg_send_main_and_maybe_pubg(caption, st, kick)

def send_status_with_screen_to_cmd(prefix: str, st: dict, kick: dict, vk: dict, chat_id: int, thread_id: int | None, reply_to: int | None) -> None:
    caption = build_caption(prefix, st, kick, vk)

    shot = None
    if kick.get("live"):
        cached = _shot_cache_get()
        if cached:
            shot, _age = cached
        else:
            shot = screenshot_from_m3u8_fast(kick.get("playback_url"))
            if shot:
                _shot_cache_set(shot)

    if shot:
        tg_send_photo_upload_to_cmd(chat_id, thread_id, shot, caption, filename=f"kick_live_{ts()}.jpg", reply_to=reply_to)
        maybe_send_to_pubg_topic(caption, st, kick)
        return

    if kick.get("live") and kick.get("thumb"):
        try:
            img = download_image(kick.get("thumb"))
            tg_send_photo_upload_to_cmd(chat_id, thread_id, img, caption, filename=f"thumb_{ts()}.jpg", reply_to=reply_to)
        except Exception:
            tg_send_photo_url_to_cmd(chat_id, thread_id, kick.get("thumb"), caption, reply_to=reply_to)
        maybe_send_to_pubg_topic(caption, st, kick)
        return

    if vk.get("live") and vk.get("thumb"):
        try:
            img = download_image(vk.get("thumb"))
            tg_send_photo_upload_to_cmd(chat_id, thread_id, img, caption, filename=f"thumb_{ts()}.jpg", reply_to=reply_to)
        except Exception:
            tg_send_photo_url_to_cmd(chat_id, thread_id, vk.get("thumb"), caption, reply_to=reply_to)
        maybe_send_to_pubg_topic(caption, st, kick)
        return

    tg_send_to_cmd(chat_id, thread_id, caption, reply_to=reply_to)
    maybe_send_to_pubg_topic(caption, st, kick)

def send_status_with_screen(prefix: str, st: dict, kick: dict, vk: dict) -> None:
    send_status_with_screen_to(prefix, st, kick, vk, GROUP_ID, TOPIC_ID, reply_to=None)


# ========== ADMIN DIAG ==========

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
    end_streak = int(st.get("end_streak") or 0)

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
        actions.append("2) Если всё так же — вероятно сеть/хостинг, нужен перезапуск.")
        actions.append("3) Если часто так бывает — смотри, не запущен ли второй экземпляр (409 Conflict).")

    if last_rec:
        actions.append("ℹ️ Watchdog уже срабатывал — бот сам пытался починиться.")

    return (
        "Админ-проверка (простыми словами)\n\n"
        "Стрим сейчас:\n"
        f"- Идёт ли стрим: {_yes_no(any_live)} (Kick: {_yes_no(kick_live)}, VK: {_yes_no(vk_live)})\n"
        f"- Время старта: {started_at}\n"
        f"- Подтверждений конца: {end_streak} (нужно {END_CONFIRM_STREAK}) ✅\n\n"
        "Команды в Телеграм:\n"
        f"- Бот “на связи”: {on_air_icon} {on_air_text} (последний опрос: {_age_str(poll_age)} назад)\n"
        f"- Последняя команда (/stream и т.п.): {_age_str(cmd_age)} назад\n"
        f"- Самовосстановление (watchdog): {_age_str(rec_age)} назад\n\n"
        "Очередь сообщений Telegram:\n"
        f"- Webhook: {webhook_state}\n"
        f"- В очереди Telegram: {esc(pend)} (сколько апдейтов ждут доставки)\n"
        f"- Указатель очереди (offset): {offset} (с какого update_id продолжаем)\n\n"
        "Что делать:\n"
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
    return ch.get("type") == "private"


def is_admin_msg(msg: dict) -> bool:
    fr = msg.get("from") or {}
    uid = fr.get("id")
    return isinstance(uid, int) and uid == ADMIN_ID


def commands_loop_forever():
    while True:
        try:
            commands_loop_once()
        except Exception as e:
            # Even this outer loop should not stall for long.
            if is_telegram_conflict_409(e):
                notify_409_dedup("⚠️ Telegram 409 Conflict (getUpdates): есть другой polling на этом токене. Проверь, не запущено ли где-то ещё.")
                time.sleep(10)
                continue
            log_line(f"commands_loop_forever error: {e}\n{traceback.format_exc()[:1500]}")
            time.sleep(LOOP_CRASH_SLEEP)


def commands_loop_once():
    if not COMMANDS_ENABLED:
        time.sleep(5)
        return

    with STATE_LOCK:
        st = load_state()
    offset = int(st.get("updates_offset") or 0)

    try:
        updates = tg_get_updates(offset=offset, timeout=COMMAND_POLL_TIMEOUT)
    except Exception as e:
        # Network glitches are expected; just wait a bit and continue.
        log_line(f"getUpdates failed: {e}")
        time.sleep(1)
        return

    now_ts = ts()
    with STATE_LOCK:
        st2 = load_state()
        last_saved = int(st2.get("last_updates_poll_ts") or 0)
        if now_ts - last_saved >= COMMAND_STATE_SAVE_SEC:
            st2["last_updates_poll_ts"] = now_ts
            save_state(st2)

    max_update_id = None

    for upd in updates:
        uid = upd.get("update_id")
        if isinstance(uid, int):
            max_update_id = uid if (max_update_id is None or uid > max_update_id) else max_update_id

        msg = upd.get("message") or {}
        text = msg.get("text") or ""
        if not text:
            continue

        try:
            # remember admin private chat id
            if is_private_chat(msg) and is_admin_msg(msg):
                with STATE_LOCK:
                    stx = load_state()
                    stx["admin_private_chat_id"] = int((msg.get("chat") or {}).get("id") or 0)
                    save_state(stx)
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
                        stx = load_state()
                        stx["updates_offset"] = 0
                        save_state(stx)
                    try:
                        tg_send_to(chat_id, None, "OK: updates_offset сброшен в 0.", reply_to=reply_to)
                    except Exception as e:
                        log_line(f"send admin_reset_offset reply failed: {e}")
                    continue

                # /admin
                with STATE_LOCK:
                    stx = load_state()
                try:
                    wh = tg_get_webhook_info()
                except Exception as e:
                    wh = {"error": str(e)}
                try:
                    tg_send_to(chat_id, None, build_admin_diag_text(stx, wh), reply_to=reply_to)
                except Exception as e:
                    log_line(f"send /admin reply failed: {e}")
                continue

            if not is_status_command(text):
                continue

            with STATE_LOCK:
                stx = load_state()
                stx["last_command_seen_ts"] = ts()
                save_state(stx)
            # Fetch current status (cache-first; avoids long waits on Kick/VK)
            snap = _cache_get_snapshot()
            if snap is not None:
                st_cur, kick, vk, _age = snap
            else:
                try:
                    kick = kick_fetch()
                except Exception as e:
                    kick = {"live": False, "title": None, "category": None, "viewers": None, "thumb": None, "created_at": None, "playback_url": None}
                    log_line(f"Kick fetch (command) error: {e}")

                try:
                    vk = vk_fetch_best_effort()
                except Exception as e:
                    vk = {"live": False, "title": None, "category": None, "viewers": None, "thumb": None}
                    log_line(f"VK fetch (command) error: {e}")

                with STATE_LOCK:
                    st_cur = load_state()
                st_cur["any_live"] = bool(kick.get("live") or vk.get("live"))
                st_cur["kick_live"] = bool(kick.get("live"))
                st_cur["vk_live"] = bool(vk.get("live"))
                if st_cur["any_live"]:
                    set_started_at_from_kick(st_cur, kick)
                    st_cur["end_streak"] = 0
                st_cur["kick_title"] = kick.get("title")
                st_cur["kick_cat"] = kick.get("category")
                st_cur["vk_title"] = vk.get("title")
                st_cur["vk_cat"] = vk.get("category")
                st_cur["kick_viewers"] = kick.get("viewers")
                st_cur["vk_viewers"] = vk.get("viewers")
                save_state(st_cur)

            if not (kick.get("live") or vk.get("live")):
                try:
                    tg_send_to(chat_id, thread_id, build_no_stream_text("Сейчас на канале Глад Валакас патока нет!"), reply_to=reply_to)
                except Exception as e:
                    log_line(f"send no-stream reply failed: {e}")
            else:
                try:
                    send_status_with_screen_to("📌 Текущее состояние патока", st_cur, kick, vk, chat_id, thread_id, reply_to)
                except Exception as e:
                    # Do not kill polling loop on timeouts; log and continue.
                    log_line(f"send_status_with_screen_to failed: {e}")

        except Exception as e:
            log_line(f"command processing error: {e}\n{traceback.format_exc()[:1200]}")

    # Always advance offset even if sending failed; otherwise bot will re-process old commands.
    if max_update_id is not None:
        with STATE_LOCK:
            st3 = load_state()
            st3["updates_offset"] = int(max_update_id) + 1
            save_state(st3)


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
            now_ts = ts()

            if last_poll == 0:
                time.sleep(10)
                continue

            silent = (now_ts - last_poll) >= COMMANDS_WATCHDOG_SILENCE_SEC
            cooldown_ok = (now_ts - last_recover) >= COMMANDS_WATCHDOG_COOLDOWN_SEC

            if silent and cooldown_ok:
                notify_admin_dedup("watchdog_triggered", "⚠️ Watchdog: getUpdates давно не отрабатывал, делаю восстановление...")
                tg_drop_pending_updates_safe()

                with STATE_LOCK:
                    st2 = load_state()
                    st2["updates_offset"] = 0
                    st2["last_commands_recover_ts"] = now_ts
                    save_state(st2)

                if COMMANDS_WATCHDOG_PING_ENABLED:
                    notify_admin_dedup("watchdog_recovered", "✅ Watchdog: восстановил polling команд.")

        except Exception as e:
            log_line(f"commands_watchdog error: {e}\n{traceback.format_exc()[:1200]}")

        time.sleep(10)


# ========== MAIN LOOP ==========

def main_loop_forever():
    while True:
        try:
            main_loop()
        except Exception as e:
            notify_admin_dedup("main_loop_crash", f"main_loop crashed: {e}\n{traceback.format_exc()[:1500]}")
            time.sleep(LOOP_CRASH_SLEEP)


def main_loop():
    # init fetch
    try:
        kick0 = kick_fetch()
    except Exception as e:
        kick0 = {"live": False, "title": None, "category": None, "viewers": None, "thumb": None, "created_at": None, "playback_url": None}
        log_line(f"Kick init fetch error: {e}")

    try:
        vk0 = vk_fetch_best_effort()
    except Exception as e:
        vk0 = {"live": False, "title": None, "category": None, "viewers": None, "thumb": None}
        log_line(f"VK init fetch error: {e}")

    any_live0 = bool(kick0.get("live") or vk0.get("live"))

    with STATE_LOCK:
        st = load_state()
        st["any_live"] = any_live0
        st["kick_live"] = bool(kick0.get("live"))
        st["vk_live"] = bool(vk0.get("live"))
        if any_live0:
            set_started_at_from_kick(st, kick0)
            st["end_streak"] = 0
        st["kick_title"] = kick0.get("title")
        st["kick_cat"] = kick0.get("category")
        st["vk_title"] = vk0.get("title")
        st["vk_cat"] = vk0.get("category")
        st["kick_viewers"] = kick0.get("viewers")
        st["vk_viewers"] = vk0.get("viewers")
        stats_tick(st, kick0, vk0, any_live0, now_ts=ts())
        save_state(st)

    # startup ping
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
            log_line(f"Startup ping failed: {e}")

    # no-stream on start
    if NO_STREAM_ON_START_MESSAGE and (not any_live0):
        with STATE_LOCK:
            st = load_state()
            last_ts = int(st.get("last_no_stream_start_ts") or 0)
        if ts() - last_ts >= NO_STREAM_START_DEDUP_SEC:
            try:
                tg_send(build_no_stream_text("Сейчас на канале Глад Валакас патока нет!"))
            except Exception as e:
                log_line(f"No-stream-on-start send error: {e}")
            with STATE_LOCK:
                st = load_state()
                st["last_no_stream_start_ts"] = ts()
                save_state(st)

    # boot status
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
            log_line(f"Boot status send error: {e}")

    cleanup_counter = 0

    while True:
        try:
            kick = kick_fetch()
        except Exception as e:
            kick = {"live": False, "title": None, "category": None, "viewers": None, "thumb": None, "created_at": None, "playback_url": None}
            log_line(f"Kick fetch error: {e}")

        try:
            vk = vk_fetch_best_effort()
        except Exception as e:
            vk = {"live": False, "title": None, "category": None, "viewers": None, "thumb": None}
            log_line(f"VK fetch error: {e}")

        with STATE_LOCK:
            st = load_state()
            prev_any = bool(st.get("any_live"))
            prev_end_streak = int(st.get("end_streak") or 0)

        any_live = bool(kick.get("live") or vk.get("live"))

        # START
        if (not prev_any) and any_live:
            with STATE_LOCK:
                st = load_state()
                last = int(st.get("last_start_sent_ts") or 0)
            if ts() - last >= START_DEDUP_SEC:
                with STATE_LOCK:
                    st_start = load_state()
                    # New stream session: force sync from Kick so start time/duration won't stick.
                    reset_stream_session(st_start)
                    set_started_at_from_kick(st_start, kick, force=True)
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
                    log_line(f"Start send error: {e}")

        # CHANGE

        kick_title_changed = False

        kick_cat_changed = False

        vk_title_changed = False

        vk_cat_changed = False


        with STATE_LOCK:

            st = load_state()

            if kick.get("live"):

                kick_title_changed = (kick.get("title") != st.get("kick_title"))

                kick_cat_changed = (kick.get("category") != st.get("kick_cat"))

            if vk.get("live"):

                vk_title_changed = (vk.get("title") != st.get("vk_title"))

                vk_cat_changed = (vk.get("category") != st.get("vk_cat"))


        changed = (kick_title_changed or kick_cat_changed or vk_title_changed or vk_cat_changed)


        if any_live and prev_any and changed:
            with STATE_LOCK:
                st = load_state()
                last = int(st.get("last_change_sent_ts") or 0)
            if ts() - last >= CHANGE_DEDUP_SEC:
                try:
                    with STATE_LOCK:
                        st = load_state()
                    caption = build_change_caption(st, kick, vk, kick_title_changed, kick_cat_changed, vk_title_changed, vk_cat_changed)
                    send_caption_with_screen(caption, st, kick, vk)
                    with STATE_LOCK:
                        st = load_state()
                        st["last_change_sent_ts"] = ts()
                        save_state(st)
                except Exception as e:
                    log_line(f"Change send error: {e}")

        # END (once per started_at)
        should_send_end = False
        with STATE_LOCK:
            st_chk = load_state()
            cur_started = st_chk.get("started_at")
            already_for = st_chk.get("end_sent_for_started_at")
            confirmed_off = (not any_live) and ((prev_end_streak + 1) >= END_CONFIRM_STREAK)
            if confirmed_off and cur_started and (already_for != cur_started):
                should_send_end = True

        if should_send_end:
            try:
                with STATE_LOCK:
                    st_end = load_state()
                    # Finalize stats up to now (counts the last interval)
                    stats_tick(st_end, kick, vk, any_live=False, now_ts=ts())
                    stats_finalize_end(st_end, now_ts=ts())
                    st_end["kick_viewers"] = st_end.get("kick_viewers") or kick.get("viewers")
                    st_end["vk_viewers"] = st_end.get("vk_viewers") or vk.get("viewers")
                    st_end["end_sent_for_started_at"] = st_end.get("started_at")
                    st_end["end_sent_ts"] = ts()
                    save_state(st_end)
                end_text = build_end_text(st_end)
                tg_send_main_and_maybe_pubg(end_text, st_end, kick)
            except Exception as e:
                log_line(f"End send error: {e}")

        # SAVE NEW STATE
        with STATE_LOCK:
            st = load_state()
            st["any_live"] = any_live
            st["kick_live"] = bool(kick.get("live"))
            st["vk_live"] = bool(vk.get("live"))
            if any_live:
                set_started_at_from_kick(st, kick)
                st["end_streak"] = 0
            else:
                st["started_at"] = None
                st["end_streak"] = prev_end_streak + 1
            st["kick_title"] = kick.get("title")
            st["kick_cat"] = kick.get("category")
            st["vk_title"] = vk.get("title")
            st["vk_cat"] = vk.get("category")
            st["kick_viewers"] = kick.get("viewers")
            st["vk_viewers"] = vk.get("viewers")
            stats_tick(st, kick, vk, any_live, now_ts=ts())
            save_state(st)
        try:
            _cache_set_snapshot(st, kick, vk)
        except Exception:
            pass

        # Periodic cleanup + quota monitor
        cleanup_counter += 1
        if cleanup_counter >= DISK_CHECK_INTERVAL:
            cleanup_temp_files()
            cleanup_old_state_backups()

            q_percent, q_used, q_total = quota_usage_for_bot()
            with STATE_LOCK:
                stq = load_state()
                last_nt = int(stq.get("last_quota_notify_ts") or 0)
            cooldown_ok = (ts() - last_nt) >= BOT_NOTIFY_COOLDOWN_SEC

            if q_percent >= BOT_WARN_PERCENT and cooldown_ok:
                top = list_largest_files(os.getcwd(), BOT_TOP_FILES)
                top_text = ""
                if top:
                    top_lines = "\n".join([f"- {fmt_bytes(sz)} — {path}" for sz, path in top])
                    top_text = "\n\nТоп файлов по размеру:\n" + top_lines

                notify_admin_dedup(
                    "quota_high",
                    "⚠️ Квота диска почти заполнена (по размеру папки бота).\n"
                    f"Занято ботом: {fmt_bytes(q_used)} из {fmt_bytes(q_total)} ({q_percent:.1f}%)."
                    + top_text
                    + "\n\nОчищаю temp/__pycache__…",
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




def screenshot_refresher_forever() -> None:
    # Refresh a real screenshot in RAM while Kick is live.
    while True:
        try:
            snap = _cache_get_snapshot()
            if snap is None:
                time.sleep(2)
                continue
            _st, kick, _vk, _age = snap
            if not kick.get("live"):
                time.sleep(max(2, int(SHOT_REFRESH_SEC)))
                continue
            if _shot_cache_get() is not None:
                time.sleep(max(2, int(SHOT_REFRESH_SEC)))
                continue
            img = screenshot_from_m3u8_fast(kick.get("playback_url"))
            if img:
                _shot_cache_set(img)
            time.sleep(max(2, int(SHOT_REFRESH_SEC)))
        except Exception:
            time.sleep(3)

def main():
    log_line(f"[cfg] COMMAND_POLL_TIMEOUT={COMMAND_POLL_TIMEOUT} COMMAND_HTTP_TIMEOUT={COMMAND_HTTP_TIMEOUT}")

    cleanup_temp_files()
    cleanup_old_state_backups()

    # Drop pending updates once at startup (helps after redeploy)
    tg_drop_pending_updates_safe()

    try:
        setup_commands_visibility()
    except Exception as e:
        log_line(f"Setup commands visibility failed: {e}")

    if COMMANDS_ENABLED:
        threading.Thread(target=commands_loop_forever, daemon=True).start()
        threading.Thread(target=commands_watchdog_forever, daemon=True).start()

    main_loop_forever()


if __name__ == "__main__":
    main()
