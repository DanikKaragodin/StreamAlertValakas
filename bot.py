import os
import re
import json
import time
from datetime import datetime, timezone

import requests


# --- CONFIG from env ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

GROUP_ID = int(os.getenv("GROUP_ID", "-1002977868330"))
TOPIC_ID = int(os.getenv("TOPIC_ID", "65114"))

KICK_SLUG = os.getenv("KICK_SLUG", "gladvalakaspwnz").strip()
VK_SLUG = os.getenv("VK_SLUG", "gladvalakas").strip()

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))

STATE_FILE = os.getenv("STATE_FILE", "state.json")


# --- URLs ---
KICK_API_URL = f"https://kick.com/api/v1/channels/{KICK_SLUG}"
KICK_PUBLIC_URL = f"https://kick.com/{KICK_SLUG}"

VK_PUBLIC_URL = f"https://live.vkvideo.ru/{VK_SLUG}"  # у тебя это точная ссылка


# --- helpers ---
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
HEADERS = {"User-Agent": UA, "Accept": "application/json,text/plain,*/*"}


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
            "started_at": None,          # ISO
            "main_message_id": None,

            "kick_title": None,
            "kick_cat": None,
            "kick_viewers": None,

            "vk_title": None,
            "vk_cat": None,
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


def tg_send(text: str) -> int:
    # message_thread_id нужен, чтобы писать именно в нужную тему (topic)
    payload = {
        "chat_id": GROUP_ID,
        "message_thread_id": TOPIC_ID,
        "text": text,
        "disable_web_page_preview": True,
    }
    res = tg_call("sendMessage", payload)
    return int(res["message_id"])


def tg_edit(message_id: int, text: str) -> None:
    payload = {
        "chat_id": GROUP_ID,
        "message_id": message_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    tg_call("editMessageText", payload)


def kick_fetch() -> dict:
    """
    Возвращает:
      live(bool), title(str|None), category(str|None), viewers(int|None)
    """
    r = requests.get(KICK_API_URL, headers=HEADERS, timeout=25)
    r.raise_for_status()
    data = r.json()

    ls = data.get("livestream") or {}
    is_live = bool(ls.get("is_live"))

    title = ls.get("session_title") or None
    viewers = ls.get("viewer_count") or ls.get("viewers") or None

    cat = None
    cats = ls.get("categories") or []
    if cats and isinstance(cats, list):
        cat = cats[0].get("name") or None

    return {
        "live": is_live,
        "title": title,
        "category": cat,
        "viewers": viewers,
    }


def _find_container_with_streaminfo(obj):
    """
    Ищем в __NEXT_DATA__ блок похожий на структуру:
      { channelInfo: {status: ...}, streamInfo: {title, category{title}, counters{viewers}} }
    """
    if isinstance(obj, dict):
        if "streamInfo" in obj and isinstance(obj.get("streamInfo"), dict):
            si = obj["streamInfo"]
            if "title" in si or ("category" in si and "counters" in si):
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
    """
    Best-effort парсинг VK Video Live:
    - Берём HTML страницы и пытаемся вытащить __NEXT_DATA__ (Next.js).
    - Если не получилось — хотя бы og:title.
    Может сломаться, если VK поменяет верстку.
    """
    r = requests.get(VK_PUBLIC_URL, headers={"User-Agent": UA}, timeout=25, allow_redirects=True)
    r.raise_for_status()
    html = r.text

    title = None
    category = None
    viewers = None
    live = False

    # 1) Пытаемся Next.js __NEXT_DATA__
    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL | re.IGNORECASE)
    if m:
        try:
            data = json.loads(m.group(1))
            container = _find_container_with_streaminfo(data)
            if container:
                ch = container.get("channelInfo") or {}
                si = container.get("streamInfo") or {}

                # status часто бывает строкой
                status = (ch.get("status") or "").upper()
                live = status in {"ONLINE", "LIVE", "STREAMING"}

                title = si.get("title") or title
                cat_obj = si.get("category") or {}
                category = cat_obj.get("title") or category
                cnt = si.get("counters") or {}
                viewers = cnt.get("viewers") or viewers

                # fallback live: если есть viewers > 0
                if viewers and isinstance(viewers, int) and viewers > 0:
                    live = True
        except Exception:
            pass

    # 2) Fallback: og:title
    if not title:
        m2 = re.search(r'<meta[^>]+property="og:title"[^>]+content="([^"]+)"', html, re.IGNORECASE)
        if m2:
            title = m2.group(1).strip()

    return {
        "live": bool(live),
        "title": title,
        "category": category,
        "viewers": viewers,
    }


def build_main_text(st: dict) -> str:
    kick_line = "Kick: OFF"
    if st.get("kick_live"):
        kick_line = f"Kick: {st.get('kick_cat') or '—'} — {st.get('kick_title') or '—'}"

    vk_line = "VK: OFF"
    if st.get("vk_live"):
        vk_line = f"VK: {st.get('vk_cat') or '—'} — {st.get('vk_title') or '—'}"

    return (
        "🧩 Глад Валакас завёл стрим!\n\n"
        f"{kick_line}\n"
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
        f"Стрим Глад Валакаса закончился\n"
        f"Зрителей на стриме: {viewers}\n"
        f"Длительность: {dur}\n\n"
        f"Kick: {KICK_PUBLIC_URL}\n"
        f"VK: {VK_PUBLIC_URL}"
    )


def main():
    st = load_state()

    while True:
        try:
            kick = kick_fetch()
        except Exception:
            kick = {"live": False, "title": None, "category": None, "viewers": None}

        try:
            vk = vk_fetch_best_effort()
        except Exception:
            vk = {"live": False, "title": None, "category": None, "viewers": None}

        prev_any = bool(st.get("any_live"))
        prev_kick = bool(st.get("kick_live"))
        prev_vk = bool(st.get("vk_live"))

        st["kick_live"] = bool(kick["live"])
        st["kick_title"] = kick["title"]
        st["kick_cat"] = kick["category"]
        st["kick_viewers"] = kick["viewers"]

        st["vk_live"] = bool(vk["live"])
        st["vk_title"] = vk["title"]
        st["vk_cat"] = vk["category"]
        st["vk_viewers"] = vk["viewers"]

        st["any_live"] = st["kick_live"] or st["vk_live"]

        # частичное завершение — отдельным сообщением
        if prev_kick and (not st["kick_live"]) and st["vk_live"]:
            tg_send(f"Kick-стрим закончился, на VK продолжается:\n{VK_PUBLIC_URL}")
        if prev_vk and (not st["vk_live"]) and st["kick_live"]:
            tg_send(f"VK-стрим закончился, на Kick продолжается:\n{KICK_PUBLIC_URL}")

        # общий старт
        if (not prev_any) and st["any_live"]:
            st["started_at"] = now_utc().isoformat()
            main_id = tg_send(build_main_text(st))
            st["main_message_id"] = main_id

        # обновление главного поста (когда стрим идёт)
        if st["any_live"] and st.get("main_message_id"):
            new_text = build_main_text(st)
            if new_text != st.get("_last_main_text"):
                try:
                    tg_edit(int(st["main_message_id"]), new_text)
                except Exception:
                    # если не получилось отредактировать (например, сообщение удалено) — создадим новое
                    main_id = tg_send(new_text)
                    st["main_message_id"] = main_id
                st["_last_main_text"] = new_text

        # общий конец
        if prev_any and (not st["any_live"]):
            tg_send(build_end_text(st))
            st["started_at"] = None
            st["main_message_id"] = None
            st["_last_main_text"] = None

        save_state(st)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
