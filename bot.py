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

# опционально: куда слать ошибки (твой личный чат id)
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
            "started_at": None,          # ISO
            "main_message_id": None,
            "_last_main_text": None,
            "startup_ping_sent": False,

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


def notify_admin(text: str) -> None:
    if not ADMIN_CHAT_ID:
        return
    try:
        payload = {
            "chat_id": int(ADMIN_CHAT_ID),
            "text": text[:3500],
            "disable_web_page_preview": True,
        }
        tg_call("sendMessage", payload)
    except Exception:
        pass


# ========== KICK ==========
def kick_fetch() -> dict:
    """
    Ожидаемые поля в ответе /api/v2/channels/{slug}:
      livestream.is_live
      livestream.session_title
      livestream.viewer_count (или viewers)
      livestream.categories[0].name
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

    return {"live": is_live, "title": title, "category": cat, "viewers": viewers}


# ========== VK (best-effort HTML parse) ==========
def _find_container_with_streaminfo(obj):
    """
    Ищем в __NEXT_DATA__ блок похожий на структуру:
      { channelInfo: {status: ...}, streamInfo: {title, category{title}, counters{viewers}} }
    """
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
    """
    Best-effort: берём HTML страницы VK Video Live и пробуем распарсить __NEXT_DATA__.
    Если VK изменит верстку — может сломаться (тогда надо будет менять парсер).
    """
    r = requests.get(VK_PUBLIC_URL, headers=HEADERS_HTML, timeout=25, allow_redirects=True)
    r.raise_for_status()
    html = r.text

    title = None
    category = None
    viewers = None
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

    if not title:
        m2 = re.search(r'<meta[^>]+property="og:title"[^>]+content="([^"]+)"', html, re.IGNORECASE)
        if m2:
            title = m2.group(1).strip()

    return {"live": bool(live), "title": title, "category": category, "viewers": viewers}


# ========== MESSAGE BUILDERS ==========
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
        "Стрим Глад Валакаса закончился\n"
        f"Зрителей на стриме: {viewers}\n"
        f"Длительность: {dur}\n\n"
        f"Kick: {KICK_PUBLIC_URL}\n"
        f"VK: {VK_PUBLIC_URL}"
    )


# ========== MAIN LOOP ==========
def main():
    st = load_state()

    # 1) первичная проверка (один раз)
    if not st.get("startup_ping_sent"):
        try:
            tg_send("✅ StreamAlertValakas запущен (ping).")
            st["startup_ping_sent"] = True
            save_state(st)
        except Exception as e:
            notify_admin(f"Startup ping failed: {e}")
            # если нет прав/токена — дальше смысла мало, но оставим цикл чтобы логи/рестарт помогли
            time.sleep(10)

    while True:
        try:
            kick = kick_fetch()
        except Exception as e:
            kick = {"live": False, "title": None, "category": None, "viewers": None}
            notify_admin(f"Kick fetch error: {e}")

        try:
            vk = vk_fetch_best_effort()
        except Exception as e:
            vk = {"live": False, "title": None, "category": None, "viewers": None}
            notify_admin(f"VK fetch error: {e}")

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

        # 2) частичное завершение — отдельным сообщением
        try:
            if prev_kick and (not st["kick_live"]) and st["vk_live"]:
                tg_send(f"Kick-стрим закончился, на VK продолжается:\n{VK_PUBLIC_URL}")
            if prev_vk and (not st["vk_live"]) and st["kick_live"]:
                tg_send(f"VK-стрим закончился, на Kick продолжается:\n{KICK_PUBLIC_URL}")
        except Exception as e:
            notify_admin(f"Partial end notify error: {e}")

        # 3) общий старт
        if (not prev_any) and st["any_live"]:
            st["started_at"] = now_utc().isoformat()
            try:
                main_id = tg_send(build_main_text(st))
                st["main_message_id"] = main_id
            except Exception as e:
                notify_admin(f"Start message send error: {e}")

        # 4) обновление главного поста
        if st["any_live"] and st.get("main_message_id"):
            new_text = build_main_text(st)
            if new_text != st.get("_last_main_text"):
                try:
                    tg_edit(int(st["main_message_id"]), new_text)
                except Exception as e:
                    # если edit не работает — создадим новое "главное" сообщение
                    notify_admin(f"Edit failed, sending new main message: {e}")
                    try:
                        main_id = tg_send(new_text)
                        st["main_message_id"] = main_id
                    except Exception as e2:
                        notify_admin(f"New main message send failed: {e2}")
                st["_last_main_text"] = new_text

        # 5) общий конец
        if prev_any and (not st["any_live"]):
            try:
                tg_send(build_end_text(st))
            except Exception as e:
                notify_admin(f"End message send error: {e}")

            st["started_at"] = None
            st["main_message_id"] = None
            st["_last_main_text"] = None

        save_state(st)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
