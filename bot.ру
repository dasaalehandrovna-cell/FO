# 🧭 Description: Code_022.2
#  • Full finance UI: окно дня, навигация, edit-меню
#  • Per-chat независимые файлы: data_<chat>.json, .csv, meta
#  • Backup: Google Drive + Backup Channel
#  • Forwarding: текст + медиа, анонимно, только OWNER настраивает
#  • known_chats → автоматическое сохранение в data_<OWNER>.json
#  • Быстрые отклики, оптимизированное сохранение
# ==========================================================

# ========== SECTION 1 — IMPORTS ==========
import os
import io
import json
import csv
import re
import html
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
import telebot
from telebot import types
from flask import Flask, request

# --- Google Drive ---
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.discovery import build
from google.oauth2 import service_account


# ========== SECTION 2 — ENVIRONMENT & GLOBALS ==========

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = os.getenv("OWNER_ID", "").strip()
BACKUP_CHAT_ID = os.getenv("BACKUP_CHAT_ID", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "").strip()
APP_URL = os.getenv("APP_URL", "").strip()
PORT = int(os.getenv("PORT", "8443"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

VERSION = "Code_022.3"

DEFAULT_TZ = "America/Argentina/Buenos_Aires"
KEEP_ALIVE_INTERVAL_SECONDS = 60

DATA_FILE = "data.json"
CSV_FILE = "data.csv"
CSV_META_FILE = "csv_meta.json"

# backup flags
backup_flags = {
    "drive": True,
    "channel": True,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)
app = Flask(__name__)

# main runtime storage
data = {}

# финансовый режим активных чатов
finance_active_chats = set()


# ==========================================================
# SECTION 3 — TIME / LOG HELPERS
# ==========================================================

def log_info(msg: str):
    logger.info(msg)

def log_error(msg: str):
    logger.error(msg)

def get_tz():
    try:
        return ZoneInfo(DEFAULT_TZ)
    except Exception:
        return timezone(timedelta(hours=-3))

def now_local():
    return datetime.now(get_tz())

def today_key() -> str:
    return now_local().strftime("%Y-%m-%d")


# ==========================================================
# SECTION 4 — JSON / CSV HELPERS
# ==========================================================

def _load_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return default

def _save_json(path: str, obj):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log_error(f"JSON save error {path}: {e}")


def _load_csv_meta():
    return _load_json(CSV_META_FILE, {})

def _save_csv_meta(meta: dict):
    try:
        _save_json(CSV_META_FILE, meta)
    except Exception as e:
        log_error(f"_save_csv_meta: {e}")


def default_data():
    return {
        "overall_balance": 0,
        "records": [],
        "chats": {},
        "active_messages": {},
        "next_id": 1,
        "backup_flags": {"drive": True, "channel": True},
        "finance_active_chats": {},
        "forward_rules": {},
        # Новый блок: данные всех известных чатов
        "known_chats": {},
    }


def load_data():
    d = _load_json(DATA_FILE, default_data())

    base = default_data()
    for k, v in base.items():
        if k not in d:
            d[k] = v

    # восстановление флагов
    flags = d.get("backup_flags") or {}
    backup_flags["drive"] = bool(flags.get("drive", True))
    backup_flags["channel"] = bool(flags.get("channel", True))

    # фин.режим
    fac = d.get("finance_active_chats") or {}
    finance_active_chats.clear()
    for cid, enabled in fac.items():
        if enabled:
            try:
                finance_active_chats.add(int(cid))
            except:
                pass

    return d


def save_data(d):
    fac = {}
    for cid in finance_active_chats:
        fac[str(cid)] = True

    d["finance_active_chats"] = fac
    d["backup_flags"] = {
        "drive": bool(backup_flags.get("drive", True)),
        "channel": bool(backup_flags.get("channel", True)),
    }

    _save_json(DATA_FILE, d)


# ==========================================================
# SECTION 5 — PER-CHAT STORAGE
# ==========================================================

def chat_json_file(chat_id: int) -> str:
    return f"data_{chat_id}.json"

def chat_csv_file(chat_id: int) -> str:
    return f"data_{chat_id}.csv"

def chat_meta_file(chat_id: int) -> str:
    return f"csv_meta_{chat_id}.json"


def get_chat_store(chat_id: int) -> dict:
    chats = data.setdefault("chats", {})
    return chats.setdefault(
        str(chat_id),
        {
            "info": {},
            "balance": 0,
            "records": [],
            "daily_records": {},
            "next_id": 1,
            "active_windows": {},
            "edit_wait": None,
            "edit_target": None,
            "current_view_day": today_key(),
        }
    )

# ==========================================================
# SECTION 6 — NUMBER PARSING & FORMATTING
# ==========================================================

def fmt_num(x: int) -> str:
    return f"{x:,}".replace(",", " ")

# Умный парсер чисел: +500, - 1.200, "1 200,50", "1.200,50"

# Универсальный парсер сумм как в Code_022:
#   • поддерживает + / - / разные разделители
#   • БЕЗ знака — расход (отрицательно)
num_re = re.compile(r'[+\-]?\s*\d(?:[\d\s\._\'",]*\d)?')

def parse_amount_token(token: str) -> float:
    """
    Преобразует строку вида:
      "1.200", "1 200", "1,200", "1.200,50", "+1 000", "-2.500,75", "1_000", "1'234"
    в число с двумя десятичными (float).
    Правила знаков:
      • без знака → ОТРИЦАТЕЛЬНО (расход)
      • '-' → отрицательно
      • '+' → положительно
    """
    if not token:
        return 0.0
    t = token.strip()

    # нормализуем возможные "длинные" минусы
    for uni_minus in ("−", "–", "—"):
        t = t.replace(uni_minus, "-")

    sign = -1
    m = re.match(r"^([+\-])\s*", t)
    if m:
        sign = 1 if m.group(1) == "+" else -1
        t = t[m.end():]

    # убираем пробелы и "декоративные" разделители
    t = re.sub(r"[ _\'\"]+", "", t)
    if not t:
        return 0.0

    # оставляем только цифры и разделители
    t = re.sub(r"[^0-9\.,]", "", t)
    if not t:
        return 0.0

    has_dot = "." in t
    has_comma = "," in t

    if has_dot and has_comma:
        # определяем, какой разделитель ближе к концу — он десятичный
        if t.rfind(",") > t.rfind("."):
            t = t.replace(".", "").replace(",", ".")
        else:
            t = t.replace(",", "")
    elif has_comma and not has_dot:
        # только запятая → считаем её десятичной
        t = t.replace(".", "").replace(",", ".")
    else:
        # только точка или вообще нет разделителей
        t = t.replace(",", "")

    try:
        value = float(t)
        return round(sign * value, 2)
    except Exception:
        return 0.0


def parse_amount(text: str) -> int:
    """
    Парсер сумм на основе Code_022.
    Ищет первое число в строке и возвращает целое значение (ARS),
    округляя до ближайшего целого.
    """
    s = (text or "").strip()
    m = num_re.search(s)
    if not m:
        raise ValueError("no number found")

    token = m.group(0)
    value = parse_amount_token(token)
    return int(round(value))


# ==========================================================
# SECTION 7 — GOOGLE DRIVE
# ==========================================================

def _get_drive_service():
    if not GOOGLE_SERVICE_ACCOUNT_JSON or not GDRIVE_FOLDER_ID:
        return None
    try:
        info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        creds = service_account.Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/drive"]
        )
        return build("drive", "v3", credentials=creds)
    except Exception as e:
        log_error(f"Drive service error: {e}")
        return None


def upload_to_gdrive(path: str, mime_type: str = None, description: str | None = None):
    if not backup_flags.get("drive", True):
        log_info("GDrive backup disabled.")
        return

    service = _get_drive_service()
    if service is None:
        return

    if not os.path.exists(path):
        log_error(f"upload_to_gdrive: file not found: {path}")
        return

    fname = os.path.basename(path)
    file_metadata = {
        "name": fname,
        "parents": [GDRIVE_FOLDER_ID],
        "description": description or "",
    }
    media = MediaFileUpload(path, mimetype=mime_type, resumable=True)

    try:
        existing = service.files().list(
            q=f"name = '{fname}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false",
            spaces="drive",
            fields="files(id, name)"
        ).execute()
        items = existing.get("files", [])
        if items:
            file_id = items[0]["id"]
            service.files().update(
                fileId=file_id,
                media_body=media,
                body={"description": description or ""},
            ).execute()
            log_info(f"GDrive updated: {fname}")
        else:
            created = service.files().create(
                body=file_metadata,
                media_body=media,
                fields="id"
            ).execute()
            log_info(f"GDrive created: {fname}")
    except Exception as e:
        log_error(f"upload_to_gdrive({path}): {e}")


def download_from_gdrive(filename: str, dest_path: str) -> bool:
    service = _get_drive_service()
    if service is None:
        return False
    try:
        res = service.files().list(
            q=f"name = '{filename}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false",
            spaces="drive",
            fields="files(id, name, mimeType, size)"
        ).execute()
        items = res.get("files", [])
        if not items:
            log_info(f"GDrive: {filename} not found")
            return False

        file_id = items[0]["id"]
        request = service.files().get_media(fileId=file_id)
        fh = io.FileIO(dest_path, "wb")
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

        log_info(f"GDrive downloaded {filename}")
        return True
    except Exception as e:
        log_error(f"download_from_gdrive({filename}): {e}")
        return False


def restore_from_gdrive_if_needed() -> bool:
    """
    Выполняется только при старте.
    Если локальные файлы отсутствуют — подтягиваем их из Google Drive.
    """
    restored_any = False

    if not os.path.exists(DATA_FILE):
        if download_from_gdrive(DATA_FILE, DATA_FILE):
            restored_any = True

    if not os.path.exists(CSV_FILE):
        if download_from_gdrive(CSV_FILE, CSV_FILE):
            restored_any = True

    if not os.path.exists(CSV_META_FILE):
        if download_from_gdrive(CSV_META_FILE, CSV_META_FILE):
            restored_any = True

    if restored_any:
        log_info("Restored data from Google Drive")
    return restored_any


# ==========================================================
# SECTION 8 — GLOBAL CSV EXPORT & BACKUP TO CHANNEL
# ==========================================================

def export_global_csv(d: dict):
    """
    Формирует единый CSV-файл по всем чатам для архивного хранения.
    """
    try:
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["chat_id", "ID", "short_id", "timestamp", "amount", "note", "owner", "day_key"])

            for cid, cdata in d.get("chats", {}).items():
                for dk, records in cdata.get("daily_records", {}).items():
                    for r in records:
                        w.writerow([
                            cid,
                            r.get("id"),
                            r.get("short_id"),
                            r.get("timestamp"),
                            r.get("amount"),
                            r.get("note"),
                            r.get("owner"),
                            dk,
                        ])
    except Exception as e:
        log_error(f"export_global_csv error: {e}")


def send_backup_to_channel_for_file(base_path: str, meta_key_prefix: str):
    """
    Отправляет файл в backup-канал, обновляя старую версию через edit_message_media.
    """
    if not BACKUP_CHAT_ID:
        return
    if not os.path.exists(base_path):
        return

    try:
        meta = _load_csv_meta()
        msg_key = f"msg_{meta_key_prefix}"
        ts_key = f"timestamp_{meta_key_prefix}"

        with open(base_path, "rb") as f:
            caption = f"📦 {os.path.basename(base_path)} — {now_local().strftime('%Y-%m-%d %H:%M')}"
            if meta.get(msg_key):
                try:
                    bot.edit_message_media(
                        chat_id=int(BACKUP_CHAT_ID),
                        message_id=meta[msg_key],
                        media=telebot.types.InputMediaDocument(f, caption=caption)
                    )
                except Exception:
                    sent = bot.send_document(int(BACKUP_CHAT_ID), f, caption=caption)
                    meta[msg_key] = sent.message_id
            else:
                sent = bot.send_document(int(BACKUP_CHAT_ID), f, caption=caption)
                meta[msg_key] = sent.message_id

        meta[ts_key] = now_local().isoformat(timespec="seconds")
        _save_csv_meta(meta)

    except Exception as e:
        log_error(f"send_backup_to_channel_for_file({base_path}): {e}")

# ==========================================================
# SECTION 9 — FORWARD RULES & OWNER META (forward_rules + known_chats)
# ==========================================================

def _owner_data_file() -> str | None:
    if not OWNER_ID:
        return None
    return f"data_{int(OWNER_ID)}.json"


def persist_owner_meta():
    """
    Сохраняет в data_<OWNER_ID>.json:
      • forward_rules (правила пересылки)
      • known_chats (список чатов с title/username/type)
    При этом НЕ трогаются финансовые данные владельца.
    """
    try:
        path = _owner_data_file()
        if not path:
            return

        payload = {}
        if os.path.exists(path):
            payload = _load_json(path, {})
            if not isinstance(payload, dict):
                payload = {}

        # добавляем forward_rules
        fr = data.get("forward_rules", {}) or {}
        if fr:
            payload["forward_rules"] = fr
        else:
            payload.pop("forward_rules", None)

        # добавляем known_chats
        kc = data.get("known_chats", {}) or {}
        if kc:
            payload["known_chats"] = kc
        else:
            payload.pop("known_chats", None)

        _save_json(path, payload)
        log_info(f"Owner meta persisted → {path}")

    except Exception as e:
        log_error(f"persist_owner_meta: {e}")


def persist_forward_rules_to_owner():
    """
    Старое имя функции. Теперь просто вызывает persist_owner_meta().
    """
    persist_owner_meta()


def register_known_chat_from_chat(chat):
    """
    Регистрирует чат в глобальном data["known_chats"].
    Сохраняет:
      id, title, username, type
    Автоматически обновляет data_<OWNER_ID>.json.
    """
    try:
        if not OWNER_ID:
            return

        chat_id = chat.id
        if str(chat_id) == str(OWNER_ID):
            # чат владельца не сохраняем как "известный"
            return

        kc = data.setdefault("known_chats", {})
        cid = str(chat_id)

        info = kc.get(cid, {})
        info["id"] = chat_id
        info["type"] = getattr(chat, "type", "")

        title = getattr(chat, "title", None)
        username = getattr(chat, "username", None)

        if title:
            info["title"] = title
        if username:
            info["username"] = username

        kc[cid] = info
        save_data(data)
        persist_owner_meta()

    except Exception as e:
        log_error(f"register_known_chat_from_chat: {e}")


# ==========================================================
# SECTION 10 — FORWARD RULES LOGIC
# ==========================================================

def resolve_forward_targets(source_chat_id: int) -> list[int]:
    """
    Возвращает список чатов, куда нужно переслать сообщение.
    Все ID приводятся к int.
    """
    fr = data.get("forward_rules", {}) or {}
    arr = fr.get(str(source_chat_id), [])
    result = []
    for x in arr:
        try:
            result.append(int(x))
        except:
            pass
    return result


def add_forward_link(src_chat_id: int, dst_chat_id: int):
    """Добавляет одностороннюю пересылку: src → dst."""
    fr = data.setdefault("forward_rules", {})
    arr = fr.setdefault(str(src_chat_id), [])
    if str(dst_chat_id) not in arr:
        arr.append(str(dst_chat_id))
    save_data(data)
    persist_owner_meta()


def remove_forward_link(src_chat_id: int, dst_chat_id: int):
    """Удаляет одностороннюю пересылку src → dst."""
    fr = data.get("forward_rules", {}) or {}
    arr = fr.get(str(src_chat_id), [])
    if str(dst_chat_id) in arr:
        arr.remove(str(dst_chat_id))
    save_data(data)
    persist_owner_meta()


def clear_forward_all():
    """Удаляет ВСЕ правила пересылки."""
    data["forward_rules"] = {}
    save_data(data)
    persist_owner_meta()


# ==========================================================
# SECTION 11 — RENDER DAY WINDOW
# ==========================================================

def render_day_window(chat_id: int, day_key: str):
    """
    Формирует окно дня: список записей + сумма.
    """
    store = get_chat_store(chat_id)
    day_recs = store.get("daily_records", {}).get(day_key, [])

    lines = [f"📅 <b>{day_key}</b>"]
    total = 0

    for r in day_recs:
        amt = r["amount"]
        total += amt
        sign = "➕" if amt >= 0 else "➖"
        note = html.escape(r.get("note", ""))
        lines.append(f"{sign} {fmt_num(amt)} — {note}")

    lines.append("")
    lines.append(f"💰 Итого за день: {fmt_num(total)}")

    return "\n".join(lines), total


# ==========================================================
# SECTION 12 — KEYBOARDS: MAIN / CALENDAR / EDIT MENU
# ==========================================================


def build_main_keyboard(day_key: str, chat_id=None):
    """
    Главная клавиатура окна дня (без кнопки "Добавить").
    Добавление записей происходит напрямую из текстовых сообщений.
    """
    kb = types.InlineKeyboardMarkup(row_width=2)

    kb.row(
        types.InlineKeyboardButton("📝 Редактировать", callback_data=f"d:{day_key}:edit_menu"),
        types.InlineKeyboardButton("💰 Общий итог", callback_data=f"d:{day_key}:total")
    )

    kb.row(
        types.InlineKeyboardButton("⬅️ Вчера", callback_data=f"d:{day_key}:prev"),
        types.InlineKeyboardButton("➡️ Завтра", callback_data=f"d:{day_key}:next")
    )

    kb.row(
        types.InlineKeyboardButton("📅 Календарь", callback_data=f"d:{day_key}:calendar"),
        types.InlineKeyboardButton("📊 Отчёт", callback_data=f"d:{day_key}:report")
    )

    kb.row(
        types.InlineKeyboardButton("ℹ️ Инфо", callback_data=f"d:{day_key}:info")
    )
    return kb



def build_calendar_keyboard(center_day: datetime, chat_id: int):
    """
    Календарь на 31 день.
    Дни, в которых есть транзакции, помечаем точкой • перед датой.
    """
    kb = types.InlineKeyboardMarkup(row_width=4)

    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {})

    start_day = center_day - timedelta(days=15)
    for week in range(0, 32, 4):
        row = []
        for d in range(4):
            day = start_day + timedelta(days=week + d)
            label = day.strftime("%d.%m")
            day_key = day.strftime("%Y-%m-%d")
            # пометка дней с операциями
            if day_key in daily and daily[day_key]:
                label = "•" + label
            row.append(types.InlineKeyboardButton(label, callback_data=f"d:{day_key}:open"))
        kb.row(*row)

    kb.row(
        types.InlineKeyboardButton("⬅️ −31", callback_data=f"c:{(center_day - timedelta(days=31)).strftime('%Y-%m-%d')}"),
        types.InlineKeyboardButton("➡️ +31", callback_data=f"c:{(center_day + timedelta(days=31)).strftime('%Y-%m-%d')}")
    )

    kb.row(types.InlineKeyboardButton("📅 Сегодня", callback_data=f"d:{today_key()}:open"))
    return kb



def build_edit_menu_keyboard(day_key: str, chat_id=None):
    kb = types.InlineKeyboardMarkup(row_width=2)

    kb.row(
        types.InlineKeyboardButton("📝 Редактировать запись", callback_data=f"d:{day_key}:edit_list"),
        types.InlineKeyboardButton("📂 Общий CSV", callback_data=f"d:{day_key}:csv_all")
    )

    kb.row(
        types.InlineKeyboardButton("📅 CSV за день", callback_data=f"d:{day_key}:csv_day"),
        types.InlineKeyboardButton("⚙️ Обнулить", callback_data=f"d:{day_key}:reset")
    )

    # Меню пересылки — ТОЛЬКО OWNER
    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        kb.row(
            types.InlineKeyboardButton("🔁 Пересылка", callback_data="fw:menu")
        )

    kb.row(
        types.InlineKeyboardButton("📅 Сегодня", callback_data=f"d:{today_key()}:open"),
        types.InlineKeyboardButton("📆 Выбрать день", callback_data=f"d:{day_key}:pick_date")
    )

    kb.row(
        types.InlineKeyboardButton("ℹ️ Инфо", callback_data=f"d:{day_key}:info"),
        types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{day_key}:back_main")
    )
    return kb

# ==========================================================
# SECTION 13 — ADD / UPDATE / DELETE RECORDS
# ==========================================================

def add_record_to_chat(chat_id: int, amount: int, note: str, owner):
    """
    Добавляет финансовую запись в чат.
    Обновляет:
        • records
        • daily_records
        • баланс
        • общий CSV
        • per-chat JSON/CSV
        • делает бэкап
    """
    store = get_chat_store(chat_id)

    rid = store.get("next_id", 1)
    rec = {
        "id": rid,
        "short_id": f"R{rid}",
        "timestamp": now_local().isoformat(timespec="seconds"),
        "amount": amount,
        "note": note,
        "owner": owner,
    }

    # Глобальный список (для общего CSV)
    data.setdefault("records", []).append(rec)

    # Per-chat списки
    store.setdefault("records", []).append(rec)
    store.setdefault("daily_records", {}).setdefault(today_key(), []).append(rec)

    store["balance"] = store.get("balance", 0) + amount
    data["overall_balance"] = data.get("overall_balance", 0) + amount
    store["next_id"] = rid + 1

    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)
    save_chat_json(chat_id)

    send_backup_to_channel(chat_id)


def update_record_in_chat(chat_id: int, rid: int, new_amount: int, new_note: str, user=None):
    """
    Обновляет существующую запись.
    Пересчитывает баланс, записывает в JSON/CSV, делает бэкап.
    """
    store = get_chat_store(chat_id)
    found = None

    for r in store.get("records", []):
        if r["id"] == rid:
            r["amount"] = new_amount
            r["note"] = new_note
            found = r
            break

    if not found:
        return

    # Обновляем запись в daily_records
    for day_recs in store.get("daily_records", {}).values():
        for r in day_recs:
            if r["id"] == rid:
                r.update(found)

    # Пересчёт баланса по чату
    store["balance"] = sum(x["amount"] for x in store.get("records", []))

    # Обновляем глобальный список
    data["records"] = [
        x if x["id"] != rid else found
        for x in data.get("records", [])
    ]
    data["overall_balance"] = sum(x["amount"] for x in data.get("records", []))

    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)
    save_chat_json(chat_id)

    send_backup_to_channel(chat_id)


def delete_record_in_chat(chat_id: int, rid: int, user=None):
    """
    Удаляет запись и пересчитывает баланс и CSV.
    """
    store = get_chat_store(chat_id)

    before = len(store.get("records", []))
    store["records"] = [r for r in store.get("records", []) if r["id"] != rid]

    # Чистим daily_records
    for dk, arr in list(store.get("daily_records", {}).items()):
        new_arr = [r for r in arr if r["id"] != rid]
        if new_arr:
            store["daily_records"][dk] = new_arr
        else:
            del store["daily_records"][dk]

    after = len(store.get("records", []))
    if before == after:
        return

    store["balance"] = sum(x["amount"] for x in store.get("records", []))

    data["records"] = [x for x in data.get("records", []) if x["id"] != rid]
    data["overall_balance"] = sum(x["amount"] for x in data.get("records", []))

    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)
    save_chat_json(chat_id)

    send_backup_to_channel(chat_id)


def reset_chat_data(chat_id: int):
    """
    Полное обнуление данных одного чата (JSON/CSV).
    """
    chats = data.setdefault("chats", {})
    if str(chat_id) in chats:
        del chats[str(chat_id)]

    # удаляем файлы
    for p in (chat_json_file(chat_id), chat_csv_file(chat_id), chat_meta_file(chat_id)):
        try:
            os.remove(p)
        except FileNotFoundError:
            pass

    save_data(data)
    export_global_csv(data)
    send_backup_to_channel(chat_id)


# ==========================================================
# SECTION 14 — ACTIVE WINDOWS (Окно дня)
# ==========================================================

def get_or_create_active_windows(chat_id: int) -> dict:
    return data.setdefault("active_messages", {}).setdefault(str(chat_id), {})

def set_active_window_id(chat_id: int, day_key: str, message_id: int):
    active = get_or_create_active_windows(chat_id)
    active[day_key] = message_id
    save_data(data)

def get_active_window_id(chat_id: int, day_key: str):
    return get_or_create_active_windows(chat_id).get(day_key)


# ==========================================================
# SECTION 15 — FINANCE MODE (режим финансов)
# ==========================================================

def is_finance_mode(chat_id: int) -> bool:
    return chat_id in finance_active_chats

def set_finance_mode(chat_id: int, enabled: bool):
    if enabled:
        finance_active_chats.add(chat_id)
    else:
        finance_active_chats.discard(chat_id)

def require_finance(chat_id: int) -> bool:
    """
    Проверка: включён ли финансовый режим.
    Если нет — показываем подсказку /поехали.
    """
    if not is_finance_mode(chat_id):
        send_info(chat_id, "⚙️ Финансовый режим выключен.\nАктивируйте /поехали")
        return False
    return True


# ==========================================================
# SECTION 16 — CALLBACK HANDLER
# ==========================================================

@bot.callback_query_handler(func=lambda c: True)
def on_callback(call):
    """
    Обработчик ВСЕХ callback_data.
    Формат:
        d:<day_key>:<cmd>
        c:<center_day>
    """
    try:
        data_str = call.data or ""
        chat_id = call.message.chat.id

        # регистрируем чат (known_chats)
        register_known_chat_from_chat(call.message.chat)

        # --------- 1) Календарь: c:<date> ---------
        if data_str.startswith("c:"):
            center = data_str[2:]
            try:
                center_dt = datetime.strptime(center, "%Y-%m-%d")
            except:
                return

            kb = build_calendar_keyboard(center_dt, chat_id)
            try:
                bot.edit_message_reply_markup(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
            except:
                pass
            return

        # ---------- 2) d:<day_key>:<cmd> ----------
        if not data_str.startswith("d:"):
            return

        _, day_key, cmd = data_str.split(":", 2)

        store = get_chat_store(chat_id)

        # ============ Навигация ============

        if cmd == "open":
            txt, _ = render_day_window(chat_id, day_key)
            kb = build_main_keyboard(day_key, chat_id)
            bot.edit_message_text(
                txt, chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb, parse_mode="HTML"
            )
            set_active_window_id(chat_id, day_key, call.message.message_id)
            return

        if cmd == "prev":
            d = datetime.strptime(day_key, "%Y-%m-%d") - timedelta(days=1)
            new_day = d.strftime("%Y-%m-%d")
            txt, _ = render_day_window(chat_id, new_day)
            kb = build_main_keyboard(new_day, chat_id)
            bot.edit_message_text(
                txt, chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb, parse_mode="HTML"
            )
            set_active_window_id(chat_id, new_day, call.message.message_id)
            return

        if cmd == "next":
            d = datetime.strptime(day_key, "%Y-%m-%d") + timedelta(days=1)
            new_day = d.strftime("%Y-%m-%d")
            txt, _ = render_day_window(chat_id, new_day)
            kb = build_main_keyboard(new_day, chat_id)
            bot.edit_message_text(
                txt, chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb, parse_mode="HTML"
            )
            set_active_window_id(chat_id, new_day, call.message.message_id)
            return


        if cmd == "calendar":
            try:
                center = datetime.strptime(day_key, "%Y-%m-%d")
            except:
                center = now_local()
            kb = build_calendar_keyboard(center, chat_id)
            bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb
            )
            return

        # ============ Отчёты / Инфо / Общий итог ============

        if cmd == "report":
            if not require_finance(chat_id):
                return
            store = get_chat_store(chat_id)

            total = sum(
                sum(r["amount"] for r in recs)
                for recs in store.get("daily_records", {}).values()
            )
            msg = (
                f"📊 <b>Отчёт по чату</b>\n\n"
                f"💬 Чат: <code>{chat_id}</code>\n"
                f"💰 Баланс: {fmt_num(store.get('balance', 0))}\n"
                f"📦 Записей: {len(store.get('records', []))}\n"
                f"🔢 Дней: {len(store.get('daily_records', {}))}\n"
                f"🟰 Сумма всех операций: {fmt_num(total)}"
            )
            bot.answer_callback_query(call.id)
            bot.send_message(chat_id, msg, parse_mode="HTML")
            return

        if cmd == "info":
            # Расширенная справка в стиле Code_022.1
            info_text = (
                f"ℹ️ Финансовый бот — версия {VERSION}\n\n"
                "Команды:\n"
                "/поехали — включить финансовый режим в чате\n"
                "/start — открыть окно дня\n"
                "/reset — обнулить данные чата (через подтверждение)\n"
                "/total — показать общий итог по чату\n"
                "/info — эта справка\n"
                "\n"
                "Кнопки:\n"
                "📝 Редактировать — открыть меню редактирования и экспорта\n"
                "📊 Отчёт — краткий отчёт по чату\n"
                "💰 Общий итог — текущий баланс по чату\n"
                "📅 Календарь — быстрый переход по датам (точкой помечены дни с операциями)\n"
                "🔁 Пересылка — настройка анонимной пересылки между чатами (только в чате владельца)\n"
            )
            bot.answer_callback_query(call.id)
            bot.send_message(chat_id, info_text)
            return

        if cmd == "total":
            if not require_finance(chat_id):
                return
            store = get_chat_store(chat_id)
            total = store.get("balance", 0)
            bot.answer_callback_query(call.id)
            bot.send_message(chat_id, f"💰 Общий итог по чату: {fmt_num(total)}")
            return

        # ============ Edit Menu ============

        if cmd == "edit_menu":
            if not require_finance(chat_id):
                return
            kb = build_edit_menu_keyboard(day_key, chat_id)
            try:
                bot.edit_message_reply_markup(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
            except:
                pass
            return

        if cmd == "back_main":
            txt, _ = render_day_window(chat_id, day_key)
            kb = build_main_keyboard(day_key, chat_id)
            bot.edit_message_text(
                txt, chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb, parse_mode="HTML"
            )
            return

        # ============ Добавить запись ============

        if cmd == "add":
            if not require_finance(chat_id):
                return
            store["edit_wait"] = {"type": "add", "day_key": day_key}
            save_data(data)
            bot.answer_callback_query(call.id)
            bot.send_message(chat_id,
                             "Введите запись:\n<b>+500 Обед</b>",
                             parse_mode="HTML")
            return

        # ============ Редактирование списка ============

        if cmd == "edit_list":
            if not require_finance(chat_id):
                return
            store = get_chat_store(chat_id)
            day_recs = store.get("daily_records", {}).get(day_key, [])

            # запоминаем текущее окно дня как активное
            try:
                set_active_window_id(chat_id, day_key, call.message.message_id)
            except Exception:
                pass

            kb = types.InlineKeyboardMarkup(row_width=1)
            for r in day_recs:
                label = f"{r['short_id']} | {fmt_num(r['amount'])} | {r['note']}"
                kb.row(types.InlineKeyboardButton(label, callback_data=f"e:{day_key}:{r['id']}"))

            kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{day_key}:edit_menu"))
            bot.answer_callback_query(call.id)
            bot.send_message(chat_id, "Выберите запись для редактирования:", reply_markup=kb)
            return

        # ============ Выбор конкретной записи для редактирования ============

        if cmd.startswith("edit_"):
            # не используется — оставлено на будущее расширение
            return

        # ============ Выбор конкретной записи ("e:day:rid") ============

        if data_str.startswith("e:"):
            try:
                _, dkey, rid_str = data_str.split(":", 2)
                rid = int(rid_str)
            except:
                return

            if not require_finance(chat_id):
                return

            store["edit_wait"] = {"type": "edit", "day_key": dkey, "rid": rid}
            save_data(data)

            bot.answer_callback_query(call.id)
            bot.send_message(chat_id,
                             "Введите новое значение:\n<b>+500 Новая_заметка</b>",
                             parse_mode="HTML")
            return

        # ============ CSV по дню ============

        if cmd == "csv_day":
            try:
                fname = f"csv_day_{chat_id}_{day_key}.csv"
                with open(fname, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow(["ID", "time", "amount", "note"])
                    store = get_chat_store(chat_id)
                    for r in store.get("daily_records", {}).get(day_key, []):
                        w.writerow([
                            r["id"],
                            r["timestamp"],
                            r["amount"],
                            r["note"],
                        ])
                with open(fname, "rb") as f:
                    bot.answer_callback_query(call.id)
                    bot.send_document(chat_id, f, caption=f"CSV за {day_key}")
            except Exception as e:
                bot.send_message(chat_id, f"Ошибка CSV: {e}")
            return

        # ============ Общий CSV (global) ============

        if cmd == "csv_all":
            try:
                export_global_csv(data)
                with open(CSV_FILE, "rb") as f:
                    bot.answer_callback_query(call.id)
                    bot.send_document(chat_id, f, caption="Общий CSV")
            except Exception as e:
                bot.send_message(chat_id, f"Ошибка CSV: {e}")
            return

        # ============ Удаление / Reset ============

        if cmd == "reset":
            if not require_finance(chat_id):
                return
            store["edit_wait"] = {"type": "reset", "day_key": day_key}
            save_data(data)

            kb = types.InlineKeyboardMarkup()
            kb.row(types.InlineKeyboardButton("Да, удалить все", callback_data=f"d:{day_key}:reset_yes"))
            kb.row(types.InlineKeyboardButton("Отмена", callback_data=f"d:{day_key}:edit_menu"))
            bot.answer_callback_query(call.id)
            bot.send_message(chat_id, "Вы уверены, что хотите обнулить все данные чата?", reply_markup=kb)
            return

        if cmd == "reset_yes":
            if not require_finance(chat_id):
                return
            reset_chat_data(chat_id)
            bot.answer_callback_query(call.id)
            bot.send_message(chat_id, "Данные очищены.")
            return

        # ============ Выбор даты ============

        if cmd == "pick_date":
            bot.answer_callback_query(call.id)
            bot.send_message(chat_id, "Введите дату в формате YYYY-MM-DD:")
            store["edit_wait"] = {"type": "pick_date"}
            save_data(data)
            return

    except Exception as e:
        log_error(f"on_callback: {e}")

# ==========================================================
# SECTION 17 — TEXT HANDLER (текстовые сообщения)
# ==========================================================


@bot.message_handler(content_types=["text"], func=lambda m: not (m.text or "").startswith("/"))
def handle_text(msg):
    """
    Обработка текстов:
    • регистрируем чат в known_chats
    • пересылка текста (анонимно)
    • добавление записи напрямую из сообщения
    • редактирование записи
    • ввод даты для /pick_date
    • подтверждение /reset
    """
    try:
        chat_id = msg.chat.id
        text = (msg.text or "").strip()

        # 1) Регистрируем чат
        register_known_chat_from_chat(msg.chat)

        store = get_chat_store(chat_id)

        # ---------- 2) Пересылка текста между чатами ----------
        targets = resolve_forward_targets(chat_id)
        if targets:
            for dst in targets:
                if dst == chat_id:
                    continue
                try:
                    bot.send_message(dst, text)
                except Exception as e:
                    log_error(f"handle_text: forward error: {e}")

        # ---------- 3) Обработка ожидаемого состояния ----------
        wait = store.get("edit_wait")

        # ---- 3A) Редактирование записи ----
        if wait and wait.get("type") == "edit":
            if not require_finance(chat_id):
                store["edit_wait"] = None
                save_data(data)
                return

            try:
                parts = text.split(" ", 1)
                amount = parse_amount(parts[0])
                note = parts[1] if len(parts) > 1 else ""
            except Exception:
                bot.send_message(chat_id, "❌ Ошибка формата. Пример: +500 Такси")
                return

            rid = wait["rid"]
            day_key = wait["day_key"]
            update_record_in_chat(chat_id, rid, amount, note, msg.from_user.id)

            store["edit_wait"] = None
            save_data(data)

            # обновляем текущее окно дня, если оно известно
            try:
                active_id = get_active_window_id(chat_id, day_key)
            except Exception:
                active_id = None

            txt, _ = render_day_window(chat_id, day_key)
            kb = build_main_keyboard(day_key, chat_id)

            if active_id:
                try:
                    bot.edit_message_text(txt, chat_id, active_id, reply_markup=kb, parse_mode="HTML")
                except Exception:
                    bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
            else:
                bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
            return

        # ---- 3B) Ввод даты (pick_date) ----
        if wait and wait.get("type") == "pick_date":
            try:
                dt = datetime.strptime(text, "%Y-%m-%d")
            except:
                bot.send_message(chat_id, "❌ Формат неверный. Пример: 2025-11-14")
                return

            day_key = dt.strftime("%Y-%m-%d")
            store["edit_wait"] = None
            save_data(data)

            txt, _ = render_day_window(chat_id, day_key)
            kb = build_main_keyboard(day_key, chat_id)

            # открываем новое окно дня
            sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
            try:
                set_active_window_id(chat_id, day_key, sent.message_id)
            except Exception:
                pass
            return

        # ---- 3C) Подтверждение /reset ----
        if text.upper() == "ДА":
            if not require_finance(chat_id):
                return
            reset_chat_data(chat_id)
            send_info(chat_id, "Данные чата обнулены.")
            return

        # ---- 4) Команды /поехали и /start обрабатываются отдельными хендлерами ----
        if text.startswith("/"):
            # отдаём обработку декораторам команд
            return

        # ---- 5) Добавление записи напрямую из сообщения ----
        if not require_finance(chat_id):
            return

        try:
            parts = text.split(" ", 1)
            amount = parse_amount(parts[0])
            note = parts[1] if len(parts) > 1 else ""
        except Exception:
            bot.send_message(chat_id, "❌ Не удалось распознать сумму. Пример: +500 Обед")
            return

        day_key = today_key()
        add_record_to_chat(chat_id, amount, note, msg.from_user.id)

        # обновляем или создаём окно дня
        try:
            active_id = get_active_window_id(chat_id, day_key)
        except Exception:
            active_id = None

        txt, _ = render_day_window(chat_id, day_key)
        kb = build_main_keyboard(day_key, chat_id)

        if active_id:
            try:
                bot.edit_message_text(txt, chat_id, active_id, reply_markup=kb, parse_mode="HTML")
            except Exception:
                sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
                try:
                    set_active_window_id(chat_id, day_key, sent.message_id)
                except Exception:
                    pass
        else:
            sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
            try:
                set_active_window_id(chat_id, day_key, sent.message_id)
            except Exception:
                pass

    except Exception as e:
        log_error(f"handle_text: {e}")


# ==========================================================
# SECTION 18 — MEDIA HANDLER 
 
# ==========================================================

@bot.message_handler(content_types=[
    "photo", "document", "audio", "voice",
    "video", "video_note", "sticker",
    "location", "contact"
])
def handle_media(msg):
    """
    Обработка медиа:
    • регистрация known_chats
    • анонимная пересылка copy_message
    • без участия финансовой логики
    """
    try:
        chat_id = msg.chat.id

        # регистрируем чат
        register_known_chat_from_chat(msg.chat)

        # пересылка
        targets = resolve_forward_targets(chat_id)
        if targets:
            for dst in targets:
                if dst == chat_id:
                    continue
                try:
                    bot.copy_message(dst, chat_id, msg.message_id)
                except Exception as e:
                    log_error(f"handle_media: {e}")

    except Exception as e:
        log_error(f"handle_media outer: {e}")

# ==========================================================
# SECTION 19 — SAVE CHAT JSON (перезаписанная версия 022.2)
# ==========================================================

def save_chat_json(chat_id: int):
    """
    Сохраняет:
      • per-chat JSON
      • per-chat CSV
      • per-chat META
    В файл владельца (data_<OWNER_ID>.json) дополнительно добавляет:
      • forward_rules
      • known_chats
    """
    try:
        store = data.get("chats", {}).get(str(chat_id), {})
        if not store:
            return

        chat_path_json = chat_json_file(chat_id)
        chat_path_csv = chat_csv_file(chat_id)
        chat_path_meta = chat_meta_file(chat_id)

        # гарантируем что файлы существуют
        for p in (chat_path_json, chat_path_csv, chat_path_meta):
            if not os.path.exists(p):
                with open(p, "a", encoding="utf-8"):
                    pass

        # базовый payload
        payload = {
            "chat_id": chat_id,
            "balance": store.get("balance", 0),
            "records": store.get("records", []),
            "daily_records": store.get("daily_records", {}),
            "next_id": store.get("next_id", 1),
            "info": store.get("info", {}),
        }

        # ДОПОЛНЕНИЕ: если это файл владельца — записываем мета-настройки
        if OWNER_ID and str(chat_id) == str(OWNER_ID):
            fr = data.get("forward_rules", {}) or {}
            kc = data.get("known_chats", {}) or {}
            if fr:
                payload["forward_rules"] = fr
            if kc:
                payload["known_chats"] = kc

        _save_json(chat_path_json, payload)

        # создаём CSV по чату
        with open(chat_path_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["chat_id", "ID", "short_id", "timestamp", "amount", "note", "owner", "day_key"])
            for dk, recs in store.get("daily_records", {}).items():
                for r in recs:
                    w.writerow([
                        chat_id,
                        r.get("id"),
                        r.get("short_id"),
                        r.get("timestamp"),
                        r.get("amount"),
                        r.get("note"),
                        r.get("owner"),
                        dk,
                    ])

        # META
        meta = {
            "last_saved": now_local().isoformat(timespec="seconds"),
            "record_count": sum(len(v) for v in store.get("daily_records", {}).values()),
        }
        _save_json(chat_path_meta, meta)

        log_info(f"Per-chat saved → {chat_id}")

    except Exception as e:
        log_error(f"save_chat_json({chat_id}): {e}")


# ==========================================================
# SECTION 20 — BACKUP TO CHANNEL (патч расширенной логики)
# ==========================================================

def send_backup_to_channel(chat_id: int):
    """
    Выполняет:
      • save_chat_json(chat_id)
      • save_chat_json(owner) → чтобы владелец видел fresh forward_rules + known_chats
      • отправляет JSON и CSV в backup-chat
      • отправляет файл владельца (общий meta-файл)
      • обновляет global CSV
    """
    flags = backup_flags or {}
    if not flags.get("channel", True):
        log_info("Channel backup disabled.")
        return
    if not BACKUP_CHAT_ID:
        log_info("No BACKUP_CHAT_ID set.")
        return

    try:
        # 1. Обновляем файл изменённого чата
        save_chat_json(chat_id)

        # 2. Обновляем файл владельца (там forward_rules+known_chats)
        if OWNER_ID:
            try:
                save_chat_json(int(OWNER_ID))
            except Exception as e:
                log_error(f"send_backup_to_channel owner update: {e}")

        # 3. Отправляем JSON и CSV изменённого чата
        send_backup_to_channel_for_file(chat_json_file(chat_id), f"json_chat_{chat_id}")
        send_backup_to_channel_for_file(chat_csv_file(chat_id), f"csv_chat_{chat_id}")

        # 4. Отправляем единый файл владельца (master-config)
        if OWNER_ID:
            own = int(OWNER_ID)
            send_backup_to_channel_for_file(chat_json_file(own), "json_owner")

        # 5. Обновляем глобальный CSV
        export_global_csv(data)
        send_backup_to_channel_for_file(CSV_FILE, "csv_global")

    except Exception as e:
        log_error(f"send_backup_to_channel({chat_id}): {e}")


# ==========================================================
# SECTION 21 — UTILITIES
# ==========================================================

def send_info(chat_id: int, text: str):
    try:
        bot.send_message(chat_id, text)
    except Exception as e:
        log_error(f"send_info: {e}")

# ==========================================================
# SECTION 22 — FILE UPLOAD HANDLER (OWNER restore)
# ==========================================================

@bot.message_handler(content_types=["document"])
def handle_document(msg):
    """
    Владелец может прислать:
        • data_<chat>.json
        • data.json
    Бот восстановит файл и, если это data.json — перезагрузит глобальные данные.
    """
    try:
        doc = msg.document
        if not doc or not doc.file_name:
            return

        fname = doc.file_name.lower()
        owner_ok = OWNER_ID and msg.chat.id == int(OWNER_ID)

        # Разрешаем владельцу заливать только JSON
        if not owner_ok or not fname.endswith(".json"):
            return

        file_info = bot.get_file(doc.file_id)
        downloaded = bot.download_file(file_info.file_path)

        # Имя файла
        if fname == "data.json":
            local = DATA_FILE
        else:
            local = fname

        # Сохраняем полученный файл
        with open(local, "wb") as f:
            f.write(downloaded)

        # Если это главный data.json — перезагрузить глобальные данные
        if local == DATA_FILE:
            try:
                restored = _load_json(DATA_FILE, default_data())
                if isinstance(restored, dict):
                    global data
                    data = restored
                    bot.send_message(msg.chat.id, "✔️ data.json успешно загружен и применён")
                else:
                    bot.send_message(msg.chat.id, "⚠️ Ошибка: data.json не является объектом JSON")
            except Exception as e:
                bot.send_message(msg.chat.id, f"Ошибка загрузки JSON: {e}")
                return

        else:
            bot.send_message(msg.chat.id, f"✔️ Файл {local} сохранён")
            return

    except Exception as e:
        log_error(f"handle_document: {e}")


# ==========================================================
# SECTION 23 — COMMANDS HANDLER
# ==========================================================

@bot.message_handler(commands=["start"])
def cmd_start(msg):
    """
    Старт: регистрирует чат, включает финансовый режим и сразу открывает окно дня.
    """
    chat_id = msg.chat.id
    register_known_chat_from_chat(msg.chat)

    # Включаем финансовый режим сразу
    set_finance_mode(chat_id, True)
    save_data(data)

    dk = today_key()
    txt, _ = render_day_window(chat_id, dk)
    kb = build_main_keyboard(dk, chat_id)

    bot.send_message(
        chat_id,
        "👋 Бот активен.\nФинансовый режим включён, окно на сегодня открыто."
    )
    bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")



@bot.message_handler(commands=["поехали"])
def cmd_go(msg):
    chat_id = msg.chat.id
    register_known_chat_from_chat(msg.chat)

    set_finance_mode(chat_id, True)
    save_data(data)

    dk = today_key()
    txt, _ = render_day_window(chat_id, dk)
    kb = build_main_keyboard(dk, chat_id)

    bot.send_message(chat_id, "⚙️ Финансовый режим включён")
    bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")

# ==========================================================
# SECTION 24 — /reset, /total, /info (команды)
# ==========================================================

@bot.message_handler(commands=["reset"])
def cmd_reset(msg):
    chat_id = msg.chat.id
    register_known_chat_from_chat(msg.chat)

    if not require_finance(chat_id):
        return

    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton("Да, удалить всё", callback_data=f"d:{today_key()}:reset_yes"))
    kb.row(types.InlineKeyboardButton("Отмена", callback_data=f"d:{today_key()}:back_main"))

    bot.send_message(chat_id, "Вы уверены, что хотите полностью обнулить данные чата?", reply_markup=kb)


@bot.message_handler(commands=["total"])
def cmd_total(msg):
    chat_id = msg.chat.id
    register_known_chat_from_chat(msg.chat)

    if not require_finance(chat_id):
        return

    store = get_chat_store(chat_id)
    total = store.get("balance", 0)

    bot.send_message(chat_id, f"💰 Общий итог по чату: {fmt_num(total)}")


@bot.message_handler(commands=["info"])
def cmd_info(msg):
    chat_id = msg.chat.id
    register_known_chat_from_chat(msg.chat)

    info_msg = (
        f"ℹ️ <b>Информация о боте</b>\n"
        f"Версия: <code>{VERSION}</code>\n"
        f"Финансовый режим: {'включён' if is_finance_mode(chat_id) else 'выключен'}\n"
        f"Часовой пояс: {DEFAULT_TZ}\n"
    )
    bot.send_message(chat_id, info_msg, parse_mode="HTML")

# ==========================================================
# SECTION 25 — FORWARD MENU (ТОЛЬКО ДЛЯ OWNER)
# ==========================================================

def build_forward_menu_keyboard():
    """
    Показывает список всех известных чатов для владельца.
    """
    kc = data.get("known_chats", {})
    kb = types.InlineKeyboardMarkup(row_width=1)

    if not kc:
        kb.row(types.InlineKeyboardButton("Нет известных чатов", callback_data="fw:none"))
        return kb

    for cid, info in kc.items():
        title = info.get("title") or info.get("username") or f"Chat {cid}"
        label = f"{title} (ID {cid})"
        kb.row(types.InlineKeyboardButton(label, callback_data=f"fw:open:{cid}"))

    kb.row(types.InlineKeyboardButton("Очистить все связи", callback_data="fw:clear_all"))
    kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{today_key()}:edit_menu"))

    return kb


def build_forward_direction_keyboard(src, dst):
    """
    Меню выбора направления пересылки:
        src → dst
        dst → src
        ⇄ обоих
    """
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.row(types.InlineKeyboardButton(f"{src} ➜ {dst}", callback_data=f"fw:one:{src}:{dst}"))
    kb.row(types.InlineKeyboardButton(f"{dst} ➜ {src}", callback_data=f"fw:one:{dst}:{src}"))
    kb.row(types.InlineKeyboardButton(f"{src} ⇄ {dst}", callback_data=f"fw:two:{src}:{dst}"))
    kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data="fw:back"))
    return kb


@bot.callback_query_handler(func=lambda c: c.data.startswith("fw"))
def on_forward_callback(call):
    """
    Меню пересылки — только для владельца.
    """
    try:
        if not OWNER_ID or call.message.chat.id != int(OWNER_ID):
            bot.answer_callback_query(call.id, "Нет доступа")
            return

        parts = call.data.split(":")
        action = parts[1]

        # ------- 1) Открыть список чатов -------
        if action == "menu":
            kb = build_forward_menu_keyboard()
            bot.edit_message_text(
                "🔁 Меню пересылки (выберите чат):",
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=kb
            )
            return

        # ------- 2) Очистить все связи -------
        if action == "clear_all":
            clear_forward_all()
            bot.answer_callback_query(call.id, "Все связи удалены.")
            kb = build_forward_menu_keyboard()
            bot.edit_message_reply_markup(
                call.message.chat.id,
                call.message.message_id,
                reply_markup=kb
            )
            return

        # ------- 3) выбран чат (fw:open:<cid>) -------
        if action == "open":
            cid = parts[2]
            selected_chat = cid
            bot.answer_callback_query(call.id)

            # меню выбора направления с OWNER_ID
            kb = build_forward_direction_keyboard(OWNER_ID, selected_chat)
            bot.edit_message_text(
                f"Чат выбран: {selected_chat}\nВыберите направление пересылки:",
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=kb
            )
            return

        # ------- 4) Односторонняя пересылка -------
        if action == "one":
            src = parts[2]
            dst = parts[3]
            add_forward_link(int(src), int(dst))

            bot.answer_callback_query(call.id, f"Добавлено: {src} → {dst}")
            kb = build_forward_menu_keyboard()
            bot.edit_message_reply_markup(
                call.message.chat.id,
                call.message.message_id,
                reply_markup=kb
            )
            return

        # ------- 5) Двусторонняя пересылка -------
        if action == "two":
            src = parts[2]
            dst = parts[3]
            add_forward_link(int(src), int(dst))
            add_forward_link(int(dst), int(src))

            bot.answer_callback_query(call.id, f"Связь {src} ⇄ {dst} установлена")
            kb = build_forward_menu_keyboard()
            bot.edit_message_reply_markup(
                call.message.chat.id,
                call.message.message_id,
                reply_markup=kb
            )
            return

        # ------- 6) Назад -------
        if action == "back":
            kb = build_forward_menu_keyboard()
            bot.edit_message_text(
                "🔁 Меню пересылки:",
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=kb
            )
            return

    except Exception as e:
        log_error(f"on_forward_callback: {e}")

# ==========================================================
# SECTION 26 — SERVICE: WEBHOOK & KEEP-ALIVE
# ==========================================================

def keep_alive_thread():
    """
    Периодически делает self-ping, чтобы Render / Railway не засыпали приложение.
    """
    while True:
        try:
            if APP_URL:
                requests.get(APP_URL)
        except Exception as e:
            log_error(f"keep_alive_thread: {e}")
        time.sleep(KEEP_ALIVE_INTERVAL_SECONDS)


def set_webhook():
    """
    Устанавливает webhook при запуске.
    """
    if not APP_URL:
        log_info("APP_URL не задан — запускаем polling.")
        return False

    wh_url = f"{APP_URL}/{BOT_TOKEN}"
    try:
        bot.remove_webhook()
        time.sleep(1)
        bot.set_webhook(url=wh_url)
        log_info(f"Webhook установлен: {wh_url}")
        return True
    except Exception as e:
        log_error(f"set_webhook error: {e}")
        return False


# ==========================================================
# SECTION 27 — FLASK ENDPOINT
# ==========================================================

@app.route(f"/{BOT_TOKEN}", methods=["POST"])
def webhook_handler():
    """
    Все обновления от Telegram попадают сюда, если активен webhook.
    """
    try:
        json_str = request.get_data().decode("utf-8")
        update = telebot.types.Update.de_json(json_str)
        bot.process_new_updates([update])
    except Exception as e:
        log_error(f"webhook_handler: {e}")
    return "OK", 200


@app.route("/", methods=["GET"])
def root():
    return f"Bot {VERSION} running.", 200


# ==========================================================
# SECTION 28 — STARTUP RESTORE (Google Drive restore once)
# ==========================================================

def startup_restore():
    """
    Выполняется один раз при старте сервера.
    Восстанавливает data.json, data.csv, csv_meta.json из Google Drive,
    если их нет локально.
    """
    try:
        restored = restore_from_gdrive_if_needed()
        if restored:
            log_info("✔️ Данные успешно восстановлены при старте.")
        else:
            log_info("Нет восстановления из Google Drive (локальные файлы найдены).")
    except Exception as e:
        log_error(f"startup_restore: {e}")


# ==========================================================
# SECTION 29 — SCHEDULE: AUTO-NEW-DAY WINDOW
# ==========================================================

def auto_new_day_thread():
    """
    Каждый день в 00:01 создаёт новое окно дня для активных чатов,
    чтобы пользователю не приходилось нажимать ничего вручную.
    """
    while True:
        try:
            now_dt = now_local()
            if now_dt.hour == 0 and now_dt.minute == 1:
                dk = today_key()
                for cid in list(finance_active_chats):
                    try:
                        txt, _ = render_day_window(cid, dk)
                        kb = build_main_keyboard(dk, cid)
                        bot.send_message(cid, txt, reply_markup=kb, parse_mode="HTML")
                    except Exception as e:
                        log_error(f"auto_new_day_thread chat {cid}: {e}")

                time.sleep(60)

        except Exception as e:
            log_error(f"auto_new_day_thread: {e}")

        time.sleep(20)


# ==========================================================
# SECTION 30 — STARTUP THREADS
# ==========================================================

def start_background_threads():
    """
    Запускает:
      • keep-alive
      • авто-новый день
    """
    th1 = threading.Thread(target=keep_alive_thread, daemon=True)
    th1.start()

    th2 = threading.Thread(target=auto_new_day_thread, daemon=True)
    th2.start()

    log_info("Background threads started.")

# ==========================================================
# SECTION 31 — RUN BOT (WEBHOOK / POLLING)
# ==========================================================

def run_polling():
    """
    Запуск обычным polling (если APP_URL не задан).
    """
    log_info("Запуск бота в режиме polling…")
    bot.infinity_polling(timeout=60, long_polling_timeout=60)


def run_webhook():
    """
    Запуск через webhook.
    """
    ok = set_webhook()
    if not ok:
        log_error("Webhook не установлен – fallback to polling")
        run_polling()
        return

    log_info("Запуск Flask-сервера для приёма webhook…")
    app.run(host="0.0.0.0", port=PORT)


# ==========================================================
# SECTION 32 — MAIN
# ==========================================================

if __name__ == "__main__":
    # 1) Восстанавливаем файлы из Google Drive, если их нет
    startup_restore()

    # 2) Загружаем данные
    data = load_data()

    # 3) Запускаем фоновые потоки
    start_background_threads()

    # 4) Запуск бота: webhook или polling
    if APP_URL:
        run_webhook()
    else:
        run_polling()