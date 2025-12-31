#норм
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
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.discovery import build
from google.oauth2 import service_account
from telebot.types import InputMediaDocument

# -----------------------------
# ⚙️ Конфигурация (жёстко прописанные значения для Render)
# -----------------------------
BOT_TOKEN = os.getenv("B_T")
#OWNER_ID = "8592220081"
APP_URL = "https://fo-1.onrender.com"
WEBHOOK_URL = "https://fo-1.onrender.com"  # если дальше в коде используется отдельная переменная вебхука
PORT = 5000

BACKUP_CHAT_ID = "-1003291414261"

#BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = os.getenv("ID", "").strip()
#BACKUP_CHAT_ID = os.getenv("BACKUP_CHAT_ID", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "").strip()
#APP_URL = os.getenv("APP_URL", "").strip()
#PORT = int(os.getenv("PORT", "8443"))
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")
VERSION = "Code_FINAL_NAVFIX_v1"
DEFAULT_TZ = "America/Argentina/Buenos_Aires"
KEEP_ALIVE_INTERVAL_SECONDS = 60
DATA_FILE = "data.json"
CSV_FILE = "data.csv"
CSV_META_FILE = "csv_meta.json"
forward_map = {}
# (src_chat_id, src_msg_id) -> [(dst_chat_id, dst_msg_id)]
backup_flags = {
    "drive": True,
    "channel": True,
}
restore_mode = None
#restore_mode = False
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)
bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)
app = Flask(__name__)
data = {}
finance_active_chats = set()
def log_info(msg: str):
    logger.info(msg)
def log_error(msg: str):
    logger.error(msg)
def get_tz():
    """Return local timezone, with fallback to UTC-3."""
    try:
        return ZoneInfo(DEFAULT_TZ)
    except Exception:
        return timezone(timedelta(hours=-3))
def now_local():
    return datetime.now(get_tz())
def today_key() -> str:
    return now_local().strftime("%Y-%m-%d")


def fmt_date_ddmmyy(day_key: str) -> str:
    """YYYY-MM-DD -> DD.MM.YY"""
    try:
        d = datetime.strptime(day_key, "%Y-%m-%d")
        return d.strftime("%d.%m.%y")
    except Exception:
        return str(day_key)

def week_start_monday(day_key: str) -> str:
    """Возвращает YYYY-MM-DD (понедельник недели) для day_key"""
    try:
        d = datetime.strptime(day_key, "%Y-%m-%d").date()
    except Exception:
        d = now_local().date()
    start = d - timedelta(days=d.weekday())
    return start.strftime("%Y-%m-%d")

def week_bounds_from_start(start_key: str):
    """start_key (YYYY-MM-DD, понедельник) -> (start_key, end_key)"""
    try:
        s = datetime.strptime(start_key, "%Y-%m-%d").date()
    except Exception:
        s = now_local().date() - timedelta(days=now_local().date().weekday())
    e = s + timedelta(days=6)
    return s.strftime("%Y-%m-%d"), e.strftime("%Y-%m-%d")
def _load_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log_error(f"JSON load error {path}: {e}")
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
        log_info("csv_meta.json updated")
    except Exception as e:
        log_error(f"_save_csv_meta: {e}")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CHAT_BACKUP_META_FILE = os.path.join(BASE_DIR, "chat_backup_meta.json")
log_info(f"chat_backup_meta.json PATH = {CHAT_BACKUP_META_FILE}")
def _load_chat_backup_meta() -> dict:
    """Загрузка meta-файла бэкапов для всех чатов."""
    try:
        if not os.path.exists(CHAT_BACKUP_META_FILE):
            return {}
        return _load_json(CHAT_BACKUP_META_FILE, {})
    except Exception as e:
        log_error(f"_load_chat_backup_meta: {e}")
        return {}
def _save_chat_backup_meta(meta: dict) -> None:
    """Сохранение meta-файла в ТОТ ЖЕ каталог, где лежит бот."""
    try:
        log_info(f"SAVING META TO: {os.path.abspath(CHAT_BACKUP_META_FILE)}")
        _save_json(CHAT_BACKUP_META_FILE, meta)
        log_info("chat_backup_meta.json updated")
    except Exception as e:
        log_error(f"_save_chat_backup_meta: {e}")
def send_backup_to_chat(chat_id: int) -> None:
    """
    Универсальный авто-бэкап JSON прямо в чате.
    Работает одинаково для владельца, групп, каналов, всех чатов.
    Логика:
    • гарантируем актуальный data_<chat_id>.json
    • читаем meta-файл chat_backup_meta.json
    • если есть msg_id → edit_message_media()
    • если нет / не найдено → отправляем новое сообщение
    • обновляем meta-файл в рабочей директории (Render-friendly)
    • при смене дня (после 00:00) создаётся НОВОЕ сообщение с файлом
    """
    try:
        if not chat_id:
            return
        try:
            save_chat_json(chat_id)
        except Exception as e:
            log_error(f"send_backup_to_chat save_chat_json({chat_id}): {e}")
        json_path = chat_json_file(chat_id)
        if not os.path.exists(json_path):
            log_error(f"send_backup_to_chat: {json_path} NOT FOUND")
            return

        meta = _load_chat_backup_meta()
        msg_key = f"msg_chat_{chat_id}"
        ts_key = f"timestamp_chat_{chat_id}"

        chat_title = _get_chat_title_for_backup(chat_id)
        caption = (
            f"🧾 Авто-бэкап JSON чата: {chat_title}\n"
            f"⏱ {now_local().strftime('%Y-%m-%d %H:%M:%S')}"
        )

        # 🔄 Новый файл после смены дня
        last_ts = meta.get(ts_key)
        msg_id = meta.get(msg_key)
        if msg_id and last_ts:
            try:
                prev_dt = datetime.fromisoformat(last_ts)
                if prev_dt.date() != now_local().date():
                    # Новый день — старое сообщение оставляем как архив, создаём новое
                    msg_id = None
            except Exception as e:
                log_error(f"send_backup_to_chat: bad timestamp for chat {chat_id}: {e}")

        def _open_file() -> io.BytesIO | None:
            """Чтение JSON в BytesIO с правильным именем файла."""
            try:
                with open(json_path, "rb") as f:
                    data_bytes = f.read()
            except Exception as e:
                log_error(f"send_backup_to_chat open({json_path}): {e}")
                return None
            if not data_bytes:
                return None
            base = os.path.basename(json_path)
            name_no_ext, dot, ext = base.partition(".")
            suffix = get_chat_name_for_filename(chat_id)
            if suffix:
                file_name = suffix
            else:
                file_name = name_no_ext
            if dot:
                file_name += f".{ext}"
            buf = io.BytesIO(data_bytes)
            buf.name = file_name
            return buf
            #🟢🟢🟢🟢
        if msg_id:
            fobj = _open_file()
            if not fobj:
                return
            try:
                
                log_info(f"Chat backup UPDATED in chat {chat_id}")
                meta[ts_key] = now_local().isoformat(timespec="seconds")
                _save_chat_backup_meta(meta)
                set_active_window_id(chat_id, day_key, mid)
                return
            except Exception as e:
                log_error(f"send_backup_to_chat edit FAILED in {chat_id}: {e}")

        fobj = _open_file()
        if not fobj:
            return
        sent = bot.send_document(chat_id, fobj, caption=caption)
        meta[msg_key] = sent.message_id
        meta[ts_key] = now_local().isoformat(timespec="seconds")
        _save_chat_backup_meta(meta)
        log_info(f"Chat backup CREATED in chat {chat_id}")
    except Exception as e:
        log_error(f"send_backup_to_chat({chat_id}): {e}")
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
    }
def load_data():
    d = _load_json(DATA_FILE, default_data())
    base = default_data()
    for k, v in base.items():
        if k not in d:
            d[k] = v
    flags = d.get("backup_flags") or {}
    backup_flags["drive"] = bool(flags.get("drive", True))
    backup_flags["channel"] = bool(flags.get("channel", True))
    fac = d.get("finance_active_chats") or {}
    finance_active_chats.clear()
    for cid, enabled in fac.items():
        if enabled:
            try:
                finance_active_chats.add(int(cid))
            except Exception:
                pass

# ✅ OWNER — финансовый режим всегда включён
    if OWNER_ID:
        try:
            finance_active_chats.add(int(OWNER_ID))
        except Exception:
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
def chat_json_file(chat_id: int) -> str:
    return f"data_{chat_id}.json"
def chat_csv_file(chat_id: int) -> str:
    return f"data_{chat_id}.csv"
def chat_meta_file(chat_id: int) -> str:
    return f"csv_meta_{chat_id}.json"
def get_chat_store(chat_id: int) -> dict:
    """
    Хранилище данных одного чата.
    Добавлено поле "known_chats" для отображения названий/username в меню пересылки.
    """
    chats = data.setdefault("chats", {})
    store = chats.setdefault(
        str(chat_id),
        {
            "info": {},
            "known_chats": {},
            "balance": 0,
            "records": [],
            "daily_records": {},
            "next_id": 1,
            "active_windows": {},
            "edit_wait": None,
            "edit_target": None,
            "current_view_day": today_key(),
            "settings": {
                "auto_add": False
            },
        }
    )
    # ✅ OWNER — авто-добавление всегда включено
    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        store["settings"]["auto_add"] = True
    if "known_chats" not in store:
        store["known_chats"] = {}
    return store
def save_chat_json(chat_id: int):
    """
    Save per-chat JSON, CSV and META for one chat.
    """
    try:
        store = data.get("chats", {}).get(str(chat_id))
        if not store:
            store = get_chat_store(chat_id)
        chat_path_json = chat_json_file(chat_id)
        chat_path_csv = chat_csv_file(chat_id)
        chat_path_meta = chat_meta_file(chat_id)
        for p in (chat_path_json, chat_path_csv, chat_path_meta):
            if not os.path.exists(p):
                with open(p, "a", encoding="utf-8"):
                    pass
        payload = {
            "chat_id": chat_id,
            "balance": store.get("balance", 0),
            "records": store.get("records", []),
            "daily_records": store.get("daily_records", {}),
            "next_id": store.get("next_id", 1),
            "info": store.get("info", {}),
            "known_chats": store.get("known_chats", {}),
        }
        _save_json(chat_path_json, payload)
        with open(chat_path_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["chat_id", "ID", "short_id", "timestamp", "amount", "note", "owner", "day_key"])
            daily = store.get("daily_records", {})
            for dk in sorted(daily.keys()):
                recs = daily.get(dk, [])
                recs_sorted = sorted(recs, key=lambda r: r.get("timestamp", ""))
                for r in recs_sorted:
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
        meta = {
            "last_saved": now_local().isoformat(timespec="seconds"),
            "record_count": sum(len(v) for v in store.get("daily_records", {}).values()),
        }
        _save_json(chat_path_meta, meta)
        log_info(f"Per-chat files saved for chat {chat_id}")
    except Exception as e:
        log_error(f"save_chat_json({chat_id}): {e}")
        
def restore_from_json(chat_id: int, path: str):
    """
    Восстановление из JSON.
    Поддержка:
      1) data.json (глобальный) — если внутри есть ключ "chats"
      2) data_<chat_id>.json (пер-чат) — если внутри есть ключи "records"/"daily_records"
    """
    global data
    payload = _load_json(path, None)
    if not isinstance(payload, dict):
        raise RuntimeError("JSON повреждён или пустой")

    # 1) Глобальный data.json
    if "chats" in payload and isinstance(payload.get("chats"), dict):
        data = payload
        # гарантируем структуру
        base = default_data()
        for k, v in base.items():
            if k not in data:
                data[k] = v

        # восстановим finance_active_chats из data (если есть)
        finance_active_chats.clear()
        fac = data.get("finance_active_chats") or {}
        if isinstance(fac, dict):
            for cid, enabled in fac.items():
                if enabled:
                    try:
                        finance_active_chats.add(int(cid))
                    except Exception:
                        pass

        rebuild_global_records()
        save_data(data)

        # сохраним файлы всех чатов, чтобы синхронизировать data_<id>.json и csv
        for cid_str in list(data.get("chats", {}).keys()):
            try:
                save_chat_json(int(cid_str))
            except Exception as e:
                log_error(f"restore_from_json: save_chat_json({cid_str}) failed: {e}")

        export_global_csv(data)
        log_info("restore_from_json: global data restored")
        return

    # 2) Пер-чат JSON: data_<chat_id>.json
    # Ожидаем ключи как в save_chat_json()
    if "records" in payload or "daily_records" in payload:
        store = get_chat_store(chat_id)

        store["records"] = payload.get("records", []) or []
        store["daily_records"] = payload.get("daily_records", {}) or {}
        store["next_id"] = int(payload.get("next_id", 1) or 1)
        store["info"] = payload.get("info", store.get("info", {})) or store.get("info", {})
        store["known_chats"] = payload.get("known_chats", store.get("known_chats", {})) or store.get("known_chats", {})

        # пересобираем records из daily_records, если вдруг records пустой/битый
        if not store["records"] and store["daily_records"]:
            all_recs = []
            for dk in sorted(store["daily_records"].keys()):
                all_recs.extend(store["daily_records"][dk] or [])
            store["records"] = all_recs

        renumber_chat_records(chat_id)
        recalc_balance(chat_id)
        rebuild_global_records()

        save_data(data)
        save_chat_json(chat_id)
        export_global_csv(data)

        log_info(f"restore_from_json: chat {chat_id} restored from per-chat JSON")
        return

    raise RuntimeError("Неизвестный формат JSON (нет 'chats' и нет 'records/daily_records').")
#🟢🟢🟢🟢
def restore_from_csv(chat_id: int, path: str):
    """
    Восстановление из CSV (пер-чат).
    Ожидает колонки как у тебя в CSV:
    chat_id,ID,short_id,timestamp,amount,note,owner,day_key
    """
    store = get_chat_store(chat_id)

    daily = {}
    records = []

    with open(path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                dk = (row.get("day_key") or today_key()).strip()
                amt = float(row.get("amount") or 0)
                note = (row.get("note") or "").strip()
                owner = row.get("owner") or ""
                ts = (row.get("timestamp") or now_local().isoformat(timespec="seconds")).strip()

                rec = {
                    "id": int(row.get("ID") or 0) or 0,
                    "short_id": row.get("short_id") or "",
                    "timestamp": ts,
                    "amount": amt,
                    "note": note,
                    "owner": owner,
                }
                daily.setdefault(dk, []).append(rec)
                records.append(rec)
            except Exception as e:
                log_error(f"restore_from_csv row skip: {e}")

    store["daily_records"] = daily
    store["records"] = records

    renumber_chat_records(chat_id)
    recalc_balance(chat_id)
    rebuild_global_records()

    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)

    log_info(f"restore_from_csv: chat {chat_id} restored from CSV")

def fmt_num(x):
    """
    Европейский формат вывода с обязательным знаком.
    Примеры:
        +1234.56 → ➕ 1.234,56
        -800     → ➖ 800
        0        → ➕ 0
    """
    sign = "+" if x >= 0 else "-"
    x = abs(x)
    s = f"{x:.12f}".rstrip("0").rstrip(".")
    if "." in s:
        int_part, dec_part = s.split(".")
    else:
        int_part, dec_part = s, ""
    int_part = f"{int(int_part):,}".replace(",", ".")
    if dec_part:
        s = f"{int_part},{dec_part}"
    else:
        s = int_part
    return f"{sign}{s}"
num_re = re.compile(r"[+\-–]?\s*\d[\d\s.,_'’]*")
def parse_amount(raw: str) -> float:
    """
    Универсальный парсер:
    - понимает любые разделители
    - смешанные форматы (1.234,56 / 1,234.56)
    - определяет десятичную часть по самому правому разделителю
    - число без знака = расход
    """
    s = raw.strip()
    is_negative = s.startswith("-") or s.startswith("–")
    is_positive = s.startswith("+")
    s_clean = s.lstrip("+-–").strip()
    s_clean = (
        s_clean.replace(" ", "")
        .replace("_", "")
        .replace("’", "")
        .replace("'", "")
    )
    if "," not in s_clean and "." not in s_clean:
        value = float(s_clean)
        if not is_positive and not is_negative:
            is_negative = True
        return -value if is_negative else value
    if "." in s_clean and "," in s_clean:
        if s_clean.rfind(",") > s_clean.rfind("."):
            s_clean = s_clean.replace(".", "")
            s_clean = s_clean.replace(",", ".")
        else:
            s_clean = s_clean.replace(",", "")
    else:
        if "," in s_clean:
            pos = s_clean.rfind(",")
            if len(s_clean) - pos - 1 in (1, 2):
                s_clean = s_clean.replace(".", "")
                s_clean = s_clean.replace(",", ".")
            else:
                s_clean = s_clean.replace(",", "")
        elif "." in s_clean:
            pos = s_clean.rfind(".")
            if len(s_clean) - pos - 1 in (1, 2):
                s_clean = s_clean.replace(",", "")
            else:
                s_clean = s_clean.replace(".", "")
    value = float(s_clean)
    if not is_positive and not is_negative:
        is_negative = True
    return -value if is_negative else value
def split_amount_and_note(text: str):
    """
    Возвращает:
        amount (float)
        note (str)
    """
    m = num_re.search(text)
    if not m:
        raise ValueError("no number found")
    raw_number = m.group(0)
    amount = parse_amount(raw_number)
    note = text.replace(raw_number, " ").strip()
    note = re.sub(r"\s+", " ", note).lower()
    return amount, note


# =============================
# 📦 EXPENSE CATEGORIES (v1)
# =============================
EXPENSE_CATEGORIES = {
    "ПРОДУКТЫ": ["продукты", "шб", "еда"],
}

def resolve_expense_category(note: str):
    if not note:
        return None
    n = str(note).lower()
    for cat, keywords in EXPENSE_CATEGORIES.items():
        for kw in keywords:
            if kw in n:
                return cat
    return None

def calc_categories_for_period(store: dict, start: str, end: str) -> dict:
    """Считает суммы расходов по статьям (только отрицательные amount) в диапазоне дат включительно."""
    out = {}
    daily = store.get("daily_records", {}) or {}
    for day, records in daily.items():
        if not (start <= day <= end):
            continue
        for r in (records or []):
            amt = float(r.get("amount", 0) or 0)
            if amt >= 0:
                continue
            cat = resolve_expense_category(r.get("note", ""))
            if not cat:
                continue
            out[cat] = out.get(cat, 0) + (-amt)
    return out


def collect_items_for_category(store: dict, start: str, end: str, category: str):
    """Возвращает список (day, amount, note) для указанной статьи и периода."""
    items = []
    daily = store.get("daily_records", {}) or {}
    for day, records in daily.items():
        if not (start <= day <= end):
            continue
        for r in (records or []):
            amt = float(r.get("amount", 0) or 0)
            if amt >= 0:
                continue
            note = r.get("note", "")
            if resolve_expense_category(note) == category:
                items.append((day, -amt, note))
    return items


def looks_like_amount(text):
    try:
        amount, note = split_amount_and_note(text)
        return True
    except:
        return False
@bot.message_handler(
    func=lambda m: not (m.text and m.text.startswith("/")),
    content_types=[
        "text", "photo", "video",
        "audio", "voice", "video_note",
        "sticker", "location", "venue", "contact"
    ]
)
def on_any_message(msg):
    chat_id = msg.chat.id

    # ✅ OWNER — гарантируем включённый финансовый режим
    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        finance_active_chats.add(chat_id)

    # ✅ 1️⃣ ВСЕГДА регистрируем чат
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    # 🔒 restore_mode — только блокируем финансы, НЕ пересылку
    if restore_mode is not None and restore_mode == chat_id:
        return
        #if msg.content_type != "document":
            # ⚠️ финансы запрещены
            #pass
        # ❗ НО пересылка РАЗРЕШЕНА

    # 2️⃣ ФИНАНСЫ — ТОЛЬКО если включены
    if msg.content_type == "text":
        try:
            if is_finance_mode(chat_id):
                handle_finance_text(msg)
        except Exception as e:
            log_error(f"handle_finance_text error: {e}")

    # 3️⃣ ПЕРЕСЫЛКА — ВСЕГДА
    forward_any_message(chat_id, msg)
def handle_finance_text(msg):
    """
    Обработка обычного текстового ввода:
    - авто-добавление (если включено)
    - добавление через кнопку "➕ Добавить"
    - редактирование записи
    """
    if msg.content_type != "text":
        return

    chat_id = msg.chat.id
    text = (msg.text or "").strip()
    if not text:
        return

    # ❌ если фин-режим выключен — выходим
    if not is_finance_mode(chat_id):
        return

    store = get_chat_store(chat_id)

    # ✏️ режим редактирования записи
    # ✏️ режим редактирования записи
    wait = store.get("edit_wait")
    if wait and wait.get("type") == "edit":
        rid = wait.get("rid")
        day_key = wait.get("day_key", today_key())

        try:
            amount, note = split_amount_and_note(text)
        except Exception:
            send_and_auto_delete(chat_id, "❌ Не удалось разобрать сумму.")
            return

        # 🔥 ВАЖНО: редактирование НЕ должно создавать новую запись
        # Обновляем существующую запись, порядок не меняется
        update_record_in_chat(chat_id, rid, amount, note)

        store["edit_wait"] = None
        save_data(data)

        update_or_send_day_window(chat_id, day_key)
        schedule_finalize(chat_id, day_key)
        return

    # ➕ режим добавления через кнопку
    if wait and wait.get("type") == "add":
        day_key = wait.get("day_key", today_key())
        try:
            amount, note = split_amount_and_note(text)
        except Exception:
            send_and_auto_delete(chat_id, "❌ Не удалось разобрать сумму.")
            return

        add_record_to_chat(
            chat_id,
            amount,
            note,
            msg.from_user.id,
            source_msg=msg
        )
        store["edit_wait"] = None
        save_data(data)
        update_or_send_day_window(chat_id, day_key)
        schedule_finalize(chat_id, day_key)
        return

    # ⚙️ авто-добавление
    settings = store.get("settings", {})
    if settings.get("auto_add") and looks_like_amount(text):
        try:
            amount, note = split_amount_and_note(text)
        except Exception:
            return

        #add_record_to_chat(chat_id, amount, note, msg.from_user.id)
        add_record_to_chat(chat_id, amount, note, msg.from_user.id, source_msg=msg)
        day_key = store.get("current_view_day", today_key())
        update_or_send_day_window(chat_id, day_key)
        schedule_finalize(chat_id, day_key)
        return
      
def handle_finance_edit(msg):
    chat_id = msg.chat.id
    text = msg.text or msg.caption
    if not text:
        return False

    store = get_chat_store(chat_id)
    records = store.get("records", [])
    target = None

    #for r in records:
    for r in records:
        if (
            r.get("source_msg_id") == msg.message_id
            or r.get("origin_msg_id") == msg.message_id
            or r.get("msg_id") == msg.message_id
        ):
            target = r
            break
        
    if not target:
        log_info(f"[EDIT-FIN] record not found for msg_id={msg.message_id}")
        return False

    try:
        amount, note = split_amount_and_note(text)

        # 🔥 ВАЖНО: если исходная запись была расходом — сохраняем знак
        if target.get("amount", 0) < 0 and amount > 0:
            amount = -amount

    except Exception:
        log_info("[EDIT-FIN] bad format, ignored")
        return True  # edit перехвачен, но данных нет

    # обновляем ОСНОВНУЮ запись
    target["amount"] = amount
    target["note"] = note
    #target["timestamp"] = now_local().isoformat(timespec="seconds")

    # 🔥 ОБЯЗАТЕЛЬНО: обновляем daily_records
    for day, arr in store.get("daily_records", {}).items():
        for r in arr:
            if r.get("id") == target.get("id"):
                r.update(target)

    # пересчитываем баланс сразу
    store["balance"] = sum(r["amount"] for r in store.get("records", []))

    log_info(
        f"[EDIT-FIN] updated record R{target['id']} "
        f"amount={amount} note={note}"
    )
    return True
    #🍕🍕🍕🍕
def _get_drive_service():
    if not GOOGLE_SERVICE_ACCOUNT_JSON or not GDRIVE_FOLDER_ID:
        return None
    try:
        info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        creds = service_account.Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/drive"]
        )
        service = build("drive", "v3", credentials=creds)
        return service
    except Exception as e:
        log_error(f"Drive service error: {e}")
        return None
def upload_to_gdrive(path: str, mime_type: str = None, description: str | None = None):
    flags = backup_flags or {}
    if not flags.get("drive", True):
        log_info("GDrive backup disabled (drive flag = False).")
        return
    service = _get_drive_service()
    if service is None:
        return
    if not os.path.exists(path):
        log_error(f"upload_to_gdrive: file not found {path}")
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
            fields="files(id, name)",
        ).execute()
        items = existing.get("files", [])
        if items:
            file_id = items[0]["id"]
            service.files().update(
                fileId=file_id,
                media_body=media,
                body={"description": description or ""},
            ).execute()
            log_info(f"GDrive: updated {fname}, id={file_id}")
        else:
            created = service.files().create(
                body=file_metadata,
                media_body=media,
                fields="id"
            ).execute()
            log_info(f"GDrive: created {fname}, id={created.get('id')}")
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
            fields="files(id, name, mimeType, size)",
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
            status, done = downloader.next_chunk()
        log_info(f"GDrive: downloaded {filename} -> {dest_path}")
        return True
    except Exception as e:
        log_error(f"download_from_gdrive({filename}): {e}")
        return False
def restore_from_gdrive_if_needed() -> bool:
    """
    If local DATA_FILE/CSV_FILE/CSV_META_FILE are missing,
    try to restore them from Google Drive.
    """
    restored_any = False
    if not os.path.exists(DATA_FILE):
        if download_from_gdrive(os.path.basename(DATA_FILE), DATA_FILE):
            restored_any = True
    if not os.path.exists(CSV_FILE):
        if download_from_gdrive(os.path.basename(CSV_FILE), CSV_FILE):
            restored_any = True
    if not os.path.exists(CSV_META_FILE):
        if download_from_gdrive(os.path.basename(CSV_META_FILE), CSV_META_FILE):
            restored_any = True
    if restored_any:
        log_info("Data restored from Google Drive.")
    else:
        log_info("GDrive restore: nothing to restore.")
    return restored_any
def export_global_csv(d: dict):
    """Legacy global CSV with all chats (for backup channel)."""
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
        log_error(f"export_global_csv: {e}")
EMOJI_DIGITS = {
    "0": "0️⃣",
    "1": "1️⃣",
    "2": "2️⃣",
    "3": "3️⃣",
    "4": "4️⃣",
    "5": "5️⃣",
    "6": "6️⃣",
    "7": "7️⃣",
    "8": "8️⃣",
    "9": "9️⃣",
}
backup_channel_notified_chats = set()
def format_chat_id_emoji(chat_id: int) -> str:
    """Преобразует числовой chat_id в строку из emoji-цифр."""
    return "".join(EMOJI_DIGITS.get(ch, ch) for ch in str(chat_id))
def _safe_chat_title_for_filename(title) -> str:
    """Делает короткое безопасное имя чата для имени файла."""
    if not title:
        return ""
    title = str(title).strip()
    title = title.replace(" ", "_")
    title = re.sub(r"[^0-9A-Za-zА-Яа-я_\-]+", "", title)
    return title[:32]
def get_chat_name_for_filename(chat_id: int) -> str:
    """
    Выбор имени для файла:
        1) username
        2) title (имя чата)
        3) chat_id
    Всё преобразуется в короткое безопасное имя.
    """
    try:
        store = get_chat_store(chat_id)
        info = store.get("info", {})
        username = info.get("username")
        title = info.get("title")
        if username:
            base = username.lstrip("@")
        elif title:
            base = title
        else:
            base = str(chat_id)
        return _safe_chat_title_for_filename(base)
    except Exception as e:
        log_error(f"get_chat_name_for_filename({chat_id}): {e}")
        return _safe_chat_title_for_filename(str(chat_id))
def _get_chat_title_for_backup(chat_id: int) -> str:
    """Пытается достать название чата из store["info"]["title"]"""
    try:
        store = data.get("chats", {}).get(str(chat_id), {}) if isinstance(data, dict) else {}
        info = store.get("info", {})
        title = info.get("title")
        if title:
            return title
    except Exception as e:
        log_error(f"_get_chat_title_for_backup({chat_id}): {e}")
    return f"chat_{chat_id}"
def _get_chat_title_for_backup(chat_id: int) -> str:
    """
    Берём название чата из store["info"], чтобы подписывать бэкап.
    """
    try:
        store = get_chat_store(chat_id)
        info = store.get("info", {})
        title = info.get("title")
        if title:
            return title
    except Exception as e:
        log_error(f"_get_chat_title_for_backup({chat_id}): {e}")
    return f"chat_{chat_id}"
def send_backup_to_channel_for_file(base_path: str, meta_key_prefix: str, chat_title: str = None):
    """Helper to send or update a file in BACKUP_CHAT_ID with csv_meta tracking.
    Правило:
    • edit → если не удалось → send
    • если сообщение удалено вручную — файл создаётся заново
    """
    if not BACKUP_CHAT_ID:
        return
    if not os.path.exists(base_path):
        log_error(f"send_backup_to_channel_for_file: {base_path} not found")
        return

    try:
        meta = _load_csv_meta()
        msg_key = f"msg_{meta_key_prefix}"
        ts_key = f"timestamp_{meta_key_prefix}"

        base_name = os.path.basename(base_path)
        name_without_ext, dot, ext = base_name.partition(".")
        safe_title = _safe_chat_title_for_filename(chat_title)

        if safe_title:
            file_name = safe_title + (f".{ext}" if dot else "")
        else:
            file_name = base_name

        caption = f"📦 {file_name} — {now_local().strftime('%Y-%m-%d %H:%M')}"

        def _open_for_telegram() -> io.BytesIO | None:
            if not os.path.exists(base_path):
                return None
            with open(base_path, "rb") as src:
                data_bytes = src.read()
            if not data_bytes:
                log_error(f"send_backup_to_channel_for_file: {base_path} is empty, skip")
                return None
            buf = io.BytesIO(data_bytes)
            buf.name = file_name
            buf.seek(0)
            return buf

        sent = False

        # ─────────────────────────────
        # 🔄 ПРОБУЕМ ОБНОВИТЬ
        # ─────────────────────────────
        if meta.get(msg_key):
            try:
                fobj = _open_for_telegram()
                if not fobj:
                    return
                bot.edit_message_media(
                    chat_id=int(BACKUP_CHAT_ID),
                    message_id=meta[msg_key],
                    media=types.InputMediaDocument(
                        media=fobj,
                        caption=caption
                    )
                )
                sent = True
                log_info(f"[BACKUP] channel file updated: {base_path}")
            except Exception as e:
                log_error(f"[BACKUP] edit failed, will resend: {e}")
                try:
                    bot.delete_message(int(BACKUP_CHAT_ID), meta[msg_key])
                except Exception:
                    pass

        # ─────────────────────────────
        # ➕ ОТПРАВЛЯЕМ НОВЫЙ
        # ─────────────────────────────
        if not sent:
            fobj = _open_for_telegram()
            if not fobj:
                return
            sent_msg = bot.send_document(
                int(BACKUP_CHAT_ID),
                fobj,
                caption=caption
            )
            meta[msg_key] = sent_msg.message_id
            log_info(f"[BACKUP] channel file sent new: {base_path}")

        meta[ts_key] = now_local().isoformat(timespec="seconds")
        _save_csv_meta(meta)

    except Exception as e:
        log_error(f"send_backup_to_channel_for_file({base_path}): {e}")
def send_backup_to_channel(chat_id: int):
    """
    Общий бэкап файлов чата в BACKUP_CHAT_ID.
    Делает:
    • проверку флага backup_flags["channel"]
    • один раз (на первый бэкап чата) отправляет chat_id эмодзи в канал
    • обновляет/создаёт:
        - data_<chat_id>.json
        - data_<chat_id>.csv
    """
    try:
        if not BACKUP_CHAT_ID:
            return
        if not backup_flags.get("channel", True):
            log_info("send_backup_to_channel: channel backup disabled by flag.")
            return
        try:
            backup_chat_id = int(BACKUP_CHAT_ID)
        except Exception:
            log_error("send_backup_to_channel: BACKUP_CHAT_ID не является числом.")
            return
        save_chat_json(chat_id)
        export_global_csv(data)
        save_data(data)
        chat_title = _get_chat_title_for_backup(chat_id)
        if chat_id not in backup_channel_notified_chats:
            try:
                emoji_id = format_chat_id_emoji(chat_id)
                bot.send_message(backup_chat_id, emoji_id)
                backup_channel_notified_chats.add(chat_id)
            except Exception as e:
                log_error(
                    f"send_backup_to_channel: не удалось отправить emoji chat_id "
                    f"в канал: {e}"
                )
        json_path = chat_json_file(chat_id)
        csv_path = chat_csv_file(chat_id)
        send_backup_to_channel_for_file(json_path, f"json_{chat_id}", chat_title)
        send_backup_to_channel_for_file(csv_path, f"csv_{chat_id}", chat_title)
    except Exception as e:
        log_error(f"send_backup_to_channel({chat_id}): {e}")
#⏏️⏏️⏏️⏏️⏏️⏏️
def _owner_data_file() -> str | None:
    """
    Файл владельца, где хранится forward_rules.
    """
    if not OWNER_ID:
        return None
    try:
        return f"data_{int(OWNER_ID)}.json"
    except Exception:
        return None
def load_forward_rules():
    """
    Загружает forward_rules из файла владельца.
    Поддерживает старый формат (списки) и новый (словарь).
    """
    try:
        path = _owner_data_file()
        if not path or not os.path.exists(path):
            return {}
        payload = _load_json(path, {}) or {}
        fr = payload.get("forward_rules", {})
        upgraded = {}
        for src, value in fr.items():
            if isinstance(value, list):
                upgraded[src] = {}
                for dst in value:
                    upgraded[src][dst] = "oneway_to"
            elif isinstance(value, dict):
                upgraded[src] = value
            else:
                continue
        return upgraded
    except Exception as e:
        log_error(f"load_forward_rules: {e}")
        return {}
def persist_forward_rules_to_owner():
    """
    Сохраняет forward_rules (в НОВОМ формате) только в data_OWNER.json.
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
        payload["forward_rules"] = data.get("forward_rules", {})
        _save_json(path, payload)
        log_info(f"forward_rules persisted to {path}")
    except Exception as e:
        log_error(f"persist_forward_rules_to_owner: {e}")
        
def resolve_forward_targets(source_chat_id: int):
    fr = data.get("forward_rules", {})
    src = str(source_chat_id)
    if src not in fr:
        return []
    out = []
    for dst, mode in fr[src].items():
        try:
            out.append((int(dst), mode))
        except:
            continue
    return out
def add_forward_link(src_chat_id: int, dst_chat_id: int, mode: str):
    fr = data.setdefault("forward_rules", {})
    src = str(src_chat_id)
    dst = str(dst_chat_id)
    fr.setdefault(src, {})[dst] = mode
    save_data(data)
def remove_forward_link(src_chat_id: int, dst_chat_id: int):
    fr = data.get("forward_rules", {})
    src = str(src_chat_id)
    dst = str(dst_chat_id)
    if src in fr and dst in fr[src]:
        del fr[src][dst]
    if src in fr and not fr[src]:
        del fr[src]
    save_data(data)
def clear_forward_all():
    """Полностью отключает всю пересылку."""
    data["forward_rules"] = {}
    persist_forward_rules_to_owner()
    save_data(data)
    

def forward_any_message(source_chat_id: int, msg):
    try:
        targets = resolve_forward_targets(source_chat_id)
        if not targets:
            return

        for dst, mode in targets:
            sent = bot.copy_message(
                dst,
                source_chat_id,
                msg.message_id
            )

            key = (source_chat_id, msg.message_id)
            forward_map.setdefault(key, []).append(
                (dst, sent.message_id)
            )

    except Exception as e:
        log_error(f"forward_any_message fatal: {e}")
        
# ===============================
# UNIVERSAL SAFE FORWARD (ALL TYPES)
# ===============================
#def _forward_copy_any(chat_id, msg, targets):
#def forward_media_anon(source_chat_id: int, msg, targets: list[tuple[int, str]]):
#_media_group_cache = {}
#def collect_media_group(chat_id: int, msg):
    
#def forward_media_group_anon(source_chat_id: int, messages: list, targets: list[tuple[int, str]]):

def render_day_window(chat_id: int, day_key: str):
    store = get_chat_store(chat_id)
    recs = store.get("daily_records", {}).get(day_key, [])
    lines = []
    d = datetime.strptime(day_key, "%Y-%m-%d")
    wd = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"][d.weekday()]
    t = now_local()
    td = t.strftime("%Y-%m-%d")
    yd = (t - timedelta(days=1)).strftime("%Y-%m-%d")
    tm = (t + timedelta(days=1)).strftime("%Y-%m-%d")
    tag = "сегодня" if day_key == td else "вчера" if day_key == yd else "завтра" if day_key == tm else ""
    dk = fmt_date_ddmmyy(day_key)
    label = f"{dk} ({tag}, {wd})" if tag else f"{dk} ({wd})"
    lines.append(f"📅 {label}")
    lines.append("")
    total_income = 0.0
    total_expense = 0.0
    recs_sorted = sorted(recs, key=lambda x: x.get("timestamp"))
    for r in recs_sorted:
        amt = r["amount"]
        if amt >= 0:
            total_income += amt
        else:
            total_expense += -amt
        note = html.escape(r.get("note", ""))
        sid = r.get("short_id", f"R{r['id']}")
        lines.append(f"{sid} {fmt_num(amt)} {note}")
    if not recs_sorted:
        lines.append("Нет записей за этот день.")
    lines.append("")
    if recs_sorted:
        lines.append(f"📉 Расход за день: {fmt_num(-total_expense) if total_expense else fmt_num(0)}")
        lines.append(f"📈 Приход за день: {fmt_num(total_income) if total_income else fmt_num(0)}")
    bal_chat = store.get("balance", 0)
    lines.append(f"🏦 Остаток по чату: {fmt_num(bal_chat)}")
    total = total_income - total_expense
    return "\n".join(lines), total
def build_main_keyboard(day_key: str, chat_id=None):
    kb = types.InlineKeyboardMarkup(row_width=3)
    kb.row(
        types.InlineKeyboardButton("➕ Добавить", callback_data=f"d:{day_key}:add"),
        types.InlineKeyboardButton("📋 Меню", callback_data=f"d:{day_key}:menu")
    )
    kb.row(
        types.InlineKeyboardButton("⬅️ Вчера", callback_data=f"d:{day_key}:prev"),
        types.InlineKeyboardButton("📅 Сегодня", callback_data=f"d:{day_key}:today"),
        types.InlineKeyboardButton("➡️ Завтра", callback_data=f"d:{day_key}:next")
    )
    kb.row(
        types.InlineKeyboardButton("📅 Календарь", callback_data=f"d:{day_key}:calendar"),
        types.InlineKeyboardButton("📊 Отчёт", callback_data=f"d:{day_key}:report")
    )
    kb.row(
        types.InlineKeyboardButton("ℹ️ Инфо", callback_data=f"d:{day_key}:info"),
        types.InlineKeyboardButton("💰 Общий итог", callback_data=f"d:{day_key}:total")
    )
    return kb
def build_calendar_keyboard(center_day: datetime, chat_id=None):
    """
    Календарь на 31 день.
    Дни с записями помечаются точкой: • 12.03
    """
    kb = types.InlineKeyboardMarkup(row_width=4)
    daily = {}
    if chat_id is not None:
        store = get_chat_store(chat_id)
        daily = store.get("daily_records", {})
    start_day = center_day - timedelta(days=15)
    for week in range(0, 32, 4):
        row = []
        for d in range(4):
            day = start_day + timedelta(days=week + d)
            label = day.strftime("%d.%m")
            key = day.strftime("%Y-%m-%d")
            if daily.get(key):
                label = "📝 " + label
            row.append(
                types.InlineKeyboardButton(
                    label,
                    callback_data=f"d:{key}:open"
                )
            )
        kb.row(*row)
    kb.row(
        types.InlineKeyboardButton(
            "⬅️ −31",
            callback_data=f"c:{(center_day - timedelta(days=31)).strftime('%Y-%m-%d')}"
        ),
        types.InlineKeyboardButton(
            "➡️ +31",
            callback_data=f"c:{(center_day + timedelta(days=31)).strftime('%Y-%m-%d')}"
        )
    )
    kb.row(
        types.InlineKeyboardButton(
            "📅 Сегодня",
            callback_data=f"d:{today_key()}:open"
        )
    )
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
    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        kb.row(
            types.InlineKeyboardButton("🔁 Пересылка", callback_data=f"d:{day_key}:forward_menu")
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
    
def build_cancel_edit_keyboard(day_key: str):
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton(
            "❌ Отмена",
            callback_data=f"d:{day_key}:cancel_edit"
        )
    )
    return kb

def build_forward_chat_list(day_key: str, chat_id: int):
    """
    Меню выбора чата для пересылки.
    Теперь список берём из known_chats владельца (все чаты, где был бот).
    """
    kb = types.InlineKeyboardMarkup()
    if not OWNER_ID:
        return kb
    owner_store = get_chat_store(int(OWNER_ID))
    known = owner_store.get("known_chats", {})
    rules = data.get("forward_rules", {})
    for cid, info in known.items():
        try:
            int_cid = int(cid)
        except:
            continue
        title = info.get("title") or f"Чат {cid}"
        cur_mode = rules.get(str(chat_id), {}).get(cid)
        if cur_mode == "oneway_to":
            label = f"{title} ➡️"
        elif cur_mode == "oneway_from":
            label = f"{title} ⬅️"
        elif cur_mode == "twoway":
            label = f"{title} ↔️"
        else:
            label = f"{title}"
        kb.row(
            types.InlineKeyboardButton(
                label,
                callback_data=f"d:{day_key}:fw_cfg_{cid}"
            )
        )
    kb.row(
        types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{day_key}:edit_menu")
    )
    return kb
def build_forward_direction_menu(day_key: str, owner_chat: int, target_chat: int):
    """
    Меню направлений:
        ➡️ owner → target
        ⬅️ target → owner
        ↔️ двусторонняя
        ❌ удалить
        🔙 назад
    """
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.row(
        types.InlineKeyboardButton(
            f"➡️ В одну сторону (от {owner_chat} → {target_chat})",
            callback_data=f"d:{day_key}:fw_one_{target_chat}"
        )
    )
    kb.row(
        types.InlineKeyboardButton(
            f"⬅️ В обратную ({target_chat} → {owner_chat})",
            callback_data=f"d:{day_key}:fw_rev_{target_chat}"
        )
    )
    kb.row(
        types.InlineKeyboardButton(
            "↔️ Двусторонняя пересылка",
            callback_data=f"d:{day_key}:fw_two_{target_chat}"
        )
    )
    kb.row(
        types.InlineKeyboardButton(
            "❌ Удалить все связи",
            callback_data=f"d:{day_key}:fw_del_{target_chat}"
        )
    )
    kb.row(
        types.InlineKeyboardButton(
            "🔙 Назад",
            callback_data=f"d:{day_key}:forward_menu"
        )
    )
    return kb
    #💰💰💰💰💰💰
def build_forward_source_menu():
    """
    Меню выбора чата A (источник пересылки).
    Использует known_chats владельца.
    """
    kb = types.InlineKeyboardMarkup()
    if not OWNER_ID:
        return kb
    owner_store = get_chat_store(int(OWNER_ID))
    known = owner_store.get("known_chats", {})
    for cid, ch in known.items():
        title = ch.get("title") or f"Чат {cid}"
        kb.row(
            types.InlineKeyboardButton(
                title,
                callback_data=f"fw_src:{cid}"
            )
        )
    kb.row(
        types.InlineKeyboardButton("🔙 Назад", callback_data="fw_back_root")
    )
    return kb
def build_forward_target_menu(src_id: int):
    """
    Меню выбора чата B (получатель пересылки) для уже выбранного A.
    """
    kb = types.InlineKeyboardMarkup()
    if not OWNER_ID:
        return kb
    owner_store = get_chat_store(int(OWNER_ID))
    known = owner_store.get("known_chats", {})
    for cid, ch in known.items():
        try:
            int_cid = int(cid)
        except Exception:
            continue
        if int_cid == src_id:
            continue
        title = ch.get("title") or f"Чат {cid}"
        kb.row(
            types.InlineKeyboardButton(
                title,
                callback_data=f"fw_tgt:{src_id}:{cid}"
            )
        )
    kb.row(
        types.InlineKeyboardButton("🔙 Назад", callback_data="fw_back_src")
    )
    return kb
def build_forward_mode_menu(A: int, B: int):
    """
    Меню выбора режима пересылки между чатами A и B:
        ➡️ A → B
        ⬅️ B → A
        ↔️ двусторонняя
        ❌ удалить связь
        🔙 назад (к выбору B)
    """
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton(
            f"➡️ {A} → {B}",
            callback_data=f"fw_mode:{A}:{B}:to"
        )
    )
    kb.row(
        types.InlineKeyboardButton(
            f"⬅️ {B} → {A}",
            callback_data=f"fw_mode:{A}:{B}:from"
        )
    )
    kb.row(
        types.InlineKeyboardButton(
            f"↔️ {A} ⇄ {B}",
            callback_data=f"fw_mode:{A}:{B}:two"
        )
    )
    kb.row(
        types.InlineKeyboardButton(
            "❌ Удалить связь A-B",
            callback_data=f"fw_mode:{A}:{B}:del"
        )
    )
    kb.row(
        types.InlineKeyboardButton(
            "🔙 Назад",
            callback_data=f"fw_back_tgt:{A}"
        )
    )
    return kb
def apply_forward_mode(A: int, B: int, mode: str):
    """
    Применяет выбранный режим пересылки между чатами A и B.
    Использует общие функции add_forward_link / remove_forward_link.
    """
    if mode == "to":
        add_forward_link(A, B, "oneway_to")
        remove_forward_link(B, A)
    elif mode == "from":
        add_forward_link(B, A, "oneway_to")
        remove_forward_link(A, B)
    elif mode == "two":
        add_forward_link(A, B, "twoway")
        add_forward_link(B, A, "twoway")
    elif mode == "del":
        remove_forward_link(A, B)
        remove_forward_link(B, A)

def safe_edit(bot, call, text, reply_markup=None, parse_mode=None):
    """Безопасное обновление: edit_text → edit_caption → send_message."""
    chat_id = call.message.chat.id
    msg_id = call.message.message_id
    try:
        bot.edit_message_text(
            text,
            chat_id=chat_id,
            message_id=msg_id,
            reply_markup=reply_markup,
            parse_mode=parse_mode
        )
        return
    except Exception:
        pass
    try:
        bot.edit_message_caption(
            chat_id=chat_id,
            message_id=msg_id,
            caption=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode
        )
        return
    except Exception:
        pass
    bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode)



def handle_categories_callback(call, data_str: str) -> bool:
    """UI: 12 месяцев → 4 недели → отчёт по статьям. Возвращает True если обработано."""
    chat_id = call.message.chat.id

    # Быстрый переход: текущая неделя (сегодня)
    if data_str == "cat_today":
        start = week_start_monday(today_key())
        return handle_categories_callback(call, f"cat_wk:{start}")

    # Навигация по неделям: start=понедельник недели (YYYY-MM-DD)
    if data_str.startswith("cat_wk:"):
        start = data_str.split(":", 1)[1].strip()
        if not start:
            start = week_start_monday(today_key())
        start, end = week_bounds_from_start(start)
        store = get_chat_store(chat_id)
        cats = calc_categories_for_period(store, start, end)

        lines = [
            "📦 Расходы по статьям",
            f"🗓 {fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)}",
            ""
        ]

        if not cats:
            lines.append("Нет данных по статьям за этот период.")
        else:
            keys = list(cats.keys())
            if "ПРОДУКТЫ" in keys:
                keys.remove("ПРОДУКТЫ")
                keys = ["ПРОДУКТЫ"] + sorted(keys)
            else:
                keys = sorted(keys)

            for cat in keys:
                lines.append(f"{cat}: −{fmt_num(cats[cat])}")
                if cat == "ПРОДУКТЫ":
                    items = collect_items_for_category(store, start, end, "ПРОДУКТЫ")
                    if items:
                        for day_i, amt_i, note_i in items:
                            note_i = (note_i or "").strip()
                            lines.append(f"  • {fmt_date_ddmmyy(day_i)}: −{fmt_num(amt_i)} {note_i}")

        kb = types.InlineKeyboardMarkup()
        try:
            prev_start = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
            next_start = (datetime.strptime(start, "%Y-%m-%d") + timedelta(days=7)).strftime("%Y-%m-%d")
        except Exception:
            prev_start = start
            next_start = start
        kb.row(
            types.InlineKeyboardButton("⬅️ Неделя", callback_data=f"cat_wk:{prev_start}"),
            types.InlineKeyboardButton("📅 Сегодня", callback_data="cat_today"),
            types.InlineKeyboardButton("Неделя ➡️", callback_data=f"cat_wk:{next_start}")
        )
        kb.row(types.InlineKeyboardButton("📆 Выбор недели", callback_data="cat_months"))
        safe_edit(bot, call, "\n".join(lines), reply_markup=kb)
        return True

    if data_str == "cat_months":
        kb = types.InlineKeyboardMarkup(row_width=3)
        # 12 месяцев
        for m in range(1, 13):
            kb.add(types.InlineKeyboardButton(
                datetime(2000, m, 1).strftime("%b"),
                callback_data=f"cat_m:{m}"
            ))
        safe_edit(bot, call, "📦 Выберите месяц:", reply_markup=kb)
        return True

    if data_str.startswith("cat_m:"):
        try:
            month = int(data_str.split(":")[1])
        except Exception:
            return True
        year = now_local().year

        # 4 недели месяца (простая разметка 1–7, 8–14, 15–21, 22–31)
        kb = types.InlineKeyboardMarkup(row_width=2)
        weeks = [(1, 7), (8, 14), (15, 21), (22, 31)]
        for a, b in weeks:
            kb.add(types.InlineKeyboardButton(
                f"{a:02d}–{b:02d}",
                callback_data=f"cat_w:{year}:{month}:{a}:{b}"
            ))
        kb.row(
            types.InlineKeyboardButton("📅 Сегодня", callback_data="cat_today"),
            types.InlineKeyboardButton("🔙 Назад", callback_data="cat_months")
        )
        safe_edit(bot, call, "📆 Выберите неделю:", reply_markup=kb)
        return True

    if data_str.startswith("cat_w:"):
        try:
            _, y, m, a, b = data_str.split(":")
            y, m, a, b = map(int, (y, m, a, b))
        except Exception:
            return True

        # нормализация конца месяца (если месяц короче 31)
        try:
            # последний день месяца: первый день следующего месяца - 1 день
            if m == 12:
                last_day = (datetime(y + 1, 1, 1) - timedelta(days=1)).day
            else:
                last_day = (datetime(y, m + 1, 1) - timedelta(days=1)).day
        except Exception:
            last_day = 31

        a = max(1, min(a, last_day))
        b = max(1, min(b, last_day))
        if b < a:
            b = a

        start = f"{y}-{m:02d}-{a:02d}"
        end = f"{y}-{m:02d}-{b:02d}"

        store = get_chat_store(chat_id)
        cats = calc_categories_for_period(store, start, end)

        lines = [
            "📦 Расходы по статьям",
            f"🗓 {fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)}",
            ""
        ]

        if not cats:
            lines.append("Нет данных по статьям за этот период.")
        else:
            # Стабильно: сначала ПРОДУКТЫ, затем остальные по алфавиту
            keys = list(cats.keys())
            if "ПРОДУКТЫ" in keys:
                keys.remove("ПРОДУКТЫ")
                keys = ["ПРОДУКТЫ"] + sorted(keys)
            else:
                keys = sorted(keys)

            for cat in keys:
                lines.append(f"{cat}: −{fmt_num(cats[cat])}")

                if cat == "ПРОДУКТЫ":
                    items = collect_items_for_category(store, start, end, "ПРОДУКТЫ")
                    if items:
                        for day_i, amt_i, note_i in items:
                            note_i = (note_i or "").strip()
                            lines.append(f"  • {fmt_date_ddmmyy(day_i)}: −{fmt_num(amt_i)} {note_i}")
                    else:
                        lines.append("  • нет операций")

        kb = types.InlineKeyboardMarkup()
        kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data=f"cat_m:{m}"))
        safe_edit(bot, call, "\n".join(lines), reply_markup=kb)
        return True

    return False
#🟡🟡🟡🟡🟡
@bot.callback_query_handler(func=lambda c: True)
def on_callback(call):
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass

    try:
        data_str = call.data or ""
        chat_id = call.message.chat.id

        try:
            update_chat_info_from_message(call.message)
        except Exception:
            pass

        if data_str == "cat_months" or data_str.startswith("cat_"):
            if handle_categories_callback(call, data_str):
                return

        if data_str.startswith("fw_"):
            if not OWNER_ID or str(chat_id) != str(OWNER_ID):
                try:
                    bot.answer_callback_query(
                        call.id,
                        "Меню пересылки доступно только владельцу.",
                        show_alert=True
                    )
                except Exception:
                    pass
                return
            if data_str == "fw_open":
                kb = build_forward_source_menu()
                safe_edit(
                    bot,
                    call,
                    "Выберите чат A:",
                    reply_markup=kb
                )
                return
            if data_str == "fw_back_root":
                owner_store = get_chat_store(int(OWNER_ID))
                day_key = owner_store.get("current_view_day", today_key())
                kb = build_edit_menu_keyboard(day_key, chat_id)
                safe_edit(
                    bot,
                    call,
                    f"Меню редактирования для {day_key}:",
                    reply_markup=kb
                )
                return
            if data_str == "fw_back_src":
                kb = build_forward_source_menu()
                safe_edit(
                    bot,
                    call,
                    "Выберите чат A:",
                    reply_markup=kb
                )
                return
            if data_str.startswith("fw_back_tgt:"):
                try:
                    A = int(data_str.split(":", 1)[1])
                except Exception:
                    return
                kb = build_forward_target_menu(A)
                safe_edit(
                    bot,
                    call,
                    f"Источник пересылки: {A}\nВыберите чат B:",
                    reply_markup=kb
                )
                return
            if data_str.startswith("fw_src:"):
                try:
                    A = int(data_str.split(":", 1)[1])
                except Exception:
                    return
                kb = build_forward_target_menu(A)
                safe_edit(
                    bot,
                    call,
                    f"Источник пересылки: {A}\nВыберите чат B:",
                    reply_markup=kb
                )
                return
            if data_str.startswith("fw_tgt:"):
                parts = data_str.split(":")
                if len(parts) != 3:
                    return
                _, A_str, B_str = parts
                try:
                    A = int(A_str)
                    B = int(B_str)
                except Exception:
                    return
                kb = build_forward_mode_menu(A, B)
                safe_edit(
                    bot,
                    call,
                    f"Настройка пересылки: {A} ⇄ {B}",
                    reply_markup=kb
                )
                return
            if data_str.startswith("fw_mode:"):
                parts = data_str.split(":")
                if len(parts) != 4:
                    return
                _, A_str, B_str, mode = parts
                try:
                    A = int(A_str)
                    B = int(B_str)
                except Exception:
                    return
                apply_forward_mode(A, B, mode)
                kb = build_forward_source_menu()
                safe_edit(
                    bot,
                    call,
                    "Маршрут обновлён.\nВыберите чат A:",
                    reply_markup=kb
                )
                return
            return
        if data_str.startswith("c:"):
            center = data_str[2:]
            try:
                center_dt = datetime.strptime(center, "%Y-%m-%d")
            except ValueError:
                return
            kb = build_calendar_keyboard(center_dt, chat_id)
            try:
                bot.edit_message_reply_markup(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
            except Exception:
                pass
            return
        if not data_str.startswith("d:"):
            return
        _, day_key, cmd = data_str.split(":", 2)
        store = get_chat_store(chat_id)
        if cmd == "open":
            store["current_view_day"] = day_key
            if OWNER_ID and str(chat_id) == str(OWNER_ID):
                backup_window_for_owner(chat_id, day_key, None)
            else:
                txt, _ = render_day_window(chat_id, day_key)
                kb = build_main_keyboard(day_key, chat_id)
                safe_edit(bot, call, txt, reply_markup=kb, parse_mode="HTML")
                set_active_window_id(chat_id, day_key, call.message.message_id)
            return
        if cmd == "prev":
            d = datetime.strptime(day_key, "%Y-%m-%d") - timedelta(days=1)
            nd = d.strftime("%Y-%m-%d")
            store["current_view_day"] = nd
            if OWNER_ID and str(chat_id) == str(OWNER_ID):
                backup_window_for_owner(chat_id, nd, call.message.message_id)
            else:
                txt, _ = render_day_window(chat_id, nd)
                kb = build_main_keyboard(nd, chat_id)
                safe_edit(bot, call, txt, reply_markup=kb, parse_mode="HTML")
                set_active_window_id(chat_id, nd, call.message.message_id)
            return
        if cmd == "next":
            d = datetime.strptime(day_key, "%Y-%m-%d") + timedelta(days=1)
            nd = d.strftime("%Y-%m-%d")
            store["current_view_day"] = nd
            if OWNER_ID and str(chat_id) == str(OWNER_ID):
                backup_window_for_owner(chat_id, nd, call.message.message_id)
            else:
                txt, _ = render_day_window(chat_id, nd)
                kb = build_main_keyboard(nd, chat_id)
                safe_edit(bot, call, txt, reply_markup=kb, parse_mode="HTML")
                set_active_window_id(chat_id, nd, call.message.message_id)
            return
        if cmd == "today":
            nd = today_key()
            store["current_view_day"] = nd
            if OWNER_ID and str(chat_id) == str(OWNER_ID):
                backup_window_for_owner(chat_id, nd, call.message.message_id)
            else:
                txt, _ = render_day_window(chat_id, nd)
                kb = build_main_keyboard(nd, chat_id)
                safe_edit(bot, call, txt, reply_markup=kb, parse_mode="HTML")
                set_active_window_id(chat_id, nd, call.message.message_id)
            return
        if cmd == "calendar":
            try:
                cdt = datetime.strptime(day_key, "%Y-%m-%d")
            except Exception:
                cdt = now_local()
            kb = build_calendar_keyboard(cdt, chat_id)
            bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb
            )
            return
        if cmd == "report":
            lines = ["📊 Отчёт:"]
            for dk, recs in sorted(store.get("daily_records", {}).items()):
                s = sum(r["amount"] for r in recs)
                lines.append(f"{dk}: {fmt_num(s)}")
            bot.send_message(chat_id, "\n".join(lines))
            return
        if cmd == "total":
            chat_bal = store.get("balance", 0)
            total_msg_id = store.get("total_msg_id")

            # Обычный чат (не владелец)
            if not OWNER_ID or str(chat_id) != str(OWNER_ID):
                text = f"💰 Общий итог по этому чату: {fmt_num(chat_bal)}"
                if total_msg_id:
                    try:
                        bot.edit_message_text(
                            text,
                            chat_id=chat_id,
                            message_id=total_msg_id,
                            parse_mode="HTML"
                        )
                        save_data(data)
                        return
                    except Exception as e:
                        log_error(f"total: edit total_msg_id for chat {chat_id} failed: {e}")
                sent = bot.send_message(chat_id, text, parse_mode="HTML")
                store["total_msg_id"] = sent.message_id
                save_data(data)
                return

            # Владелец — общий итог по всем чатам
            lines = []
            info = store.get("info", {})
            title = info.get("title") or f"Чат {chat_id}"
            lines.append("💰 Общий итог (для владельца)")
            lines.append("")
            lines.append(f"• Этот чат ({title}): {fmt_num(chat_bal)}")

            all_chats = data.get("chats", {})
            total_all = 0
            other_lines = []
            for cid, st in all_chats.items():
                try:
                    cid_int = int(cid)
                except Exception:
                    continue
                bal = st.get("balance", 0)
                total_all += bal
                if cid_int == chat_id:
                    continue
                info2 = st.get("info", {})
                title2 = info2.get("title") or f"Чат {cid_int}"
                other_lines.append(f"   • {title2}: {fmt_num(bal)}")
            if other_lines:
                lines.append("")
                lines.append("• Другие чаты:")
                lines.extend(other_lines)
            lines.append("")
            lines.append(f"• Всего по всем чатам: {fmt_num(total_all)}")

            text = "\n".join(lines)
            if total_msg_id:
                try:
                    bot.edit_message_text(
                        text,
                        chat_id=chat_id,
                        message_id=total_msg_id,
                        parse_mode="HTML"
                    )
                    save_data(data)
                    return
                except Exception as e:
                    log_error(f"total(owner): edit total_msg_id for chat {chat_id} failed: {e}")
            sent = bot.send_message(chat_id, text, parse_mode="HTML")
            store["total_msg_id"] = sent.message_id
            save_data(data)
            return
        if cmd == "info":
            try:
                bot.answer_callback_query(call.id)
            except Exception:
                pass
            info_text = (
                f"ℹ️ Финансовый бот — версия {VERSION}\n\n"
                "Команды:\n"
                "/ok, /поехали — включить финансовый режим\n"
                "/start — окно сегодняшнего дня\n"
                "/view YYYY-MM-DD — открыть конкретный день\n"
                "/prev — предыдущий день\n"
                "/next — следующий день\n"
                "/balance — баланс по этому чату\n"
                "/report — краткий отчёт по дням\n"
                "/csv — CSV этого чата\n"
                "/json — JSON этого чата\n"
                "/reset — обнулить данные чата (с подтверждением)\n"
                "/stopforward — отключить пересылку\n"
                "/ping — проверка, жив ли бот\n"
                "/backup_gdrive_on / _off — включить/выключить GDrive\n"
                "/backup_channel_on / _off — включить/выключить бэкап в канал\n"
                "/restore / /restore_off — режим восстановления JSON/CSV\n"
                "/autoadd_info — режим авто-добавления по суммам\n"
                "/help — эта справка\n"
            )
            kb = types.InlineKeyboardMarkup()
            kb.row(types.InlineKeyboardButton("📦 Расходы по статьям", callback_data="cat_months"))
            bot.send_message(chat_id, info_text, reply_markup=kb)
            return
        if cmd in ("edit_menu", "menu"):
            # open edit/menu

            store["current_view_day"] = day_key
            kb = build_edit_menu_keyboard(day_key, chat_id)
            cur_text = getattr(call.message, "caption", None) or getattr(call.message, "text", None) or ""
            safe_edit(bot, call, cur_text, reply_markup=kb, parse_mode="HTML")
            return
        if cmd == "back_main":
            store["current_view_day"] = day_key
            if OWNER_ID and str(chat_id) == str(OWNER_ID):
                backup_window_for_owner(chat_id, day_key, None)
            else:
                txt, _ = render_day_window(chat_id, day_key)
                kb = build_main_keyboard(day_key, chat_id)
                safe_edit(bot, call, txt, reply_markup=kb, parse_mode="HTML")
            return
        if cmd == "csv_all":
            cmd_csv_all(chat_id)
            return
        if cmd == "csv_day":
            cmd_csv_day(chat_id, day_key)
            return
        if cmd == "reset":
            if not require_finance(chat_id):
                return
            store["reset_wait"] = True
            store["reset_time"] = time.time()
            save_data(data)
            send_info(chat_id, "Вы уверены, что хотите обнулить данные? Напишите ДА.")
            return
        if cmd == "add":
            store["edit_wait"] = {"type": "add", "day_key": day_key}
            save_data(data)
            send_and_auto_delete(
                chat_id,
                "Введите сумму и комментарий (пример: +500 кафе)",
                15
            )
            schedule_cancel_wait(chat_id, 15)
            return
        if cmd == "edit_list":
            day_recs = store.get("daily_records", {}).get(day_key, [])
            if not day_recs:
                send_and_auto_delete(chat_id, "Нет записей за этот день.")
                return
            kb2 = types.InlineKeyboardMarkup(row_width=3)
            for r in day_recs:
                lbl = f"{r['short_id']} {fmt_num(r['amount'])} — {r.get('note','')}"
                rid = r["id"]
                kb2.row(
                    types.InlineKeyboardButton(lbl, callback_data="none"),
                    types.InlineKeyboardButton("✏️", callback_data=f"d:{day_key}:edit_rec_{rid}"),
                    types.InlineKeyboardButton("❌", callback_data=f"d:{day_key}:del_rec_{rid}")
                )
            kb2.row(
                types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{day_key}:edit_menu")
            )
            safe_edit(
                bot,
                call,
                "Выберите действие:",
                reply_markup=kb2,
                #parse_mode="HTML"
            )
            return
            
        if cmd.startswith("edit_rec_"):
            rid = int(cmd.split("_")[-1])

            # найдём запись
            store = get_chat_store(chat_id)
            rec = next((r for r in store.get("records", []) if r["id"] == rid), None)
            if not rec:
                send_and_auto_delete(chat_id, "❌ Запись не найдена.")
                return

            store["edit_wait"] = {
                "type": "edit",
                "rid": rid,
                "day_key": day_key,
            }
            save_data(data)

            # текст отдельного окна
            text = (
                f"✏️ Редактирование записи R{rid}\n\n"
                f"Текущие данные:\n"
                f"{fmt_num(rec['amount'])} {rec.get('note','')}\n\n"
                f"✍️ Напишите новые данные.\n\n"
                f"⏳ Это сообщение будет удалено через 30 секунд,\n"
                f"если изменений не будет — редактирование отменится."
            )

            kb = build_cancel_edit_keyboard(day_key)

            sent = bot.send_message(
                chat_id,
                text,
                reply_markup=kb
            )

            # авто-отмена через 30 сек
            schedule_cancel_edit(chat_id, sent.message_id, delay=30)

            return
        if cmd.startswith("del_rec_"):
            rid = int(cmd.split("_")[-1])
            delete_record_in_chat(chat_id, rid)
            update_or_send_day_window(chat_id, day_key)
            refresh_total_message_if_any(chat_id)
            if OWNER_ID and str(chat_id) != str(OWNER_ID):
                try:
                    refresh_total_message_if_any(int(OWNER_ID))
                except Exception:
                    pass
            send_and_auto_delete(chat_id, f"🗑 Запись R{rid} удалена.", 10)
            return
        if cmd == "forward_menu":
            if not OWNER_ID or str(chat_id) != str(OWNER_ID):
                bot.send_message(chat_id, "Меню доступно только владельцу.")
                return
            kb = types.InlineKeyboardMarkup(row_width=1)
            kb.row(
                types.InlineKeyboardButton(
                    "📨 По чатам (старый режим)",
                    callback_data=f"d:{day_key}:forward_old"
                )
            )
            kb.row(
                types.InlineKeyboardButton(
                    "🔀 Пары A ↔ B",
                    callback_data="fw_open"
                )
            )
            kb.row(
                types.InlineKeyboardButton(
                    "🔙 Назад",
                    callback_data=f"d:{day_key}:edit_menu"
                )
            )
            safe_edit(
                bot,
                call,
                "Меню пересылки:\nВыберите режим:",
                reply_markup=kb
            )
            return
        if cmd == "forward_old":
            if not OWNER_ID or str(chat_id) != str(OWNER_ID):
                bot.send_message(chat_id, "Меню доступно только владельцу.")
                return
            kb = build_forward_chat_list(day_key, chat_id)
            safe_edit(
                bot,
                call,
                "Выберите чат, для которого хотите настроить пересылку:",
                reply_markup=kb
            )
            return
        if cmd.startswith("fw_cfg_"):
            tgt = int(cmd.split("_")[-1])
            kb = build_forward_direction_menu(day_key, chat_id, tgt)
            safe_edit(
                bot,
                call,
                f"Настройка пересылки для чата {tgt}:",
                reply_markup=kb
            )
            return
        if cmd.startswith("fw_one_"):
            tgt = int(cmd.split("_")[-1])
            add_forward_link(chat_id, tgt, "oneway_to")
            send_and_auto_delete(chat_id, f"Установлена пересылка ➡️  {chat_id} → {tgt}")
            return
        if cmd.startswith("fw_rev_"):
            tgt = int(cmd.split("_")[-1])
            add_forward_link(tgt, chat_id, "oneway_to")
            add_forward_link(chat_id, tgt, "oneway_from")
            send_and_auto_delete(chat_id, f"Установлена пересылка ⬅️  {tgt} → {chat_id}")
            return
        if cmd.startswith("fw_two_"):
            tgt = int(cmd.split("_")[-1])
            add_forward_link(chat_id, tgt, "twoway")
            add_forward_link(tgt, chat_id, "twoway")
            send_and_auto_delete(chat_id, f"Установлена двусторонняя пересылка ↔️  {chat_id} ⇄ {tgt}")
            return
        if cmd.startswith("fw_del_"):
            tgt = int(cmd.split("_")[-1])
            remove_forward_link(chat_id, tgt)
            remove_forward_link(tgt, chat_id)
            send_and_auto_delete(chat_id, f"Все связи с {tgt} удалены.")
            return
        if cmd == "pick_date":
            bot.send_message(chat_id, "Введите дату:\n/view YYYY-MM-DD")
            return
        if cmd == "cancel_edit":
            store = get_chat_store(chat_id)
            store["edit_wait"] = None
            save_data(data)

            try:
                bot.delete_message(chat_id, call.message.message_id)
            except Exception:
                pass

            send_and_auto_delete(chat_id, "❎ Редактирование отменено.", 5)
            return
    except Exception as e:
        log_error(f"on_callback error: {e}")
        #🐳🐳🐳🐳🐳🐳🐳🐳
def add_record_to_chat(
    chat_id: int,
    amount: float,
    note: str,
    owner: int,
    source_msg=None
):
    store = get_chat_store(chat_id)
    rid = store.get("next_id", 1)

    rec = {
        "id": rid,
        "short_id": f"R{rid}",
        "timestamp": now_local().isoformat(timespec="seconds"),
        "amount": amount,
        "note": note,
        "source_msg_id": source_msg.message_id if source_msg else None,
        #"source_msg_id": msg.message_id,
        "owner": owner,
        "msg_id": source_msg.message_id if source_msg else None,
        "origin_msg_id": source_msg.message_id if source_msg else None,
    }

    store.setdefault("records", []).append(rec)
    store.setdefault("daily_records", {}).setdefault(today_key(), []).append(rec)

    store["next_id"] = rid + 1
    store["balance"] = sum(r["amount"] for r in store["records"])

    data.setdefault("records", []).append(rec)
    data["overall_balance"] = sum(r["amount"] for r in data["records"])

    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)
    send_backup_to_channel(chat_id)
    
def update_record_in_chat(chat_id: int, rid: int, new_amount: float, new_note: str, skip_chat_backup: bool = False):
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

    for day, arr in store.get("daily_records", {}).items():
        for r in arr:
            if r["id"] == rid:
                r.update(found)

    store["balance"] = sum(x["amount"] for x in store["records"])
    data["records"] = [x if x["id"] != rid else found for x in data["records"]]
    data["overall_balance"] = sum(x["amount"] for x in data["records"])

    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)
    send_backup_to_channel(chat_id)

    if not skip_chat_backup:
        send_backup_to_chat(chat_id)
        
def delete_record_in_chat(chat_id: int, rid: int):
    store = get_chat_store(chat_id)
    store["records"] = [x for x in store["records"] if x["id"] != rid]
    for day, arr in list(store.get("daily_records", {}).items()):
        arr2 = [x for x in arr if x["id"] != rid]
        if arr2:
            store["daily_records"][day] = arr2
        else:
            del store["daily_records"][day]
    renumber_chat_records(chat_id)
    store["balance"] = sum(x["amount"] for x in store["records"])
    data["records"] = [x for x in data["records"] if x["id"] != rid]
    data["overall_balance"] = sum(x["amount"] for x in data["records"])
    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)
    send_backup_to_channel(chat_id)
    send_backup_to_chat(chat_id)
def renumber_chat_records(chat_id: int):
    """
    Перенумеровывает записи в чате по реальному порядку:
      • сортируем по day_key и timestamp
      • присваиваем ID: 1,2,3... и short_id: R1,R2,...
      • обновляем store["records"] и next_id
    """
    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {})
    all_recs = []
    for dk in sorted(daily.keys()):
        recs = daily.get(dk, [])
        recs_sorted = sorted(recs, key=lambda r: r.get("timestamp", ""))
        daily[dk] = recs_sorted
        for r in recs_sorted:
            all_recs.append(r)
    new_id = 1
    for r in all_recs:
        r["id"] = new_id
        r["short_id"] = f"R{new_id}"
        new_id += 1
    store["records"] = list(all_recs)
    store["next_id"] = new_id
def get_or_create_active_windows(chat_id: int) -> dict:
    return data.setdefault("active_messages", {}).setdefault(str(chat_id), {})
def set_active_window_id(chat_id: int, day_key: str, message_id: int):
    aw = get_or_create_active_windows(chat_id)
    aw[day_key] = message_id
    save_data(data)
def get_active_window_id(chat_id: int, day_key: str):
    aw = get_or_create_active_windows(chat_id)
    return aw.get(day_key)
def delete_active_window_if_exists(chat_id: int, day_key: str):
    mid = message_id_override or get_active_window_id(chat_id, day_key)
    if not mid:
        return
    try:
        bot.delete_message(chat_id, mid)
    except:
        pass
    aw = get_or_create_active_windows(chat_id)
    if day_key in aw:
        del aw[day_key]
    save_data(data)
def update_or_send_day_window(chat_id: int, day_key: str):
    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        backup_window_for_owner(chat_id, day_key)
        return

    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)
    mid = get_active_window_id(chat_id, day_key)
    if mid:
        try:
            bot.edit_message_text(
                txt,
                chat_id=chat_id,
                message_id=mid,
                reply_markup=kb,
                parse_mode="HTML"
            )
            return
        except:
            pass
    sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
    set_active_window_id(chat_id, day_key, sent.message_id)
def is_finance_mode(chat_id: int) -> bool:
    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        return True
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
        send_and_auto_delete(chat_id, "⚙️ Финансовый режим выключен.\nАктивируйте командой /поехали")
        return False
    return True
def refresh_total_message_if_any(chat_id: int):
    """
    Если в чате есть активное сообщение '💰 Общий итог',
    пересчитывает и обновляет его текст.
    """
    store = get_chat_store(chat_id)
    msg_id = store.get("total_msg_id")
    if not msg_id:
        return
    try:
        chat_bal = store.get("balance", 0)
        if not OWNER_ID or str(chat_id) != str(OWNER_ID):
            text = f"💰 Общий итог по этому чату: {fmt_num(chat_bal)}"
        else:
            lines = []
            info = store.get("info", {})
            title = info.get("title") or f"Чат {chat_id}"
            lines.append("💰 Общий итог (для владельца)")
            lines.append("")
            lines.append(f"• Этот чат ({title}): {fmt_num(chat_bal)}")
            all_chats = data.get("chats", {})
            total_all = 0
            other_lines = []
            for cid, st in all_chats.items():
                try:
                    cid_int = int(cid)
                except Exception:
                    continue
                bal = st.get("balance", 0)
                total_all += bal
                if cid_int == chat_id:
                    continue
                info2 = st.get("info", {})
                title2 = info2.get("title") or f"Чат {cid_int}"
                other_lines.append(f"   • {title2}: {fmt_num(bal)}")
            if other_lines:
                lines.append("")
                lines.append("• Другие чаты:")
                lines.extend(other_lines)
            lines.append("")
            lines.append(f"• Всего по всем чатам: {fmt_num(total_all)}")
            text = "\n".join(lines)
        bot.edit_message_text(
            text,
            chat_id=chat_id,
            message_id=msg_id,
            parse_mode="HTML"
        )
    except Exception as e:
        log_error(f"refresh_total_message_if_any({chat_id}): {e}")
        store["total_msg_id"] = None
        save_data(data)
def send_info(chat_id: int, text: str):
    send_and_auto_delete(chat_id, text, 10)
                
@bot.message_handler(commands=["ok"])
def cmd_enable_finance(msg):
    chat_id = msg.chat.id
    # ❌ OWNER — команда не нужна
    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        delete_message_later(chat_id, msg.message_id, 5)
        send_and_auto_delete(
            chat_id,
            "ℹ️ Для владельца финансовый режим и авто-добавление включены всегда.",
            10
        )
        return
    delete_message_later(chat_id, msg.message_id, 15)

    set_finance_mode(chat_id, True)
    save_data(data)

    send_info(chat_id, "🚀 Финансовый режим включён!\nОтправьте /start")
@bot.message_handler(commands=["start"])
def cmd_start(msg):
    chat_id = msg.chat.id
    delete_message_later(chat_id, msg.message_id, 15)
    if not require_finance(chat_id):
        return
    day_key = today_key()
    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        backup_window_for_owner(chat_id, day_key, None)
    else:
        txt, _ = render_day_window(chat_id, day_key)
        kb = build_main_keyboard(day_key, chat_id)
        sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
        set_active_window_id(chat_id, day_key, sent.message_id)
        
@bot.message_handler(commands=["help"])
def cmd_help(msg):
    chat_id = msg.chat.id
    delete_message_later(chat_id, msg.message_id, 15)
    if not is_finance_mode(chat_id):
        send_info(chat_id, "ℹ️ Финансовый режим выключен")
        return
    help_text = (
        f"ℹ️ Финансовый бот — версия {VERSION}\n\n"
        "Команды:\n"
        "/ok, /поехали — включить финансовый режим\n"
        "/start — окно сегодняшнего дня\n"
        "/view YYYY-MM-DD — открыть конкретный день\n"
        "/prev — предыдущий день\n"
        "/next — следующий день\n"
        "/balance — баланс по этому чату\n"
        "/report — краткий отчёт по дням\n"
        "/csv — CSV этого чата\n"
        "/json — JSON этого чата\n"
        "/reset — обнулить данные чата (с подтверждением)\n"
        "/stopforward — отключить пересылку\n"
        "/ping — проверка, жив ли бот\n"
        "/backup_gdrive_on / _off — включить/выключить GDrive\n"
        "/backup_channel_on / _off — включить/выключить бэкап в канал\n"
        "/restore / /restore_off — режим восстановления JSON/CSV\n"
        "/autoadd_info — режим авто-добавления по суммам\n"
        "/help — эта справка\n"
    )
    send_info(chat_id, help_text)
    
@bot.message_handler(commands=["restore"])
def cmd_restore(msg):
    global restore_mode
    restore_mode = msg.chat.id  # включаем только для текущего чата
    cleanup_forward_links(msg.chat.id)
    send_and_auto_delete(
        msg.chat.id,
        "📥 Режим восстановления включён.\n"
        "Отправьте JSON/CSV файл для восстановления."
    )
    
@bot.message_handler(commands=["restore_off"])
def cmd_restore_off(msg):
    global restore_mode
    restore_mode = None  # выключаем
    cleanup_forward_links(msg.chat.id)
    send_and_auto_delete(msg.chat.id, "🔒 Режим восстановления выключен.")
@bot.message_handler(commands=["ping"])
def cmd_ping(msg):
    send_info(msg.chat.id, "PONG — бот работает 🟢")
@bot.message_handler(commands=["view"])
def cmd_view(msg):
    chat_id = msg.chat.id
    delete_message_later(chat_id, msg.message_id, 15)
    if not require_finance(chat_id):
        return
    parts = (msg.text or "").split()
    if len(parts) < 2:
        send_info(chat_id, "Использование: /view YYYY-MM-DD")
        return
    day_key = parts[1]
    try:
        datetime.strptime(day_key, "%Y-%m-%d")
    except ValueError:
        send_info(chat_id, "❌ Неверная дата. Формат: YYYY-MM-DD")
        return
    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        backup_window_for_owner(chat_id, day_key, None)
    else:
        txt, _ = render_day_window(chat_id, day_key)
        kb = build_main_keyboard(day_key, chat_id)
        sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
        set_active_window_id(chat_id, day_key, sent.message_id)
@bot.message_handler(commands=["prev"])
def cmd_prev(msg):
    chat_id = msg.chat.id
    delete_message_later(chat_id, msg.message_id, 15)
    if not require_finance(chat_id):
        return
    d = datetime.strptime(today_key(), "%Y-%m-%d") - timedelta(days=1)
    day_key = d.strftime("%Y-%m-%d")
    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        backup_window_for_owner(chat_id, day_key, None)
    else:
        txt, _ = render_day_window(chat_id, day_key)
        kb = build_main_keyboard(day_key, chat_id)
        sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
        set_active_window_id(chat_id, day_key, sent.message_id)
@bot.message_handler(commands=["next"])
def cmd_next(msg):
    chat_id = msg.chat.id
    delete_message_later(chat_id, msg.message_id, 15)
    if not require_finance(chat_id):
        return
    d = datetime.strptime(today_key(), "%Y-%m-%d") + timedelta(days=1)
    day_key = d.strftime("%Y-%m-%d")
    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        backup_window_for_owner(chat_id, day_key, None)
    else:
        txt, _ = render_day_window(chat_id, day_key)
        kb = build_main_keyboard(day_key, chat_id)
        sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
        set_active_window_id(chat_id, day_key, sent.message_id)
        #❗️❗️❗️❗️❗️❗️❗️❗️
@bot.message_handler(commands=["balance"])
def cmd_balance(msg):
    chat_id = msg.chat.id
    delete_message_later(chat_id, msg.message_id, 15)
    if not require_finance(chat_id):
        return
    store = get_chat_store(chat_id)
    bal = store.get("balance", 0)
    send_info(chat_id, f"💰 Баланс: {fmt_num(bal)}")
@bot.message_handler(commands=["report"])
def cmd_report(msg):
    chat_id = msg.chat.id
    delete_message_later(chat_id, msg.message_id, 15)
    if not require_finance(chat_id):
        return
    store = get_chat_store(chat_id)
    lines = ["📊 Отчёт:"]
    for dk, recs in sorted(store.get("daily_records", {}).items()):
        day_sum = sum(r["amount"] for r in recs)
        lines.append(f"{dk}: {fmt_num(day_sum)}")
    send_info(chat_id, "\n".join(lines))
def cmd_csv_all(chat_id: int):
    """
    Общий CSV этого чата (все дни этого чата).
    """
    if not require_finance(chat_id):
        return
    try:
        save_chat_json(chat_id)
        path = chat_csv_file(chat_id)
        if not os.path.exists(path):
            send_info(chat_id, "CSV файла ещё нет.")
            return
        with open(path, "rb") as f:
            bot.send_document(
                chat_id,
                f,
                caption=f"📂 Общий CSV всех операций чата {chat_id}"
            )
    except Exception as e:
        log_error(f"cmd_csv_all: {e}")
def cmd_csv_day(chat_id: int, day_key: str):
    """
    CSV только за один день для текущего чата.
    """
    if not require_finance(chat_id):
        return
    store = get_chat_store(chat_id)
    day_recs = store.get("daily_records", {}).get(day_key, [])
    if not day_recs:
        send_info(chat_id, "Нет записей за этот день.")
        return
    tmp_name = f"data_{chat_id}_{day_key}.csv"
    try:
        with open(tmp_name, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["chat_id", "ID", "short_id", "timestamp", "amount", "note", "owner", "day_key"])
            for r in day_recs:
                w.writerow([
                    chat_id,
                    r.get("id"),
                    r.get("short_id"),
                    r.get("timestamp"),
                    r.get("amount"),
                    r.get("note"),
                    r.get("owner"),
                    day_key,
                ])
        upload_to_gdrive(tmp_name)
        with open(tmp_name, "rb") as f:
            bot.send_document(chat_id, f, caption=f"📅 CSV за день {day_key}")
    except Exception as e:
        log_error(f"cmd_csv_day: {e}")
    finally:
        try:
            os.remove(tmp_name)
        except FileNotFoundError:
            pass
@bot.message_handler(commands=["csv"])
def cmd_csv(msg):
    """
    Экспортирует CSV текущего чата.
    """
    chat_id = msg.chat.id
    delete_message_later(chat_id, msg.message_id, 15)
    if not require_finance(chat_id):
        return
    export_global_csv(data)
    save_chat_json(chat_id)
    per_csv = chat_csv_file(chat_id)
    sent = None
    if os.path.exists(per_csv):
        upload_to_gdrive(per_csv)
        with open(per_csv, "rb") as f:
            sent = bot.send_document(chat_id, f, caption="📂 CSV этого чата")
    if OWNER_ID and chat_id == int(OWNER_ID):
        meta = _load_csv_meta()
        if sent and getattr(sent, "document", None):
            meta["file_id_csv"] = sent.document.file_id
        meta["message_id_csv"] = getattr(sent, "message_id", meta.get("message_id_csv"))
        _save_csv_meta(meta)
    send_backup_to_channel(chat_id)
@bot.message_handler(commands=["json"])
def cmd_json(msg):
    chat_id = msg.chat.id
    delete_message_later(chat_id, msg.message_id, 15)
    if not require_finance(chat_id):
        return
    save_chat_json(chat_id)
    p = chat_json_file(chat_id)
    if os.path.exists(p):
        with open(p, "rb") as f:
            bot.send_document(chat_id, f, caption="🧾 JSON этого чата")
    else:
        send_info(chat_id, "Файл JSON ещё не создан.")
@bot.message_handler(commands=["reset"])
def cmd_reset(msg):
    chat_id = msg.chat.id
    cleanup_forward_links(chat_id)  # 🔥 ВАЖНО
    if not require_finance(chat_id):
        return
    store = get_chat_store(chat_id)
    store["reset_wait"] = True
    store["reset_time"] = time.time()
    save_data(data)
    send_and_auto_delete(
        chat_id,
        "⚠️ Вы уверены, что хотите обнулить данные? Напишите ДА в течение 15 секунд.",
        15
    )
    schedule_cancel_wait(chat_id, 15)
@bot.message_handler(commands=["stopforward"])
def cmd_stopforward(msg):
    chat_id = msg.chat.id
    if str(chat_id) != str(OWNER_ID):
        send_info(chat_id, "Эта команда только для владельца.")
        delete_message_later(chat_id, msg.message_id, 15)
        return
    clear_forward_all()
    send_info(chat_id, "Пересылка полностью отключена.")
    delete_message_later(chat_id, msg.message_id, 15)
@bot.message_handler(commands=["backup_gdrive_on"])
def cmd_on_drive(msg):
    chat_id = msg.chat.id
    backup_flags["drive"] = True
    save_data(data)
    send_info(chat_id, "☁️ Бэкап в Google Drive включён")
    delete_message_later(chat_id, msg.message_id, 15)
@bot.message_handler(commands=["backup_gdrive_off"])
def cmd_off_drive(msg):
    chat_id = msg.chat.id
    backup_flags["drive"] = False
    save_data(data)
    send_info(chat_id, "☁️ Бэкап в Google Drive выключен")
    delete_message_later(chat_id, msg.message_id, 15)
@bot.message_handler(commands=["backup_channel_on"])
def cmd_on_channel(msg):
    chat_id = msg.chat.id
    backup_flags["channel"] = True
    save_data(data)
    send_info(chat_id, "📡 Бэкап в канал включён")
    delete_message_later(chat_id, msg.message_id, 15)
@bot.message_handler(commands=["backup_channel_off"])
def cmd_off_channel(msg):
    chat_id = msg.chat.id
    backup_flags["channel"] = False
    save_data(data)
    send_info(chat_id, "📡 Бэкап в канал выключен")
    delete_message_later(chat_id, msg.message_id, 15)
@bot.message_handler(commands=["autoadd_info", "autoadd.info"])
def cmd_autoadd_info(msg):
    chat_id = msg.chat.id
    # ❌ OWNER — авто-добавление всегда включено
    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        send_and_auto_delete(
            chat_id,
            "⚙️ Авто-добавление у владельца ВСЕГДА включено.",
            10
        )
    return
    
    delete_message_later(chat_id, msg.message_id, 15)
    store = get_chat_store(chat_id)
    settings = store.setdefault("settings", {})
    current = settings.get("auto_add", False)
    new_state = not current
    settings["auto_add"] = new_state
    save_chat_json(chat_id)
    send_and_auto_delete(
        chat_id,
        f"⚙️ Авто-добавление сообщений: {'ВКЛЮЧЕНО' if new_state else 'ВЫКЛЮЧЕНО'}\n"
        f"Использование:\n"
        f"- ВКЛ → каждое сообщение с суммой записывается автоматически\n"
        f"- ВЫКЛ → работает только через кнопку «Добавить»"
    )
def send_and_auto_delete(chat_id: int, text: str, delay: int = 10):
    try:
        msg = bot.send_message(chat_id, text)
        def _delete():
            time.sleep(delay)
            try:
                bot.delete_message(chat_id, msg.message_id)
            except Exception:
                pass
        threading.Thread(target=_delete, daemon=True).start()
    except Exception as e:
        log_error(f"send_and_auto_delete: {e}")
def delete_message_later(chat_id: int, message_id: int, delay: int = 10):
    """
    Отложенное удаление сообщения пользователя (например, команд).
    """
    try:
        def _job():
            time.sleep(delay)
            try:
                bot.delete_message(chat_id, message_id)
            except Exception:
                pass
        threading.Thread(target=_job, daemon=True).start()
    except Exception as e:
        log_error(f"delete_message_later: {e}")
_edit_cancel_timers = {}
def schedule_cancel_wait(chat_id: int, delay: float = 15.0):
    """
    Через delay секунд:
      • отменяет режим добавления записи (edit_wait.type == 'add')
      • сбрасывает флаг reset_wait (если ещё висит)
    """
    def _job():
        try:
            store = get_chat_store(chat_id)
            changed = False
            wait = store.get("edit_wait")
            if wait and wait.get("type") == "add":
                store["edit_wait"] = None
                changed = True
            if store.get("reset_wait", False):
                store["reset_wait"] = False
                store["reset_time"] = 0
                changed = True
            if changed:
                save_data(data)
        except Exception as e:
            log_error(f"schedule_cancel_wait job: {e}")
    prev = _edit_cancel_timers.get(chat_id)
    if prev and prev.is_alive():
        try:
            prev.cancel()
        except Exception:
            pass
    t = threading.Timer(delay, _job)
    _edit_cancel_timers[chat_id] = t
    t.start()
def schedule_cancel_edit(chat_id: int, message_id: int, delay: int = 30):
    def _job():
        try:
            store = get_chat_store(chat_id)
            if store.get("edit_wait"):
                store["edit_wait"] = None
                save_data(data)
            try:
                bot.delete_message(chat_id, message_id)
            except Exception:
                pass
        except Exception as e:
            log_error(f"schedule_cancel_edit: {e}")

    t = threading.Timer(delay, _job)
    t.start()
def update_chat_info_from_message(msg):
    """
    Обновляет информацию о чате при каждом сообщении.
    Хранится в: store["info"] и store["known_chats"] (для OWNER).
    """
    chat_id = msg.chat.id
    store = get_chat_store(chat_id)
    info = store.setdefault("info", {})
    info["title"] = msg.chat.title or info.get("title") or f"Чат {chat_id}"
    info["username"] = msg.chat.username or info.get("username")
    info["type"] = msg.chat.type
    if OWNER_ID and str(chat_id) != str(OWNER_ID):
        owner_store = get_chat_store(int(OWNER_ID))
        kc = owner_store.setdefault("known_chats", {})
        kc[str(chat_id)] = {
            "title": info["title"],
            "username": info["username"],
            "type": info["type"],
        }
        save_chat_json(int(OWNER_ID))
    save_chat_json(chat_id)
_finalize_timers = {}
def schedule_finalize(chat_id: int, day_key: str, delay: float = 2.0):
    def _safe(action_name, func):
        """
        Безопасный вызов: любая ошибка — логируем и продолжаем выполнение.
        """
        try:
            return func()
        except Exception as e:
            log_error(f"[FINALIZE ERROR] {action_name}: {e}")
            return None

    def _job():
        # 1️⃣ Внутренний перерасчёт и сохранение
        _safe("recalc_balance", lambda: recalc_balance(chat_id))
        _safe("rebuild_global_records", rebuild_global_records)
        _safe("save_chat_json", lambda: save_chat_json(chat_id))
        _safe("save_data", lambda: save_data(data))
        _safe("export_global_csv", lambda: export_global_csv(data))

        # 2️⃣ Окно дня + бэкап
        if OWNER_ID and str(chat_id) == str(OWNER_ID):
            _safe(
                "owner_backup_window",
                lambda: backup_window_for_owner(chat_id, day_key, None)
            )
        else:
            _safe(
                "force_new_day_window",
                lambda: force_new_day_window(chat_id, day_key)
            )
            _safe(
                "backup_to_chat",
                lambda: force_backup_to_chat(chat_id)
            )

        # 3️⃣ Бэкап в канал (для всех)
        _safe(
            "backup_to_channel",
            lambda: send_backup_to_channel(chat_id)
        )

        # 4️⃣ Итоги
        _safe(
            "refresh_total_chat",
            lambda: refresh_total_message_if_any(chat_id)
        )
        if OWNER_ID and str(chat_id) != str(OWNER_ID):
            _safe(
                "refresh_total_owner",
                lambda: refresh_total_message_if_any(int(OWNER_ID))
            )

    t_prev = _finalize_timers.get(chat_id)
    if t_prev and t_prev.is_alive():
        try:
            t_prev.cancel()
        except Exception:
            pass
    t = threading.Timer(delay, _job)
    _finalize_timers[chat_id] = t
    t.start()
def recalc_balance(chat_id: int):
    store = get_chat_store(chat_id)
    store["balance"] = sum(r.get("amount", 0) for r in store.get("records", []))
def rebuild_global_records():
    all_recs = []
    for cid, st in data.get("chats", {}).items():
        all_recs.extend(st.get("records", []))
    data["records"] = all_recs
    data["overall_balance"] = sum(r.get("amount", 0) for r in all_recs)
#🔴🔴🔴🔴🔴
def force_backup_to_chat(chat_id: int):
    try:
        save_chat_json(chat_id)
        json_path = chat_json_file(chat_id)
        if not os.path.exists(json_path):
            log_error(f"force_backup_to_chat: {json_path} missing")
            return

        meta = _load_chat_backup_meta()
        msg_key = f"msg_chat_{chat_id}"
        ts_key = f"timestamp_chat_{chat_id}"

        chat_title = _get_chat_title_for_backup(chat_id)
        caption = (
            f"🧾 Авто-бэкап JSON чата: {chat_title}\n"
            f"⏱ {now_local().strftime('%Y-%m-%d %H:%M:%S')}"
        )

        with open(json_path, "rb") as f:
            data_bytes = f.read()
            if not data_bytes:
                log_error("force_backup_to_chat: empty JSON")
                return

        base = os.path.basename(json_path)
        name_no_ext, dot, ext = base.partition(".")
        suffix = get_chat_name_for_filename(chat_id)
        file_name = suffix if suffix else name_no_ext
        if dot:
            file_name += f".{ext}"

        buf = io.BytesIO(data_bytes)
        buf.name = file_name

        sent = bot.send_document(chat_id, buf, caption=caption)
        meta[msg_key] = sent.message_id
        meta[ts_key] = now_local().isoformat(timespec="seconds")
        _save_chat_backup_meta(meta)

    except Exception as e:
        log_error(f"force_backup_to_chat({chat_id}): {e}")

def backup_window_for_owner(chat_id: int, day_key: str, message_id_override: int | None = None):
    """
    Для OWNER_ID: одно сообщение, в котором:
      • документ JSON (backup)
      • caption = окно дня (render_day_window)
      • те же кнопки (build_main_keyboard)
    """
    if not OWNER_ID or str(chat_id) != str(OWNER_ID):
        return

    # Текст окна и кнопки
    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)

    # Обновляем JSON-файл
    save_chat_json(chat_id)
    json_path = chat_json_file(chat_id)
    if not os.path.exists(json_path):
        log_error(f"backup_window_for_owner: {json_path} missing")
        return

    try:
        with open(json_path, "rb") as f:
            data_bytes = f.read()
        if not data_bytes:
            log_error("backup_window_for_owner: empty JSON")
            return

        base = os.path.basename(json_path)
        name_no_ext, dot, ext = base.partition(".")
        suffix = get_chat_name_for_filename(chat_id)
        if suffix:
            file_name = suffix
        else:
            file_name = name_no_ext
        if dot:
            file_name += f".{ext}"

        buf = io.BytesIO(data_bytes)
        buf.name = file_name

        # Если кнопка нажата на конкретном сообщении — редактируем именно его
        mid = message_id_override or get_active_window_id(chat_id, day_key)
        if message_id_override:
            try:
                set_active_window_id(chat_id, day_key, message_id_override)
            except Exception:
                pass

        # Пытаемся обновить старое окно, если оно есть
        if mid:
            try:
                media = InputMediaDocument(buf, caption=txt, parse_mode="HTML")
                bot.edit_message_media(
                    chat_id=chat_id,
                    message_id=mid,
                    media=media,
                    reply_markup=kb
                )
                set_active_window_id(chat_id, day_key, mid)
                return
            except Exception as e:
                log_error(f"backup_window_for_owner: edit_message_media failed: {e}")
                # fallback: пробуем хотя бы caption+кнопки обновить
                try:
                    bot.edit_message_caption(
                        chat_id=chat_id,
                        message_id=mid,
                        caption=txt,
                        reply_markup=kb,
                        parse_mode="HTML"
                    )
                    set_active_window_id(chat_id, day_key, mid)
                    return
                except Exception as e2:
                    log_error(f"backup_window_for_owner: edit_caption failed: {e2}")
                    try:
                        bot.delete_message(chat_id, mid)
                    except Exception:
                        pass

        # Если не получилось отредактировать — создаём новое сообщение
        sent = bot.send_document(
            chat_id,
            buf,
            caption=txt,
            reply_markup=kb
        )
        set_active_window_id(chat_id, day_key, sent.message_id)
    except Exception as e:
        log_error(f"backup_window_for_owner({chat_id}, {day_key}): {e}")
        
def force_new_day_window(chat_id: int, day_key: str):
    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        backup_window_for_owner(chat_id, day_key)
        return

    old_mid = get_active_window_id(chat_id, day_key)
    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)
    sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
    set_active_window_id(chat_id, day_key, sent.message_id)
    if old_mid:
        try:
            bot.delete_message(chat_id, old_mid)
        except Exception:
            pass
#@bot.message_handler(content_types=["text"])
def reset_chat_data(chat_id: int):
    """
    Полное обнуление данных чата:
      • баланс
      • записи / daily_records
      • next_id
      • active_windows
      • edit_wait / edit_target
      • обновление окна дня
      • бэкап
    """
    try:
        store = get_chat_store(chat_id)
        store["balance"] = 0
        store["records"] = []
        store["daily_records"] = {}
        store["next_id"] = 1
        store["active_windows"] = {}
        store["edit_wait"] = None
        store["edit_target"] = None
        save_data(data)
        save_chat_json(chat_id)
        export_global_csv(data)
        send_backup_to_channel(chat_id)
        send_backup_to_chat(chat_id)
        day_key = store.get("current_view_day", today_key())
        update_or_send_day_window(chat_id, day_key)
        try:
            day_key = get_chat_store(chat_id).get("current_view_day", today_key())
            update_or_send_day_window(chat_id, day_key)
        except Exception:
            pass
        refresh_total_message_if_any(chat_id)
        if OWNER_ID and str(chat_id) != str(OWNER_ID):
            try:
                refresh_total_message_if_any(int(OWNER_ID))
            except Exception:
                pass
    except Exception as e:
        log_error(f"reset_chat_data({chat_id}): {e}")

@bot.message_handler(content_types=["document"])
def handle_document(msg):
    global restore_mode, data

    chat_id = msg.chat.id
    update_chat_info_from_message(msg)

    file = msg.document
    fname = (file.file_name or "").lower()

    log_info(f"[DOC] recv chat={chat_id} restore={restore_mode} fname={fname}")

    # ==================================================
    # 🔒 RESTORE MODE — ПЕРЕХВАТ ДОКУМЕНТА
    # ==================================================
    if restore_mode is not None and restore_mode == chat_id:

        if not (fname.endswith(".json") or fname.endswith(".csv")):
            send_and_auto_delete(
                chat_id,
                "⚠️ В режиме восстановления принимаются только JSON / CSV."
            )
            return

        try:
            file_info = bot.get_file(file.file_id)
            raw = bot.download_file(file_info.file_path)
        except Exception as e:
            send_and_auto_delete(chat_id, f"❌ Ошибка скачивания: {e}")
            return

        tmp_path = f"restore_{chat_id}_{fname}"
        with open(tmp_path, "wb") as f:
            f.write(raw)

        try:
            # 🌍 GLOBAL DATA.JSON
            if fname == "data.json":
                os.replace(tmp_path, "data.json")
                data = load_data()

                finance_active_chats.clear()
                fac = data.get("finance_active_chats") or {}
                for cid, enabled in fac.items():
                    if enabled:
                        try:
                            finance_active_chats.add(int(cid))
                        except Exception:
                            pass

                restore_mode = None
                send_and_auto_delete(chat_id, "🟢 Глобальный data.json восстановлен!")
                return

            # 🌍 CSV META
            if fname == "csv_meta.json":
                os.replace(tmp_path, "csv_meta.json")
                restore_mode = None
                send_and_auto_delete(chat_id, "🟢 csv_meta.json восстановлён")
                return

            # 🧾 CHAT JSON
            if fname.endswith(".json"):
                payload = _load_json(tmp_path, None)
                if not isinstance(payload, dict):
                    raise RuntimeError("JSON не является объектом")

                # если это вдруг глобальный data.json
                if "chats" in payload:
                    os.replace(tmp_path, "data.json")
                    data.clear()
                    data.update(load_data())
                    restore_mode = None
                    send_and_auto_delete(chat_id, "🟢 Глобальный data.json восстановлен")
                    return

                inner_chat_id = payload.get("chat_id")
                if inner_chat_id is None:
                    raise RuntimeError("В JSON нет chat_id")

                if int(inner_chat_id) != int(chat_id):
                    raise RuntimeError(
                        f"JSON относится к чату {inner_chat_id}, а не к текущему {chat_id}"
                    )

                restore_from_json(chat_id, tmp_path)

                # 🛠 ОБНОВЛЕНИЕ ПОСЛЕ RESTORE
                day_key = get_chat_store(chat_id).get(
                    "current_view_day",
                    today_key()
                )
                update_or_send_day_window(chat_id, day_key)
                send_backup_to_chat(chat_id)
                send_backup_to_channel(chat_id)

                restore_mode = None
                send_and_auto_delete(
                    chat_id,
                    f"🟢 JSON чата {chat_id} восстановлен"
                )
                return

            # 📊 CHAT CSV
            if fname.startswith("data_") and fname.endswith(".csv"):
                restore_from_csv(chat_id, tmp_path)

                day_key = get_chat_store(chat_id).get(
                    "current_view_day",
                    today_key()
                )
                update_or_send_day_window(chat_id, day_key)
                send_backup_to_chat(chat_id)
                send_backup_to_channel(chat_id)

                restore_mode = None
                send_and_auto_delete(
                    chat_id,
                    f"🟢 CSV чата восстановлен ({fname})"
                )
                return

            send_and_auto_delete(chat_id, f"⚠️ Неизвестный файл: {fname}")

        except Exception as e:
            send_and_auto_delete(chat_id, f"❌ Ошибка восстановления: {e}")

        finally:
            try:
                os.remove(tmp_path)
            except Exception:
                pass

        return

    # ==================================================
    # 🟢 ОБЫЧНЫЙ РЕЖИМ — ПЕРЕСЫЛКА
    # ==================================================
    forward_any_message(chat_id, msg)
                                    
def cleanup_forward_links(chat_id: int):
    """
    Удаляет все связи пересылки для чата.
    ОБЯЗАТЕЛЬНО вызывать при reset / restore.
    """
    for key in list(forward_map.keys()):
        if key[0] == chat_id:
            del forward_map[key]
            
KEEP_ALIVE_SEND_TO_OWNER = False
def keep_alive_task():
    while True:
        try:
            if APP_URL:
                try:
                    resp = requests.get(APP_URL, timeout=10)
                    log_info(f"Keep-alive ping -> {resp.status_code}")
                except Exception as e:
                    log_error(f"Keep-alive self error: {e}")
            if KEEP_ALIVE_SEND_TO_OWNER and OWNER_ID:
                try:
                    pass
                except Exception as e:
                    log_error(f"Keep-alive notify error: {e}")
        except Exception as e:
            log_error(f"Keep-alive loop error: {e}")
        time.sleep(max(10, KEEP_ALIVE_INTERVAL_SECONDS))
        
@bot.edited_message_handler(
    content_types=["text", "photo", "video", "document", "audio"]
)
def on_edited_message(msg):
    chat_id = msg.chat.id

    # 🔴 ФИНАНСЫ — БЕЗ РЕЖИМОВ
    try:
        edited = handle_finance_edit(msg)
        if edited:
            store = get_chat_store(chat_id)
            day_key = store.get("current_view_day") or today_key()
            log_info(f"[EDIT-FIN] finalize day_key={day_key}")
            schedule_finalize(chat_id, day_key)
    except Exception as e:
        log_error(f"[EDIT-FIN] failed: {e}")

    # 🟢 ПЕРЕСЫЛКА — НЕ ТРОГАЕМ
    key = (chat_id, msg.message_id)
    links = forward_map.get(key)
    if not links:
        return

    text = msg.text or msg.caption
    if not text:
        return

    for dst_chat_id, dst_msg_id in list(links):
        try:
            bot.edit_message_text(
                text,
                chat_id=dst_chat_id,
                message_id=dst_msg_id
            )
        except Exception:
            try:
                bot.edit_message_caption(
                    caption=text,
                    chat_id=dst_chat_id,
                    message_id=dst_msg_id
                )
            except Exception as e:
                log_error(f"edit forward failed {dst_chat_id}:{dst_msg_id}: {e}")
                                            
def start_keep_alive_thread():
    t = threading.Thread(target=keep_alive_task, daemon=True)
    t.start()
@app.route(f"/{BOT_TOKEN}", methods=["POST"])
def telegram_webhook():
    try:
        payload = request.get_json(force=True, silent=False)
    except Exception as e:
        log_error(f"WEBHOOK: get_json failed: {e}")
        return "BAD REQUEST", 400

    try:
        # Логи — чтобы ты 100% видел, приходят ли edited_message
        if isinstance(payload, dict):
            if "edited_message" in payload:
                log_info("WEBHOOK: получен update с edited_message ✅")
            elif "message" in payload:
                log_info("WEBHOOK: получен update с message")
            elif "callback_query" in payload:
                log_info("WEBHOOK: получен update с callback_query")

        update = telebot.types.Update.de_json(payload)
        bot.process_new_updates([update])
    except Exception as e:
        log_error(f"WEBHOOK: process update error: {e}")
        return "ERROR", 500

    return "OK", 200
        
def set_webhook():
    if not APP_URL:
        log_info("APP_URL не указан — работаем в режиме polling.")
        return

    wh_url = APP_URL.rstrip("/") + f"/{BOT_TOKEN}"

    # Важно: снимаем, затем ставим заново с нужными типами апдейтов
    bot.remove_webhook()
    time.sleep(0.5)

    bot.set_webhook(
        url=wh_url,
        allowed_updates=[
            "message",
            "edited_message",
            "callback_query",
            "channel_post",
            "edited_channel_post",
        ],
    )
    log_info(f"Webhook установлен: {wh_url} (allowed_updates включает edited_message)")
        
def main():
    global data
    restored = restore_from_gdrive_if_needed()
    data = load_data()
    data["forward_rules"] = load_forward_rules()
    # ✅ OWNER — всегда активен
    if OWNER_ID:
        try:
            finance_active_chats.add(int(OWNER_ID))
        except Exception:
            pass
    log_info(f"Данные загружены. Версия бота: {VERSION}")
    set_webhook()
    start_keep_alive_thread()
    if OWNER_ID:
        try:
            owner_id = int(OWNER_ID)
        except Exception:
            owner_id = None
        if owner_id:
            try:
                bot.send_message(
                    owner_id,
                    f"✅ Бот запущен (версия {VERSION}).\n"
                    f"Восстановление: {'OK' if restored else 'пропущено'}"
                )
            except Exception as e:
                log_error(f"notify owner on start: {e}")
    app.run(host="0.0.0.0", port=PORT)
if __name__ == "__main__":
    main()