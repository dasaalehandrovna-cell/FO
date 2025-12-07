# даты/оновл цифр/нов окно вост/
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
from telebot.types import (
    InputMediaPhoto,
    InputMediaVideo,
    InputMediaDocument,
    InputMediaAudio
)
from flask import Flask, request
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.discovery import build
from google.oauth2 import service_account
# -----------------------------
# ⚙️ Конфигурация (жёстко прописанные значения для Render)
# -----------------------------
BOT_TOKEN = "8353050321:AAHS5p9JAZpqfesrScSOgSbGw8_FADEX8l8"
OWNER_ID = "8592220081"
APP_URL = "https://fo-1.onrender.com"
WEBHOOK_URL = "https://fo-1.onrender.com"  # если дальше в коде используется отдельная переменная вебхука
PORT = 5000
#VERSION = "Code_022.3-C"
BACKUP_CHAT_ID = "-1003291414261"

#BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
#OWNER_ID = os.getenv("OWNER_ID", "").strip()
#BACKUP_CHAT_ID = os.getenv("BACKUP_CHAT_ID", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "").strip()
#APP_URL = os.getenv("APP_URL", "").strip()
#PORT = int(os.getenv("PORT", "8443"))
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")
VERSION = "Code_ 022.9.11 🎈с4-15/18/20"
DEFAULT_TZ = "America/Argentina/Buenos_Aires"
KEEP_ALIVE_INTERVAL_SECONDS = 60
DATA_FILE = "data.json"
CSV_FILE = "data.csv"
CSV_META_FILE = "csv_meta.json"
backup_flags = {
    "drive": True,
    "channel": True,
}
restore_mode = False
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

        if msg_id:
            fobj = _open_file()
            if not fobj:
                return
            try:
                bot.edit_message_media(
                    chat_id=chat_id,
                    message_id=msg_id,
                    media=telebot.types.InputMediaDocument(fobj, caption=caption)
                )
                log_info(f"Chat backup UPDATED in chat {chat_id}")
                meta[ts_key] = now_local().isoformat(timespec="seconds")
                _save_chat_backup_meta(meta)
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
def looks_like_amount(text):
    try:
        amount, note = split_amount_and_note(text)
        return True
    except:
        return False
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
    Добавлено:
    • если передан chat_title — он включается в имя файла, которое видит Telegram
    • защита от пустого файла (Telegram даёт 400)
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
            file_name = safe_title
            if dot:
                file_name += f".{ext}"
        else:
            file_name = base_name
        caption = f"📦 {file_name} — {now_local().strftime('%Y-%m-%d %H:%M')}"
        def _open_for_telegram() -> io.BytesIO | None:
            if not os.path.exists(base_path):
                log_error(f"send_backup_to_channel_for_file: {base_path} not found")
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
        if meta.get(msg_key):
            try:
                fobj = _open_for_telegram()
                if not fobj:
                    return
                bot.edit_message_media(
                    chat_id=int(BACKUP_CHAT_ID),
                    message_id=meta[msg_key],
                    media=telebot.types.InputMediaDocument(fobj, caption=caption),
                )
                log_info(f"Channel file updated: {base_path}")
            except Exception as e:
                log_error(f"edit_message_media {base_path}: {e}")
                try:
                    bot.delete_message(int(BACKUP_CHAT_ID), meta[msg_key])
                except Exception as del_e:
                    log_error(f"delete_message {base_path}: {del_e}")
                fobj = _open_for_telegram()
                if not fobj:
                    return
                sent = bot.send_document(int(BACKUP_CHAT_ID), fobj, caption=caption)
                meta[msg_key] = sent.message_id
        else:
            fobj = _open_for_telegram()
            if not fobj:
                return
            sent = bot.send_document(int(BACKUP_CHAT_ID), fobj, caption=caption)
            meta[msg_key] = sent.message_id
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
def forward_text_anon(source_chat_id: int, msg, targets: list[tuple[int, str]]):
    """Анонимная пересылка текста."""
    for dst, mode in targets:
        try:
            bot.copy_message(dst, source_chat_id, msg.message_id)
        except Exception as e:
            log_error(f"forward_text_anon to {dst}: {e}")
def forward_media_anon(source_chat_id: int, msg, targets: list[tuple[int, str]]):
    """Анонимная пересылка любых медиа."""
    for dst, mode in targets:
        try:
            bot.copy_message(dst, source_chat_id, msg.message_id)
        except Exception as e:
            log_error(f"forward_media_anon to {dst}: {e}")
_media_group_cache = {}
def collect_media_group(chat_id: int, msg):
    """
    Собирает альбом (media_group) в кэш пока все элементы не пришли.
    """
    gid = msg.media_group_id
    if not gid:
        return [msg]
    group = _media_group_cache.setdefault(chat_id, {})
    arr = group.setdefault(gid, [])
    arr.append(msg)
    if len(arr) == 1:
        time.sleep(0.2)
    complete = group.pop(gid, arr)
    return complete
def forward_media_group_anon(source_chat_id: int, messages: list, targets: list[tuple[int, str]]):
    """
    Пересылка собранного альбома анонимно.
    """
    if not messages:
        return
    media_list = []
    for msg in messages:
        if msg.content_type == "photo":
            file_id = msg.photo[-1].file_id
            caption = msg.caption or None
            media_list.append(InputMediaPhoto(file_id, caption=caption))
        elif msg.content_type == "video":
            file_id = msg.video.file_id
            caption = msg.caption or None
            media_list.append(InputMediaVideo(file_id, caption=caption))
        elif msg.content_type == "document":
            file_id = msg.document.file_id
            caption = msg.caption or None
            media_list.append(InputMediaDocument(file_id, caption=caption))
        elif msg.content_type == "audio":
            file_id = msg.audio.file_id
            caption = msg.caption or None
            media_list.append(InputMediaAudio(file_id, caption=caption))
        else:
            for dst, mode in targets:
                try:
                    bot.copy_message(dst, source_chat_id, msg.message_id)
                except:
                    pass
            return
    for dst, mode in targets:
        try:
            bot.send_media_group(dst, media_list)
        except Exception as e:
            log_error(f"forward_media_group_anon to {dst}: {e}")
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
    label = f"{day_key} ({tag}, {wd})" if tag else f"{day_key} ({wd})"
    lines.append(f"📅 <b>{label}</b>")
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
        lines.append(f"{sid} {fmt_num(amt)} <i>{note}</i>")
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
        types.InlineKeyboardButton("📝 Редактировать", callback_data=f"d:{day_key}:edit_menu")
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
@bot.callback_query_handler(func=lambda c: True)
def on_callback(call):
    """
    Универсальный обработчик всех callback_data:
      • fw_*  — новое меню пересылки A ↔ B (только для владельца)
      • c:*   — календарь
      • d:*   — команды окна дня, редактирование, старое меню пересылки
    """
    try:
        data_str = call.data or ""
        chat_id = call.message.chat.id
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
                bot.edit_message_text(
                    "Выберите чат A:",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
                return
            if data_str == "fw_back_root":
                owner_store = get_chat_store(int(OWNER_ID))
                day_key = owner_store.get("current_view_day", today_key())
                kb = build_edit_menu_keyboard(day_key, chat_id)
                try:
                    bot.edit_message_text(
                        f"Меню редактирования для {day_key}:",
                        chat_id=chat_id,
                        message_id=call.message.message_id,
                        reply_markup=kb
                    )
                except Exception:
                    bot.send_message(
                        chat_id,
                        f"Меню редактирования для {day_key}:",
                        reply_markup=kb
                    )
                return
            if data_str == "fw_back_src":
                kb = build_forward_source_menu()
                bot.edit_message_text(
                    "Выберите чат A:",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
                return
            if data_str.startswith("fw_back_tgt:"):
                try:
                    A = int(data_str.split(":", 1)[1])
                except Exception:
                    return
                kb = build_forward_target_menu(A)
                bot.edit_message_text(
                    f"Источник пересылки: {A}\nВыберите чат B:",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
                return
            if data_str.startswith("fw_src:"):
                try:
                    A = int(data_str.split(":", 1)[1])
                except Exception:
                    return
                kb = build_forward_target_menu(A)
                bot.edit_message_text(
                    f"Источник пересылки: {A}\nВыберите чат B:",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
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
                bot.edit_message_text(
                    f"Настройка пересылки: {A} ⇄ {B}",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
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
                bot.edit_message_text(
                    "Маршрут обновлён.\nВыберите чат A:",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
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
            txt, _ = render_day_window(chat_id, day_key)
            kb = build_main_keyboard(day_key, chat_id)
            store["current_view_day"] = day_key
            bot.edit_message_text(
                txt,
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb,
                parse_mode="HTML"
            )
            set_active_window_id(chat_id, day_key, call.message.message_id)
            return
        if cmd == "prev":
            d = datetime.strptime(day_key, "%Y-%m-%d") - timedelta(days=1)
            nd = d.strftime("%Y-%m-%d")
            txt, _ = render_day_window(chat_id, nd)
            kb = build_main_keyboard(nd, chat_id)
            store["current_view_day"] = nd
            bot.edit_message_text(
                txt,
                chat_id,
                call.message.message_id,
                reply_markup=kb,
                parse_mode="HTML"
            )
            set_active_window_id(chat_id, nd, call.message.message_id)
            return
        if cmd == "next":
            d = datetime.strptime(day_key, "%Y-%m-%d") + timedelta(days=1)
            nd = d.strftime("%Y-%m-%d")
            txt, _ = render_day_window(chat_id, nd)
            kb = build_main_keyboard(nd, chat_id)
            store["current_view_day"] = nd
            bot.edit_message_text(
                txt,
                chat_id,
                call.message.message_id,
                reply_markup=kb,
                parse_mode="HTML"
            )
            set_active_window_id(chat_id, nd, call.message.message_id)
            return
        if cmd == "today":
            nd = today_key()
            txt, _ = render_day_window(chat_id, nd)
            kb = build_main_keyboard(nd, chat_id)
            store["current_view_day"] = nd
            bot.edit_message_text(
                txt,
                chat_id,
                call.message.message_id,
                reply_markup=kb,
                parse_mode="HTML"
            )
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
                text = f"💰 <b>Общий итог по этому чату:</b> {fmt_num(chat_bal)}"
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
            lines.append("💰 <b>Общий итог (для владельца)</b>")
            lines.append("")
            lines.append(f"• Этот чат ({title}): <b>{fmt_num(chat_bal)}</b>")

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
            lines.append(f"• Всего по всем чатам: <b>{fmt_num(total_all)}</b>")

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
            bot.send_message(chat_id, info_text)
            return
        if cmd == "edit_menu":
            store["current_view_day"] = day_key
            kb = build_edit_menu_keyboard(day_key, chat_id)
            bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb
            )
            return
        if cmd == "back_main":
            store["current_view_day"] = day_key
            txt, _ = render_day_window(chat_id, day_key)
            kb = build_main_keyboard(day_key, chat_id)
            bot.edit_message_text(
                txt,
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb,
                parse_mode="HTML"
            )
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
            bot.edit_message_text(
                "Выберите действие:",
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb2
            )
            return
        if cmd.startswith("edit_rec_"):
            rid = int(cmd.split("_")[-1])
            store["edit_wait"] = {
                "type": "edit",
                "day_key": day_key,
                "rid": rid
            }
            save_data(data)
            text_edit = f"✏️ Редактирование записи R{rid}\n\n"\
                        f"Введите новую сумму и текст.\n"\
                        f"Можно прислать несколько строк."
            kb_back = types.InlineKeyboardMarkup()
            kb_back.row(
                types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{day_key}:edit_list")
            )
            bot.edit_message_text(
                text_edit,
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb_back
            )
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
            bot.edit_message_text(
                "Меню пересылки:\nВыберите режим:",
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb
            )
            return
        if cmd == "forward_old":
            if not OWNER_ID or str(chat_id) != str(OWNER_ID):
                bot.send_message(chat_id, "Меню доступно только владельцу.")
                return
            kb = build_forward_chat_list(day_key, chat_id)
            bot.edit_message_text(
                "Выберите чат, для которого хотите настроить пересылку:",
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb
            )
            return
        if cmd.startswith("fw_cfg_"):
            tgt = int(cmd.split("_")[-1])
            kb = build_forward_direction_menu(day_key, chat_id, tgt)
            bot.edit_message_text(
                f"Настройка пересылки для чата {tgt}:",
                chat_id=chat_id,
                message_id=call.message.message_id,
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
    except Exception as e:
        log_error(f"on_callback error: {e}")
def add_record_to_chat(chat_id: int, amount: int, note: str, owner):
    store = get_chat_store(chat_id)
    rid = store.get("next_id", 1)
    rec = {
        "id": rid,
        "short_id": f"R{rid}",
        "timestamp": now_local().isoformat(timespec="seconds"),
        "amount": amount,
        "note": note,
        "owner": owner,
        "msg_id": msg.message_id,
        "origin_msg_id": msg.message_id,
    }
    data.setdefault("records", []).append(rec)
    store.setdefault("records", []).append(rec)
    store.setdefault("daily_records", {}).setdefault(today_key(), []).append(rec)
    renumber_chat_records(chat_id)
    store["balance"] = sum(x["amount"] for x in store["records"])
    data["overall_balance"] = sum(x["amount"] for x in data["records"])
    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)
    send_backup_to_channel(chat_id)
def update_record_in_chat(chat_id: int, rid: int, new_amount: int, new_note: str):
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
    mid = get_active_window_id(chat_id, day_key)
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
    """
    Если окно дня существует — обновляем через edit.
    Если нет — создаём.
    """
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
            text = f"💰 <b>Общий итог по этому чату:</b> {fmt_num(chat_bal)}"
        else:
            lines = []
            info = store.get("info", {})
            title = info.get("title") or f"Чат {chat_id}"
            lines.append("💰 <b>Общий итог (для владельца)</b>")
            lines.append("")
            lines.append(f"• Этот чат ({title}): <b>{fmt_num(chat_bal)}</b>")
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
            lines.append(f"• Всего по всем чатам: <b>{fmt_num(total_all)}</b>")
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
    delete_message_later(chat_id, msg.message_id, 15)
    set_finance_mode(chat_id, True)
    save_data(data)
    send_info(chat_id, "🚀 Финансовый режим включён!\nОтправьте /start")
    return
@bot.message_handler(commands=["start"])
def cmd_start(msg):
    chat_id = msg.chat.id
    delete_message_later(chat_id, msg.message_id, 15)
    if not require_finance(chat_id):
        return
    day_key = today_key()
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
    restore_mode = True
    send_and_auto_delete(
        msg.chat.id,
        "📥 Режим восстановления включён.\n"
        "Теперь отправьте файл:\n"
        "• data.json\n"
        "• data_<chat_id>.json\n"
        "• csv_meta.json\n"
        "• data_<chat>.csv\n\n"
        "Пересылка документов временно отключена."
    )
@bot.message_handler(commands=["restore_off"])
def cmd_restore_off(msg):
    global restore_mode
    restore_mode = False
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
    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)
    sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
    set_active_window_id(chat_id, day_key, sent.message_id)
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

        # 2️⃣ Обновление/создание окна дня
        _safe("force_new_day_window", lambda: force_new_day_window(chat_id, day_key))

        # 3️⃣ Бэкапы
        _safe("backup_to_channel", lambda: send_backup_to_channel(chat_id))
        _safe("backup_to_chat", lambda: force_backup_to_chat(chat_id))

        # 4️⃣ Итоги
        _safe("refresh_total_chat", lambda: refresh_total_message_if_any(chat_id))
        if OWNER_ID and str(chat_id) != str(OWNER_ID):
            _safe("refresh_total_owner", lambda: refresh_total_message_if_any(int(OWNER_ID)))

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
        old_mid = meta.get(msg_key)
        last_ts = meta.get(ts_key)

        # 🔄 Новый файл после смены дня
        if old_mid and last_ts:
            try:
                prev_dt = datetime.fromisoformat(last_ts)
                if prev_dt.date() != now_local().date():
                    old_mid = None
            except Exception as e:
                log_error(f"force_backup_to_chat: bad timestamp for chat {chat_id}: {e}")

        chat_title = _get_chat_title_for_backup(chat_id)
        caption = (
            f"🧾 Авто-бэкап JSON чата: {chat_title}\n"
            f"⏱ {now_local().strftime('%Y-%m-%d %H:%M:%S')}"
        )

        with open(json_path, "rb") as f:
            data = f.read()
            if not data:
                log_error("force_backup_to_chat: empty JSON")
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
            buf = io.BytesIO(data)
            buf.name = file_name

        if old_mid:
            try:
                bot.edit_message_media(
                    chat_id=chat_id,
                    message_id=old_mid,
                    media=telebot.types.InputMediaDocument(buf),
                    caption=caption
                )
                return
            except Exception as e:
                log_error(f"force_backup_to_chat: edit failed: {e}")

        sent = bot.send_document(chat_id, buf, caption=caption)
        meta[msg_key] = sent.message_id
        meta[ts_key] = now_local().isoformat(timespec="seconds")
        _save_chat_backup_meta(meta)
    except Exception as e:
        log_error(f"force_backup_to_chat({chat_id}): {e}")
def force_new_day_window(chat_id: int, day_key: str):
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
@bot.message_handler(content_types=["text"])
def handle_text(msg):
    try:
        chat_id = msg.chat.id
        text = (msg.text or "").strip()
        update_chat_info_from_message(msg)
        targets = resolve_forward_targets(chat_id)
        if targets:
            forward_text_anon(chat_id, msg, targets)
        store = get_chat_store(chat_id)
        wait = store.get("edit_wait")
        auto_add_enabled = store.get("settings", {}).get("auto_add", False)
        should_add = False
        if wait and wait.get("type") == "add" and looks_like_amount(text):
                should_add = True
                day_key = wait.get("day_key")
        elif auto_add_enabled and looks_like_amount(text):
                should_add = True
                day_key = store.get("current_view_day", today_key())
        if not should_add:
                pass
        else:
                lines = text.split("\n")
                added_any = False
                for line in lines:
                        line = line.strip()
                        if not line:
                                continue
                        try:
                                amount, note = split_amount_and_note(line)
                        except Exception:
                                send_and_auto_delete(chat_id, f"❌ Ошибка суммы: {line}\nПродолжаю расчёт…")
                                continue
                        rid = store.get("next_id", 1)
                        rec = {
                                "id": rid,
                                "short_id": f"R{rid}",
                                "timestamp": now_local().isoformat(timespec="seconds"),
                                "amount": amount,
                                "note": note,
                                "owner": msg.from_user.id,
                                "msg_id": msg.message_id,
                                "origin_msg_id": msg.message_id,
                        }
                        store.setdefault("records", []).append(rec)
                        store.setdefault("daily_records", {}).setdefault(day_key, []).append(rec)
                        store["next_id"] = rid + 1
                        added_any = True
                if added_any:
                        update_or_send_day_window(chat_id, day_key)
                        schedule_finalize(chat_id, day_key)
                store["balance"] = sum(x["amount"] for x in store["records"])
                data["records"] = []
                for cid, st in data.get("chats", {}).items():
                        data["records"].extend(st.get("records", []))
                data["overall_balance"] = sum(x["amount"] for x in data["records"])
                save_data(data)
                save_chat_json(chat_id)
                store["edit_wait"] = None
                save_data(data)
                return
        if wait and wait.get("type") == "edit":
            rid = wait.get("rid")
            day_key = wait.get("day_key", store.get("current_view_day", today_key()))
            lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
            target = None
            for r in store.get("records", []):
                if r["id"] == rid:
                    target = r
                    break
            if not target:
                send_and_auto_delete(chat_id, "❌ Запись не найдена.")
                store["edit_wait"] = None
                return
            delete_record_in_chat(chat_id, rid)
            for line in lines:
                try:
                    amount, note = split_amount_and_note(line)
                except:
                    bot.send_message(chat_id, f"❌ Ошибка суммы: {line}")
                    continue
                rid2 = store.get("next_id", 1)
                new_rec = {
                    "id": rid2,
                    "short_id": f"R{rid2}",
                    "timestamp": now_local().isoformat(timespec="seconds"),
                    "amount": amount,
                    "note": note,
                    "owner": msg.from_user.id,
                    "msg_id": msg.message_id,
                    "origin_msg_id": msg.message_id,
                }
                store.setdefault("records", []).append(new_rec)
                store.setdefault("daily_records", {}).setdefault(day_key, []).append(new_rec)
                store["next_id"] = rid2 + 1
            update_record_in_chat(chat_id, rid, amount, note)
            schedule_finalize(chat_id, day_key)
            refresh_total_message_if_any(chat_id)
            if OWNER_ID and str(chat_id) != str(OWNER_ID):
                try:
                    refresh_total_message_if_any(int(OWNER_ID))
                except Exception:
                    pass
            store["edit_wait"] = None
            save_data(data)
            return
        if text.upper() == "ДА":
            reset_flag = store.get("reset_wait", False)
            reset_time = store.get("reset_time", 0)
            now_t = time.time()
            if reset_flag and (now_t - reset_time <= 15):
                reset_chat_data(chat_id)
                send_and_auto_delete(chat_id, "🔄 Данные чата обнулены.", 15)
            else:
                send_and_auto_delete(chat_id, "Нет активного запроса на обнуление.", 15)
            store["reset_wait"] = False
            store["reset_time"] = 0
            save_data(data)
            return
        if store.get("reset_wait", False):
            store["reset_wait"] = False
            store["reset_time"] = 0
            save_data(data)
    except Exception as e:
        log_error(f"handle_text: {e}")
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
@bot.message_handler(
    content_types=[
        "photo", "audio", "video", "voice",
        "video_note", "sticker", "animation"
    ]
)
def handle_media_forward(msg):
    try:
        chat_id = msg.chat.id
        update_chat_info_from_message(msg)
        try:
            BOT_ID = bot.get_me().id
        except:
            BOT_ID = None
        if BOT_ID and msg.from_user and msg.from_user.id == BOT_ID:
            return
        targets = resolve_forward_targets(chat_id)
        if not targets:
            return
        group_msgs = collect_media_group(chat_id, msg)
        if not group_msgs:
            return
        if len(group_msgs) > 1:
            forward_media_group_anon(chat_id, group_msgs, targets)
            return
        for dst, mode in targets:
            try:
                bot.copy_message(dst, chat_id, msg.message_id)
            except Exception as e:
                log_error(f"handle_media_forward to {dst}: {e}")
    except Exception as e:
        log_error(f"handle_media_forward error: {e}")
@bot.message_handler(content_types=["location", "contact", "poll", "venue"])
def handle_special_forward(msg):
    global restore_mode
    if restore_mode:
        return
    try:
        chat_id = msg.chat.id
        update_chat_info_from_message(msg)
        try:
            BOT_ID = bot.get_me().id
        except:
            BOT_ID = None
        if BOT_ID and msg.from_user and msg.from_user.id == BOT_ID:
            return
        targets = resolve_forward_targets(chat_id)
        if not targets:
            return
        for dst, mode in targets:
            try:
                bot.copy_message(dst, chat_id, msg.message_id)
            except Exception as e:
                log_error(f"handle_special_forward to {dst}: {e}")
    except Exception as e:
        log_error(f"handle_special_forward error: {e}")
@bot.message_handler(content_types=["document"])
def handle_document(msg):
    """
    Логика обработки документов:
    1) ВСЕ документы обновляют info/known_chats
    2) Если restore_mode == True → используется как файл восстановления
    3) Если restore_mode == False → обычная пересылка документа
    """
    global restore_mode, data
    chat_id = msg.chat.id
    update_chat_info_from_message(msg)
    file = msg.document
    fname = (file.file_name or "").lower()
    if restore_mode:
        if not (fname.endswith(".json") or fname.endswith(".csv")):
            send_and_auto_delete(chat_id, f"⚠️ Файл '{fname}' не является JSON/CSV.")
            return
        try:
            file_info = bot.get_file(file.file_id)
            raw = bot.download_file(file_info.file_path)
        except Exception as e:
            send_and_auto_delete(chat_id, f"❌ Ошибка скачивания файла: {e}")
            return
        tmp_path = f"restore_{chat_id}_{fname}"
        with open(tmp_path, "wb") as f:
            f.write(raw)
        if fname == "data.json":
            try:
                os.replace(tmp_path, "data.json")
                data = load_data()
                restore_mode = False
                send_and_auto_delete(chat_id, "🟢 Глобальный data.json восстановлен!")
            except Exception as e:
                send_and_auto_delete(chat_id, f"❌ Ошибка: {e}")
            return
        if fname == "csv_meta.json":
            try:
                os.replace(tmp_path, "csv_meta.json")
                restore_mode = False
                send_and_auto_delete(chat_id, "🟢 csv_meta.json восстановлен!")
            except Exception as e:
                send_and_auto_delete(chat_id, f"❌ Ошибка: {e}")
            return
        if fname.endswith(".json"):
            try:
                try:
                    file_info = bot.get_file(file.file_id)
                    raw = bot.download_file(file_info.file_path)
                except Exception as e:
                    send_and_auto_delete(chat_id, f"❌ Ошибка скачивания файла: {e}")
                    return
                tmp_path = f"restore_{chat_id}_{fname}"
                with open(tmp_path, "wb") as f:
                    f.write(raw)
                payload = _load_json(tmp_path, None)
                if not payload or not isinstance(payload, dict):
                    send_and_auto_delete(chat_id, "❌ JSON повреждён или пуст.")
                    return
                target = payload.get("chat_id")
                if not target:
                    send_and_auto_delete(
                        chat_id,
                        "❌ В JSON нет поля chat_id — не могу определить чат!"
                    )
                    return
                target = int(target)
                out_name = f"data_{target}.json"
                os.replace(tmp_path, out_name)
                store = payload
                store["balance"] = sum(r.get("amount", 0) for r in store.get("records", []))
                data.setdefault("chats", {})[str(target)] = store
                finance_active_chats.add(target)
                all_recs = []
                for cid, s in data.get("chats", {}).items():
                    all_recs.extend(s.get("records", []))
                data["records"] = all_recs
                data["overall_balance"] = sum(r.get("amount", 0) for r in all_recs)
                save_data(data)
                save_chat_json(target)
                force_new_day_window(target, today_key())
                restore_mode = False
                send_and_auto_delete(
                    chat_id,
                    f"🟢 Чат {target} восстановлен из файла '{fname}'.\n"
                    f"Записей: {len(store.get('records', []))}\n"
                    f"Баланс: {store['balance']}"
                )
            except Exception as e:
                                # Авто-обновление общих итогов после восстановления чата
                try:
                    refresh_total_message_if_any(target)
                    if OWNER_ID and str(target) != str(OWNER_ID):
                        try:
                            refresh_total_message_if_any(int(OWNER_ID))
                        except Exception as e2:
                            log_error(f"restore JSON: refresh_total_message_if_any(owner) error: {e2}")
                except Exception as e2:
                    log_error(f"restore JSON: refresh_total_message_if_any({target}) error: {e2}")
                send_and_auto_delete(chat_id, f"❌ Ошибка восстановления JSON: {e}")
            return
        if fname.startswith("data_") and fname.endswith(".csv"):
            try:
                os.replace(tmp_path, fname)
                restore_mode = False
                send_and_auto_delete(chat_id, f"🟢 CSV восстановлен: {fname}")
            except Exception as e:
                send_and_auto_delete(chat_id, f"❌ Ошибка: {e}")
            return
        send_and_auto_delete(chat_id, f"⚠️ Формат не поддерживается: {fname}")
        return
    try:
        try:
            BOT_ID = bot.get_me().id
        except:
            BOT_ID = None
        if BOT_ID and msg.from_user and msg.from_user.id == BOT_ID:
            return
        targets = resolve_forward_targets(chat_id)
        if not targets:
            return
        group_msgs = collect_media_group(chat_id, msg)
        if not group_msgs:
            return
        if len(group_msgs) > 1:
            forward_media_group_anon(chat_id, group_msgs, targets)
            return
        for dst, mode in targets:
            try:
                bot.copy_message(dst, chat_id, msg.message_id)
            except Exception as e:
                log_error(f"handle_document forward to {dst}: {e}")
    except Exception as e:
        log_error(f"handle_document error: {e}")
@bot.edited_message_handler(content_types=["text"])
def handle_edited_message(msg):
    """
    Редактирование записи через редактирование сообщения.
    """
    chat_id = msg.chat.id
    message_id = msg.message_id
    new_text = (msg.text or "").strip()
    log_info(f"EDITED: пришёл edited_message в чате {chat_id}, msg_id={message_id}, text='{new_text}'")
    if not is_finance_mode(chat_id):
        log_info(f"EDITED: игнор, finance_mode=OFF для чата {chat_id}")
        return
    if restore_mode:
        log_info("EDITED: игнор, restore_mode=True")
        return
    update_chat_info_from_message(msg)
    store = get_chat_store(chat_id)
    day_key = today_key()
    target = None
    for day, recs in store.get("daily_records", {}).items():
        for r in recs:
            if r.get("msg_id") == message_id or r.get("origin_msg_id") == message_id:
                target = r
                day_key = day
                break
        if target:
            break
    if not target:
        log_info(f"EDITED: запись не найдена по msg_id={message_id} в daily_records чата {chat_id}")
        return
    log_info(f"EDITED: найдена запись ID={target.get('id')} за день {day_key}")
    try:
        new_amount, new_note = split_amount_and_note(new_text)
    except Exception as e:
        log_error(f"EDITED: ошибка парсинга суммы: {e}")
        bot.send_message(chat_id, "❌ Ошибка: не удалось разобрать сумму.")
        return
    rid = target["id"]
    log_info(f"EDITED: обновляем запись ID={rid}, amount={new_amount}, note='{new_note}'")
    update_record_in_chat(chat_id, rid, new_amount, new_note)
    update_or_send_day_window(chat_id, day_key)
    log_info(f"EDITED: окно дня {day_key} обновлено для чата {chat_id}")
    try:
        refresh_total_message_if_any(chat_id)
        if OWNER_ID and str(chat_id) != str(OWNER_ID):
            try:
                refresh_total_message_if_any(int(OWNER_ID))
            except Exception as e:
                log_error(f"EDITED: refresh_total_message_if_any(owner) error: {e}")
    except Exception as e:
        log_error(f"EDITED: refresh_total_message_if_any({chat_id}) error: {e}")
@bot.message_handler(content_types=["deleted_message"])
def handle_deleted_message(msg):
    try:
        chat_id = msg.chat.id
        store = get_chat_store(chat_id)
        if store.get("reset_wait", False):
            store["reset_wait"] = False
            store["reset_time"] = 0
            save_data(data)
    except:
        pass
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
def start_keep_alive_thread():
    t = threading.Thread(target=keep_alive_task, daemon=True)
    t.start()
@app.route(f"/{BOT_TOKEN}", methods=["POST"])
def telegram_webhook():
    json_str = request.get_data().decode("utf-8")
    try:
        if '"edited_message"' in json_str:
            log_info("WEBHOOK: получен update с edited_message")
    except Exception as e:
        log_error(f"DEBUG webhook edited check error: {e}")
    update = telebot.types.Update.de_json(json_str)
    bot.process_new_updates([update])
    return "OK", 200
def set_webhook():
    if not APP_URL:
        log_info("APP_URL не указан — работаем в режиме polling.")
        return
    wh_url = APP_URL.rstrip("/") + f"/{BOT_TOKEN}"
    bot.remove_webhook()
    time.sleep(0.5)
    bot.set_webhook(url=wh_url)
    log_info(f"Webhook установлен: {wh_url}")
def main():
    global data
    restored = restore_from_gdrive_if_needed()
    data = load_data()
    data["forward_rules"] = load_forward_rules()
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