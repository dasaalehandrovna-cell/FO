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

# ... (all other imports and initializations in bot.py remain the same)

def update_or_send_day_window(chat_id: int, day_key: str):
    """
    Обновление или отправка окна дня. Для обычных чатов новые окна не создаются,
    если активное окно уже существует. Исключение — OWNER_ID, где логика остаётся прежней.
    """
    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        backup_window_for_owner(chat_id, day_key)
        return

    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)
    mid = get_active_window_id(chat_id, day_key)
    
    if mid:
        try:
            # Если окно существует, то просто обновляем текст и клавиатуру
            bot.edit_message_text(
                txt,
                chat_id=chat_id,
                message_id=mid,
                reply_markup=kb,
                parse_mode="HTML"
            )
            return
        except Exception:
            # Логируем ошибку, но не пытаемся создать новое окно
            log_error(f"Failed to edit message for chat {chat_id}, day {day_key}")
            return

    # Если окна нет — ничего не делаем для обычных чатов
    log_info(f"No active window found for chat {chat_id}, skipping creation.")

# ... (remaining logic in bot.py remains unchanged)
