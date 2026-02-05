# -*- coding: utf-8 -*-
"""
Скачать нужное вложение из IMAP (папка IMAP_FOLDER) в SAVE_PATH.
Возвращает полный путь к файлу или None.
"""
import imaplib
import email
from email.header import decode_header
import os
import datetime
import logging
import email.utils
import pytz
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

IMAP_SERVER = os.getenv("IMAP_SERVER", "imap.yandex.ru")
EMAIL_ACCOUNT = os.getenv("EMAIL_ACCOUNT")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
FOLDER_NAME = os.getenv("IMAP_FOLDER", "Debitor")
SAVE_PATH = os.getenv("SAVE_PATH", "downloads")

FILTERS = {
    "ТАРА": {
        "subject_contains": ["тара"],
        "attachment_contains": ["Ведомость по переданной возвратной таре"],
        "extensions": [".xlsx"],
        "sender": "supbeer@mail.ru",
    },
    "ДЕБИТОРКА": {
        "subject_contains": ["дз", "дебитор", "дебиторская"],
        "attachment_contains": ["Дебиторская задолженность по срокам долга"],
        "extensions": [".xls", ".xlsx"],
        "sender": "supbeer@mail.ru",
    },
}

def _decode(s: str) -> str:
    if not s:
        return ""
    parts = decode_header(s)
    out = []
    for text, enc in parts:
        if isinstance(text, bytes):
            out.append(text.decode(enc or "utf-8", errors="ignore"))
        else:
            out.append(text)
    return " ".join(out)

def connect_mail() -> imaplib.IMAP4_SSL:
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(EMAIL_ACCOUNT, EMAIL_PASSWORD)
    return mail

def _safe_write(path: str, payload: bytes) -> str:
    """Пишем файл. Если занят (PermissionError) — сохраняем как *_new.ext"""
    try:
        with open(path, "wb") as f:
            f.write(payload)
        return path
    except PermissionError:
        base, ext = os.path.splitext(path)
        new_path = f"{base}_new{ext}"
        logging.warning("Файл занят: %s. Сохраняю как %s", path, new_path)
        with open(new_path, "wb") as f:
            f.write(payload)
        return new_path

def fetch_latest_file(mail_type: str = "ДЕБИТОРКА") -> Optional[str]:
    """mail_type: 'ДЕБИТОРКА' или 'ТАРА'."""
    if mail_type not in FILTERS:
        logging.error("Неизвестный тип письма: %s", mail_type)
        return None

    os.makedirs(SAVE_PATH, exist_ok=True)
    flt = FILTERS[mail_type]
    days_back = 10 if mail_type == "ТАРА" else 3
    date_since = (datetime.date.today() - datetime.timedelta(days=days_back)).strftime("%d-%b-%Y")

    mail = connect_mail()
    try:
        sel = mail.select(FOLDER_NAME)
        if sel[0] != "OK":
            logging.error("Не удалось выбрать папку IMAP: %s", FOLDER_NAME)
            return None

        result, data = mail.search(None, f'(SINCE "{date_since}")')
        if result != "OK":
            logging.error("Ошибка поиска писем.")
            return None

        ids = data[0].split()
        if not ids:
            logging.warning("Нет писем за последние %s дней.", days_back)
            return None

        for num in reversed(ids):  # от новых к старым
            result, msg_data = mail.fetch(num, "(RFC822)")
            if result != "OK" or not msg_data or not msg_data[0]:
                continue
            raw = msg_data[0][1]
            msg = email.message_from_bytes(raw)

            subject = _decode(msg.get("Subject") or "").lower()
            sender = _decode(msg.get("From") or "").lower()

            if flt["sender"].lower() not in sender:
                continue
            if not any(x in subject for x in flt["subject_contains"]):
                continue

            for part in msg.walk():
                fn = part.get_filename()
                if not fn:
                    continue
                fn_dec = _decode(fn)
                ext = os.path.splitext(fn_dec)[1].lower()
                if ext not in flt["extensions"]:
                    continue
                if not any(k.lower() in fn_dec.lower() for k in flt["attachment_contains"]):
                    continue

                path = os.path.join(SAVE_PATH, fn_dec)
                payload = part.get_payload(decode=True)
                path = _safe_write(path, payload)
                logging.info("Скачан файл: %s", path)
                return path

        logging.warning("Подходящих вложений не найдено.")
        return None
    finally:
        try:
            mail.logout()
        except Exception:
            pass
