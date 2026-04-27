import requests

from env_config import (
    FUTURE_SCANNER_CHAT_ID,
    FUTURE_SCANNER_TELE_TOKEN,
    PAPER_TRADE_CHAT_ID,
    PAPER_TRADE_TELE_TOKEN,
    TELE_CHAT_ID,
    TELE_TOKEN,
)


def send_telegram_message(message, token=None, chat_id=None):
    token = token or TELE_TOKEN
    chat_id = chat_id or TELE_CHAT_ID
    if not token or not chat_id or chat_id == "YOUR_CHAT_ID":
        print(f"Telegram credentials missing: {chat_id}")
        return None

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message}

    try:
        response = requests.post(url, json=payload, timeout=30)
        json_res = response.json()
        if not json_res.get("ok"):
            print(f"Telegram error response: {json_res}")
        else:
            print("Telegram alert sent successfully.")
        return json_res
    except Exception as e:
        print(f"Error sending Telegram alert: {e}")
        return None


def send_telegram_mcx(message):
    return send_telegram_message(message, token=TELE_TOKEN, chat_id=TELE_CHAT_ID)


def send_future_scanner_alert(message):
    if not FUTURE_SCANNER_TELE_TOKEN or not FUTURE_SCANNER_CHAT_ID:
        print("Future scanner Telegram credentials missing.")
        return None

    return send_telegram_message(
        message,
        token=FUTURE_SCANNER_TELE_TOKEN,
        chat_id=FUTURE_SCANNER_CHAT_ID,
    )


def send_paper_trade_alert(message):
    return send_telegram_message(
        message,
        token=PAPER_TRADE_TELE_TOKEN,
        chat_id=PAPER_TRADE_CHAT_ID,
    )
