import requests
from env_config import TELE_TOKEN, TELE_CHAT_ID

def send_telegram_mcx(message):
    if not TELE_TOKEN or not TELE_CHAT_ID or TELE_CHAT_ID == "YOUR_CHAT_ID":
        print(f"MCX Telegram Credentials Missing: {TELE_CHAT_ID}")
        return
        
    url = f"https://api.telegram.org/bot{TELE_TOKEN}/sendMessage"
    payload = {"chat_id": TELE_CHAT_ID, "text": message}
    
    try:
        response = requests.post(url, json=payload)
        json_res = response.json()
        if not json_res.get('ok'):
            print(f"Telegram Error Response: {json_res}")
        else:
            print(f"Telegram Alert Sent Successfully!")
        return json_res
    except Exception as e:
        print(f"Error sending MCX Alert: {e}")
        return None
