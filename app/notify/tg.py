import requests

from config.const import BACKUP_SERVER, TG_BOT_TOKEN,TG_BOT_GROUP

def send_telegram_message(message: str, bot_token: str = TG_BOT_TOKEN, chat_id: str = TG_BOT_GROUP) -> bool:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": f"[{BACKUP_SERVER}]{message}"
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        return response.ok
    except requests.RequestException:
        return False