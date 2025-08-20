import inspect

from utils.log import mainLog
from notify.tg import send_telegram_message
from config.const import BACKUP_SERVER

def get_current_func_name():
    return inspect.currentframe().f_back.f_code.co_name

def log_and_send(func_name, exc, message=None):
    try:
        mainLog.error(f"[{func_name}][EXCEPTION] {exc.args} {message}")
        send_telegram_message(f"[{func_name}][EXCEPTION] {exc.args} {message}")
    except Exception as exc:
        mainLog.critical(f"[log_and_send] {exc.args}")
