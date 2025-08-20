from utils.log import mainLog
from datetime import datetime
from functools import wraps

def log_execution(func):
    """
        Декоратор для логирования запуска и завершения выполнения функции.

        Формат логов:
        - [имя_функции][user] Запуск
        - [имя_функции][user] Завершено. Время выполнения: ...

        Если первый аргумент функции имеет атрибут `user`, он добавляется в лог.
        В противном случае `user` опускается.

        Пример:
            @log_execution
            def do_something(self):
                ...

        Подходит для методов классов, где `self.user` определён, и для обычных функций.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        obj = args[0] if args else None
        user = getattr(obj, 'user', None)
        tag = func.__name__
        user_str = f"[{user}]" if user else ""
        start_time = datetime.now()
        mainLog.debug(f"[{tag}] {user_str} Запуск")
        result = func(*args, **kwargs)
        duration = datetime.now() - start_time
        mainLog.debug(f"[{tag}] {user_str} Завершено. Время выполнения: {duration}")
        return result
    return wrapper