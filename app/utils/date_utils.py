from datetime import datetime, timedelta

startDate = datetime.today()

def get_last_date() -> str:
    """Возвращает дату предыдущего дня в формате YYYY-MM-DD"""
    return get_sub_day_date(1)

def get_current_date() -> str:
    """Возвращает дату запуска скрипта в формате YYYY-MM-DD"""
    return startDate.date().isoformat()

def get_sub_day_date(day: int) -> str:
    """Возвращает дату (дата запуска - day) в формате YYYY-MM-DD"""
    return (startDate - timedelta(days=day)).date().isoformat()
