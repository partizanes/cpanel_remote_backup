import shutil

from utils.log import mainLog
from notify.tg import send_telegram_message

from config.const import LOCAL_DIST


def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def check_free_space():
    disk_info = shutil.disk_usage(LOCAL_DIST)

    total = disk_info[0]
    used  = disk_info[1]
    free  = disk_info[2]

    used_perc = round(100 * used / total)
    free_perc = round(100 * free / total)

    if(free_perc < 6):

        mainLog.error("[check_free_space] Недостаточно свободного места: \nВсего: {0:>20} [100%] \nИспользовано: {1:>13} [{3}%] \nСвободно: {2:>17} [{4}%]".format
                      (sizeof_fmt(total), sizeof_fmt(used), sizeof_fmt(free), used_perc, free_perc))
        
        send_telegram_message(f"[check_free_space] Недостаточно свободного места для проведения резервного копирования: Свободно: {sizeof_fmt(free)}")

        exit()
    else:
        mainLog.info("[check_free_space] Проверка наличия свободного места завершена успешно: \nВсего: {0:>20} [100%] \nИспользовано: {1:>13} [{3}%] \nСвободно: {2:>17} [{4}%]".format
                     (sizeof_fmt(total), sizeof_fmt(used), sizeof_fmt(free), used_perc, free_perc))