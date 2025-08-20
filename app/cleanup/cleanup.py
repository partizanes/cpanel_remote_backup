import re
import sys
import time

from pathlib import Path
from datetime import datetime
from utils.log import mainLog
from utils.fs_utils import get_base_dir, get_list_dirs
from utils.local_exec import run_local_command
from utils.logging_tools import log_execution


from config.const import LOCAL_DIST, MYSQL_DUMP_PATH, LOCAL_DIST_UPLOAD
from config.const import (
    DAILY_BACKUP_DAYS_LIMIT,
    DAILY_BACKUP_MIN_COUNT,
    WEEKLY_BACKUP_DAYS_LIMIT,
    WEEKLY_BACKUP_MIN_COUNT,
    MONTHLY_BACKUP_DAYS_LIMIT,
    MONTHLY_BACKUP_MIN_COUNT,
    MYSQL_XTRABACKUP_DAYS_LIMIT,
    MYSQL_XTRABACKUP_MIN_COUNT
)

def collect_outdated_dirs(path: str, days_limit: int, min_count: int = 6) -> list:
    """
        Собирает список директорий с названиями в формате 'YYYY-MM-DD' в указанном пути, 
        которые старше заданного лимита дней.

        Удаление не производится, если количество директорий меньше или равно min_count — 
        это предотвращает случайное удаление при нехватке резервных копий.

        :param path: Путь к директории с бэкапами.
        :param days_limit: Лимит возраста файлов в днях — директории старше этого значения будут помечены как устаревшие.
        :param min_count: Минимальное количество директорий, при котором разрешается сбор устаревших (по умолчанию 6).
        :return: Список полных путей к устаревшим директориям.
    """
    backup_dir_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    all_dirs = [f for f in get_list_dirs(path) if backup_dir_pattern.match(f)]

    if len(all_dirs) <= min_count:
        mainLog.debug(f"[collect_outdated_dirs] Кол-во копий в {path} ≤ {min_count}. Очистка отменена.")
        return []

    outdated_dirs = []
    time_limit = int(time.time()) - (days_limit * 86400)

    for dir_name in all_dirs:
        try:
            dir_timestamp = int(datetime.strptime(dir_name, '%Y-%m-%d').timestamp())
            if dir_timestamp < time_limit:
                outdated_dirs.append(f"{path}/{dir_name}")
        except Exception as e:
            mainLog.warning(f"[collect_outdated_dirs] Ошибка разбора даты в '{dir_name}': {e}")

    return outdated_dirs

@log_execution
def cleanup_outdated_backups():
    """ Собирает и удаляет устаревшие daily/weekly/monthly директории. """

    dirs_to_delete = []
    dirs_to_delete += collect_outdated_dirs(MYSQL_DUMP_PATH, days_limit=MYSQL_XTRABACKUP_DAYS_LIMIT, min_count=MYSQL_XTRABACKUP_MIN_COUNT)     # xtrabackup > 1 дня
    dirs_to_delete += collect_outdated_dirs(LOCAL_DIST_UPLOAD, days_limit=-1, min_count=-1)                                                    # upload     Remove all
    dirs_to_delete += collect_outdated_dirs(LOCAL_DIST, days_limit=DAILY_BACKUP_DAYS_LIMIT, min_count=DAILY_BACKUP_MIN_COUNT)                  # daily      > 5 дней
    dirs_to_delete += collect_outdated_dirs(f"{LOCAL_DIST}/weekly", days_limit=WEEKLY_BACKUP_DAYS_LIMIT, min_count=WEEKLY_BACKUP_MIN_COUNT)    # weekly     > 8 дней
    dirs_to_delete += collect_outdated_dirs(f"{LOCAL_DIST}/monthly", days_limit=MONTHLY_BACKUP_DAYS_LIMIT, min_count=MONTHLY_BACKUP_MIN_COUNT) # monthly    > 28 дня

    if dirs_to_delete:
        full_list_str = ' '.join(dirs_to_delete)
        mainLog.info(f"[cleanup_archives] Удаляем директории: {full_list_str}")

        # Удаление данных с низким приоритетом
        cmd_remove_all = f"/bin/ionice -c3 /bin/rm -rf {full_list_str}"
        # Поиск директорий с неправильными правами и добавления необходимых прав
        cmd_fix_dirs_chmod = f"/bin/find {full_list_str} -type d \\( ! -perm -200 -o ! -perm -100 \\) -exec chmod u+rwx {{}} +"
        # Поиск файлов с неправильными правами и добавления необходимых прав
        cmd_fix_files_chmod = f"/bin/find {full_list_str} -type f ! -perm -200 -exec chmod u+rw {{}} +"

        # Запускаем сначала удаление , после чего поиск директорий и файлов с неправильными правами и повторно удаляем. 
        run_local_command(f"/bin/nohup bash -c '{cmd_remove_all}; {cmd_fix_dirs_chmod}; {cmd_fix_files_chmod}; {cmd_remove_all}' > {get_base_dir()}/logs/remove_error.log 2>&1 &")

