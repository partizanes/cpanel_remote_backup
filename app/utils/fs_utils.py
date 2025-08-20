import os
import sys

from pathlib import Path
from utils.log import mainLog

from config.const import LOCAL_DIST, LOCAL_DIST_UPLOAD, MYSQL_DUMP_PATH, LOCAL_DIST_ARCHIVE
from utils.date_utils import get_current_date

def make_dir(path: str, exist_ok: bool = True) -> bool:
    """
    Создаёт директорию по указанному пути.

    Параметры:
    - path: путь к директории.
    - exist_ok: если True, не выдаёт ошибку, если директория уже существует.

    Возвращает:
    - True, если директория создана или уже существует.
    - False при ошибке.
    """
    try:
        os.makedirs(path, exist_ok=exist_ok)
        return True
    except Exception as exc:
        mainLog.error(f"[make_dir] {path} {exc.args}")
        return False
    

def create_current_backup_dir():
    mainLog.info("[create_current_backup_dir] Создаем основную директорию резервного копирования с текущей датой.")
    
    path = f"{LOCAL_DIST}/{get_current_date()}"

    if make_dir(path):
        mainLog.debug(f"[create_current_backup_dir] Создана: {path}")


def create_current_upload_dir():
    mainLog.info("[create_current_upload_dir] Создаем директорию для pkgacct с текущей датой")

    path = f"{LOCAL_DIST_UPLOAD}/{get_current_date()}"

    if make_dir(path):
        mainLog.debug(f"[create_current_upload_dir] Создана: {path}")


def create_weekly_backup_dir():
    mainLog.info("[create_weekly_backup_dir] Создаем основную директорию резервного копирования с текущей датой.")
    
    path = f"{LOCAL_DIST}/weekly"

    if make_dir(path):
        mainLog.debug(f"[create_weekly_backup_dir] Создана: {path}")
        return path


def create_monthly_backup_dir():
    mainLog.info("[create_monthly_backup_dir] Создаем основную директорию резервного копирования с текущей датой.")
    
    path = f"{LOCAL_DIST}/monthly"

    if make_dir(path):
        mainLog.debug(f"[create_monthly_backup_dir] Создана: {path}")
        return path


def create_current_mysqldump_dir():
    mainLog.info("[create_current_mysqldump_dir] Создаем директорию резервного копирования для xtrabackup с текущей датой.")

    path =  f"{MYSQL_DUMP_PATH}/{get_current_date()}"

    if make_dir(path):
        mainLog.debug(f"[create_current_mysqldump_dir] Создана: {path}")
        return path


def create_archive_dir():
    mainLog.info("[create_archive_dir] Создаем директорию для архивного хранения аккаунтов.")

    if make_dir(LOCAL_DIST_ARCHIVE):
        mainLog.debug(f"[create_archive_dir] Создана: {LOCAL_DIST_ARCHIVE}")


def get_list_dirs(path) -> list: 
    if os.path.isdir(path):
        with os.scandir(path) as it:
            return [entry.name for entry in it if entry.is_dir()]
    
    mainLog.warning(f"[get_list_dirs] Директория недоступна: {path}")
    return []


def get_list_files(path: str, extension: str = None) -> list:
    """
    Возвращает список файлов в директории path.
    Если указан extension (например, '.tar.gz'), возвращает только файлы с этим расширением.

    :param path: путь к директории
    :param extension: расширение файла с точкой (например, '.tar.gz'), необязательно
    :return: список имен файлов
    """
    if os.path.isdir(path):
        with os.scandir(path) as it:
            if extension:
                return [entry.name for entry in it if entry.is_file() and entry.name.endswith(extension)]
            else:
                return [entry.name for entry in it if entry.is_file()]
    
    mainLog.error(f"[get_list_files][EXCEPTION] Директория недоступна: {path}")
    return []

def get_base_dir() -> Path:
    return Path(sys.argv[0]).resolve().parent.parent