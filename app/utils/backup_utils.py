import os
import tarfile

from config.const import LOCAL_DIST
from utils.log import mainLog
from notify.tg import send_telegram_message
from utils.local_exec import run_local_command
from remote.cpanel.account import CpanelAccount
from utils.date_utils import get_sub_day_date


def get_last_date_path(account: CpanelAccount = None, specific_dir: str = None) -> str:
    """
    Ищет последний существующий путь к директории резервной копии аккаунта
    за последние 9 дней (от вчерашнего дня). 

    Можно указать дополнительный подкаталог внутри резервной копии (например, 'homedir').

    :param account: Объект аккаунта CpanelAccount, может быть None.
    :param specific_dir: Название подкаталога внутри резервной копии или None.
    :return: Путь к последней найденной директории или None, если такой нет.
    """
    for x in range(1, 10):
        additional_account = f"/{account.user}" if account else ""
        addition_path = f"/{specific_dir}" if specific_dir else ""
        preview_path = f"{LOCAL_DIST}/{get_sub_day_date(x)}{additional_account}{addition_path}"

        if os.path.isdir(preview_path):
            return preview_path


def create_tar_gz_archive(source_dir: str, archive_path: str) -> bool:
    """
    Создаёт tar.gz архив из содержимого source_dir и сохраняет его в archive_path
    с помощью внешнего tar --ignore-failed-read.

    :param source_dir: Путь к директории, содержимое которой нужно архивировать.
    :param archive_path: Путь к создаваемому архиву (.tar.gz).
    :return: True при успехе, False при ошибке.
    """
    if not os.path.isdir(source_dir):
        mainLog.error(f"[create_tar_gz_archive] Ошибка: директория {source_dir} не существует")
        send_telegram_message(f"[create_tar_gz_archive] Ошибка: директория {source_dir} не существует")
        return False

    parent_dir = os.path.dirname(source_dir)
    base_name = os.path.basename(source_dir)
    cmd = f"tar czf {archive_path} --ignore-failed-read -C {parent_dir} {base_name}"

    result = run_local_command(cmd)

    if not result["success"]:
        mainLog.error(f"[create_tar_gz_archive] Ошибка при создании архива {archive_path} из {source_dir}: {result['stderr']}")
        send_telegram_message(f"[create_tar_gz_archive] Ошибка при создании архива {archive_path} из {source_dir}")
        return False

    return True