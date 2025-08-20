import os
import re
import time

from datetime import datetime
from utils.log import mainLog
from utils.date_utils import get_current_date
from utils.backup_utils import get_last_date_path, create_tar_gz_archive
from utils.logging_tools import log_execution
from utils.fs_utils import create_archive_dir, get_list_dirs, get_list_files
from notify.tg import send_telegram_message

from config.const import LOCAL_DIST, LOCAL_DIST_ARCHIVE, ARCHIVE_SERVER_TAG, ARCHIVE_LIFETIME_SECS


def remove_outdated_archive():
    """
        Удаляет архивы из директории LOCAL_DIST_ARCHIVE, которые старше заданного срока хранения ARCHIVE_LIFETIME_SECS.

        Логика работы:
        - Получает список файлов с расширением .tar.gz в архивной директории.
        - Фильтрует файлы по строгому шаблону имени: <слово>.<слово>.<ГГГГ-ММ-ДД>.tar.gz.
        - Проверяет значение ARCHIVE_LIFETIME_SECS из конфигурации, если оно меньше 6 месяцев (15778463 секунд), удаление отменяется с логированием и уведомлением.
        - Для каждого подходящего файла извлекает дату из имени и преобразует в timestamp.
        - Удаляет файлы с датой старше текущего времени минус ARCHIVE_LIFETIME_SECS.
        - Логирует удаление каждого файла.

        Особенности:
        - Защита от случайного удаления при слишком маленьком значении срока хранения.
        - Использует логирование и отправку уведомлений при ошибках конфигурации.
    """

    accounts_archive_list = get_list_files(LOCAL_DIST_ARCHIVE, "tar.gz")

    # Дополнительная проверка названия файла
    account_archive_pattern = re.compile(r'^\w+\.\w+\.\d{4}-\d{2}-\d{2}\.tar\.gz$')
    accounts_archive_list = [f for f in accounts_archive_list if account_archive_pattern.match(f)]

    # (Защита от дурака) значение в конфигурации менее 6 месяцев.
    if int(ARCHIVE_LIFETIME_SECS) < 15778463:
        mainLog.error(f"[remove_outdated_archive] Значение ARCHIVE_LIFETIME_SECS: {ARCHIVE_LIFETIME_SECS} менее 6 месяцев, удаление отменено.")
        send_telegram_message(f"[remove_outdated_archive] Значение ARCHIVE_LIFETIME_SECS: {ARCHIVE_LIFETIME_SECS} менее 6 месяцев, удаление отменено.")
        return
              
    more_then = int(time.time()) - ARCHIVE_LIFETIME_SECS

    for archive in accounts_archive_list:
        archive_timestamp = int(datetime.strptime(re.search('\d{4}-\d{2}-\d{2}', archive).group(), "%Y-%m-%d").timestamp())
        
        if (more_then > archive_timestamp):
            os.remove(f"{LOCAL_DIST_ARCHIVE}/{archive}")
            mainLog.info(f"[remove_outdated_archive] {LOCAL_DIST_ARCHIVE}/{archive} удален.")


@log_execution
def create_account_archive(last_available_backup_path, username):
    """
        Создает tar.gz архив для указанного аккаунта из последней доступной резервной копии.

        :param last_available_backup_path: Путь к директории с последней доступной резервной копией.
        :param username: Имя пользователя (аккаунта), для которого создается архив.
        :return: None. В случае ошибки логирует и отправляет уведомление в Telegram.
    """
    account_source_path = f"{last_available_backup_path}/{username}"
    account_distance_archive_path = f"{LOCAL_DIST_ARCHIVE}/{ARCHIVE_SERVER_TAG}.{username}.{get_current_date()}.tar.gz"

    status = create_tar_gz_archive(account_source_path, account_distance_archive_path)

    if not status:
        mainLog.error(f"[create_account_archive] При создании архива для аккаунта {username} из директории {last_available_backup_path} произошла ошибка.")
        send_telegram_message(f"[create_account_archive] При создании архива для аккаунта {username} из директории {last_available_backup_path} произошла ошибка.")


def get_account_removed_list(last_date_path: str):
    """
        Определяет список удалённых аккаунтов, которых нет в текущей резервной копии, но есть в предыдущей.

        :param last_date_path: Путь к директории с последней доступной резервной копией аккаунтов.
        :return: Множество имён удалённых аккаунтов или None, если списки аккаунтов пусты или не доступны.
    """

    current_date_path = f"{LOCAL_DIST}/{get_current_date()}"

    users_last_day_list = get_list_dirs(last_date_path)
    users_current_day_list = get_list_dirs(current_date_path)

    if not users_last_day_list or not users_current_day_list:
        mainLog.error(f"[get_list_removed_accout] В одной из директорий не обнаружены аккаунты. \n{last_date_path}:{len(users_last_day_list)} \n{current_date_path}:{len(users_current_day_list)}")
        return []

    return set(users_last_day_list) - set(users_current_day_list)


def backup_removed_account():
    """
        Выполняет архивацию удалённых аккаунтов на основе сравнения текущих и предыдущих резервных копий.

        - Создаёт директорию для хранения архивов (если её нет).
        - Определяет путь к последней доступной резервной копии.
        - Получает список аккаунтов, которые были удалены (присутствуют в старой резервной копии, но отсутствуют в текущей).
        - Для каждого удалённого аккаунта создаёт архив.

        Если не удаётся найти последнюю резервную копию, процесс архивации отменяется.

        :return: None
    """
    create_archive_dir()

    # Возвращает последнуюю доступную резервную копию
    last_available_backup_path = get_last_date_path()

    if not last_available_backup_path:
        mainLog.error(f"[backup_removed_account] Не удалось найти последную резервную копию, архивация удаленных аккаунтов отменена.")
        return None

    # Получение списка удаленных аккаунтов.
    accounts_removed_list = get_account_removed_list(last_available_backup_path)

    if accounts_removed_list:
        mainLog.info(f"[backup_removed_account] Обнаружено {len(accounts_removed_list)} удаленных аккаунтов для архивации.")

    # Создает архив для каждого из удаленных аккаунтов.
    for username in accounts_removed_list:
        create_account_archive(last_available_backup_path, username)