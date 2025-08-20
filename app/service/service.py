import logging

from utils.log import mainLog
from datetime import date, datetime, timedelta
from time import time, sleep
from multiprocessing import Process
from notify.tg import send_telegram_message

from utils.logging_tools import log_execution
from remote.cpanel.account import CpanelAccount

from utils.backup_utils import get_last_date_path
from utils.date_utils import get_current_date, get_last_date
from utils.fs_utils import create_weekly_backup_dir, create_monthly_backup_dir
from utils.remote_exec import run_ssh_command_on_prod
from utils.local_exec import run_local_command

from database.xtrabackup import create_mysql_xtrabackup

from tenacity import retry, stop_after_attempt, retry_if_result, wait_random, before_sleep_log

from config.const import (
    LOCAL_DIST, 
    REMOTE_SERVER_MOUNT_DIR, 
    LOCAL_DIST_UPLOAD, 
    REMOTE_SERVER, 
    REMOTE_SSH_PORT,
    PKGACCT_TIMEOUT,
    RSYNC_HOMEDIR_TIMEOUT,
    EXCLUDE_DIR,
    RSYNC_HOMEDIR_ERR_EXCLUDE,
    RSYNC_SUSPENDED_ERR_EXCLUDE
)

#### PKGACCT ####
@log_execution
@retry(stop=stop_after_attempt(5), wait=wait_random(min=30, max=90), retry=retry_if_result(lambda x: x is False), before_sleep=before_sleep_log(mainLog, logging.ERROR))
def pre_clean_pkgacct(account: CpanelAccount) -> bool:
    """
    Удаляет временные данные резервного копирования для аккаунта cPanel на удалённом сервере.

    В случае ошибки логирует stdout/stderr и отправляет уведомление в Telegram.

    :param account: объект CpanelAccount с информацией об аккаунте
    :return: True при успешном удалении, иначе False
    
    """
    username = account.user

    pkgacct_src = f"{LOCAL_DIST_UPLOAD}/{get_current_date()}/{username}"

    cmd = f"/bin/rm -rf {pkgacct_src}"

    result = run_local_command(cmd, PKGACCT_TIMEOUT)

    if not result['success']:
        mainLog.error(f"[pre_clean_pkgacct] [{username}] завершился с ошибкой. stdout: {result['stdout']} stderr: {result['stderr']}")
        send_telegram_message(f"[pre_clean_pkgacct] [{username}] завершился с ошибкой. stderr: {result['stderr']}")
        return False

    #mainLog.info(f"[pre_clean_pkgacct] [{username}] Завершен успешно.")
    return True


@log_execution
@retry(stop=stop_after_attempt(5), wait=wait_random(min=60, max=180), retry=retry_if_result(lambda x: x is False), before_sleep=before_sleep_log(mainLog, logging.WARNING))
def run_pkgacct(account: CpanelAccount) -> bool:
    """
        Запускает резервное копирование аккаунта cPanel на удалённом сервере с помощью pkgacct.

        Без копирования домашнего каталога, квот, логов и трафика.
        Бэкап сохраняется в директорию, соответствующую текущей дате.

        В случае ошибки логирует stdout/stderr и отправляет сообщение в Telegram.

        :param username: имя пользователя cPanel
        :return: True при успешном завершении, иначе False
    """
    username = account.user

    pkgacct_current_backup_path = f"{REMOTE_SERVER_MOUNT_DIR}/{get_current_date()}/"

    pre_clean_pkgacct(account)

    cmd = f"/bin/timeout {PKGACCT_TIMEOUT} /usr/local/cpanel/scripts/pkgacct --skiphomedir --skipquota --skiplogs --skipbwdata --backup --incremental {username} {pkgacct_current_backup_path}"
    
    result = run_ssh_command_on_prod(cmd, PKGACCT_TIMEOUT)

    if not result['success']:
        mainLog.error(f"[run_pkgacct] [{username}] завершился с ошибкой. stdout: {result['stdout']} stderr: {result['stderr']}")
        send_telegram_message(f"[run_pkgacct] [{username}] завершился с ошибкой. stderr: {result['stderr']}")
        return False
    
    mainLog.info(f"[run_pkgacct] [{username}] Завершен успешно.")
    return True

@log_execution
def move_pkgacct_with_hardlinks(account: CpanelAccount) -> bool:
    """
        Перемещает результат pkgacct из временной директории загрузки в постоянное хранилище с использованием hardlink'ов.

        - Источник: upload/<дата>/<user>
        - Назначение: backup/<дата>/<user>
        - Используется rsync с --link-dest для экономии места при неизменённых файлах.

        Если операция завершается с ошибкой — отправляется уведомление и пишется лог.

        :param account: Объект CpanelAccount с полем user
        :return: True при успехе, False при ошибке
    """
    username = account.user

    pkgacct_linkdest = f"{LOCAL_DIST}/{get_last_date()}/{username}"
    pkgacct_src = f"{LOCAL_DIST_UPLOAD}/{get_current_date()}/{username}/"
    pkgacct_dest = f"{LOCAL_DIST}/{get_current_date()}/{username}/"

    cmd = f"rsync -rlpgoD -c --delete --link-dest={pkgacct_linkdest} --exclude=homedir {pkgacct_src} {pkgacct_dest}"

    result = run_local_command(cmd, 36000)

    if not result['success']:
        mainLog.error(f"[run_pkgacct_move] [{username}] завершился с ошибкой. stdout: {result['stdout']} stderr: {result['stderr']}")
        send_telegram_message(f"[run_pkgacct_move] [{username}] завершился с ошибкой. stderr: {result['stderr']}")
        return False
    
    mainLog.info(f"[run_pkgacct_move] [{username}] Завершен успешно.")
    return True

###############


#### RSYNC ####
@log_execution
def run_rsync_suspended(account: CpanelAccount) -> bool:

    last_date_path = get_last_date_path(account)

    if not last_date_path:
        return False
   
    acc_backup_src      = f"{last_date_path}/"
    acc_backup_dest     = f"{LOCAL_DIST}/{get_current_date()}/{account.user}/"

    cmd = f"rsync -a --delete --link-dest={acc_backup_src} {acc_backup_src} {acc_backup_dest}"

    result = run_local_command(cmd, RSYNC_HOMEDIR_TIMEOUT)

    if not result['success'] and result.get('returncode') not in RSYNC_SUSPENDED_ERR_EXCLUDE:
        mainLog.error(f"[run_rsync_suspended] [{account.user}] завершился с ошибкой. stdout: {result['stdout']} stderr: {result['stderr']}")
        send_telegram_message(f"[run_rsync_suspended] [{account.user}] завершился с ошибкой. stderr: {result['stderr']}")
        return False
    
    return True

@log_execution
@retry(stop=stop_after_attempt(5), wait=wait_random(min=60, max=180), retry=retry_if_result(lambda x: x is False), before_sleep=before_sleep_log(mainLog, logging.WARNING))
def run_rsync_homedir(account: CpanelAccount) -> bool:
    last_date_path = get_last_date_path(account, "homedir")

    link_dest = f"--link-dest={last_date_path}" if last_date_path else ""
    exclude_from = f"--exclude-from={EXCLUDE_DIR[account.user]}" if account.user in EXCLUDE_DIR else ""

    cmd = f"/usr/bin/rsync -a --delete -e 'ssh -p {REMOTE_SSH_PORT}' {exclude_from} {link_dest} {REMOTE_SERVER}:/{account.partition}/{account.user}/ {LOCAL_DIST}/{get_current_date()}/{account.user}/homedir/"
    
    result = run_local_command(cmd, 36000)

    if not result['success'] and result.get('returncode') not in RSYNC_HOMEDIR_ERR_EXCLUDE:
        mainLog.error(f"[run_rsync_homedir] [{account.user}] завершился с ошибкой. stdout: {result['stdout']} stderr: {result['stderr']}")
        send_telegram_message(f"[run_rsync_homedir] [{account.user}] завершился с ошибкой. stderr: {result['stderr']}")
        return False
    
    return True

###############


#### AdditionalCopy ####
@log_execution
def create_weekly_copy() -> bool:
    weekly_dir_path = create_weekly_backup_dir()

    if not weekly_dir_path:
        mainLog.error(f"[create_weekly_copy] Не удалось создать директорию для сохранения недельной копии.")
        send_telegram_message("[create_weekly_copy] Не удалось создать директорию для сохранения недельной копии.")
        return False

    # Важен / в конце path
    source_link_dest = f"{LOCAL_DIST}/{get_current_date()}"
    current_backup_path = f"{LOCAL_DIST}/{get_current_date()}/"
    weekly_backup_path = f"{weekly_dir_path}/{get_current_date()}/"

    cmd = f"rsync -a --delete --link-dest={source_link_dest} {current_backup_path} {weekly_backup_path}"

    result = run_local_command(cmd, 36000)

    if not result['success']:
        mainLog.error(f"[create_weekly_copy] Создание недельной копии завершилось с ошибкой. stderr: {result['stderr']}")
        send_telegram_message(f"[create_weekly_copy] Создание недельной копии завершилось с ошибкой. stderr: {result['stderr']}")
        return False
    
    return True


@log_execution
def create_monthly_copy() -> bool:
    monthly_dir_path = create_monthly_backup_dir()

    if not monthly_dir_path:
        mainLog.error(f"[create_monthly_copy] Не удалось создать директорию для сохранения месячной копии.")
        send_telegram_message("[create_monthly_copy] Не удалось создать директорию для сохранения месячной копии.")
        return False

    # Важен / в конце path
    source_link_dest = f"{LOCAL_DIST}/{get_current_date()}"
    current_backup_path = f"{LOCAL_DIST}/{get_current_date()}/"
    monthly_backup_path = f"{monthly_dir_path}/{get_current_date()}/"

    cmd = f"rsync -a --delete --link-dest={source_link_dest} {current_backup_path} {monthly_backup_path}"

    result = run_local_command(cmd, 36000)

    if not result['success']:
        mainLog.error(f"[create_monthly_copy] Создание месячной копии завершилось с ошибкой. stderr: {result['stderr']}")
        send_telegram_message(f"[create_monthly_copy] Создание месячной копии завершилось с ошибкой. stderr: {result['stderr']}")
        return False
    
    return True

def create_additional_copy():

    today = date.today()

    # Каждые 3 дня создаём дамп MySQL
    if today.day % 3 == 0:
        create_mysql_xtrabackup()
    
    # Каждое воскресенье — недельный бэкап
    if today.weekday() == 6:

        if create_weekly_copy():
            mainLog.debug(f"[create_additional_copy] Создание недельной копии успешно завершено.")

    # Первое число месяца — месячный бэкап
    if today.day == 1:

        if create_monthly_copy():
            mainLog.debug(f"[create_additional_copy] Создание месячной копии успешно завершено.")


@log_execution
def run_account_backup(account):
    try:
        ## Если аккаунт приостановлен, то делаем копию предущего дня
        if int(account.suspended) and run_rsync_suspended(account):
            return True

        ## PKGACCT STAGE ##
        if run_pkgacct(account):
            move_pkgacct_with_hardlinks(account)
        ## PKGACCT END ##

        ## HOMEDIR STAGE ##
        run_rsync_homedir(account)
        ## HOMEDIR END ##
    
    except Exception as exc:
        mainLog.error(f"[run_account_backup][EXCEPTION] {exc.args}")
        send_telegram_message(f"[run_account_backup] {exc.args}")


def processing_account_data(accounts_data, shared_report_dict):
    proc_count = []
    current = 0
    total = len(accounts_data)

    for account in accounts_data:
        user_obj = accounts_data[account][0]
        current += 1

        user = user_obj.user
        partition = user_obj.partition

        # Ограничение по числу процессов
        while len(proc_count) >= 2:
            for proc, start_time in proc_count[:]:
                if not proc.is_alive():
                    shared_report_dict[proc.name] = timedelta(seconds=int((datetime.now() - start_time).total_seconds()))
                    proc_count.remove((proc, start_time))
                    proc.join()
                    break

            sleep(0.5)

        mainLog.info(f"[{current}/{total}] [{partition}] Обработка аккаунта: {user}")

        proc = Process(target=run_account_backup, args=(user_obj,), name=user)
        proc_count.append((proc, datetime.now()))
        proc.start()

    # Ожидание завершения оставшихся потоков
    while len(proc_count) > 0: 
        for proc, start_time in proc_count:
            if not proc.is_alive():
                shared_report_dict[proc.name] = timedelta(seconds=int((datetime.now() - start_time).total_seconds()))
                proc_count.remove((proc, start_time))
                proc.join()
                break