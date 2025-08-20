
from utils.log import mainLog

from config.const import (
    BACKUP_SERVER,
    BACKUP_SERVER_PORT,
    BACKUP_SERVER_SFTP_DIR,
    REMOTE_SERVER_MOUNT_DIR
)

from notify.tg import send_telegram_message
from utils.remote_exec import run_ssh_command_on_prod

def create_mount_dir() -> bool:
    cmd = f"/bin/mkdir -p {REMOTE_SERVER_MOUNT_DIR}"

    result = run_ssh_command_on_prod(cmd)

    if not result['success']:
        mainLog.error(f"[create_mount_dir] Не удалось создать директорию на производственном сервере: {REMOTE_SERVER_MOUNT_DIR}")
        send_telegram_message(f"[create_mount_dir] Не удалось создать директорию на производственном сервере: {REMOTE_SERVER_MOUNT_DIR}")
        exit(1)


### MOUNT ###
def is_sshfs_mounted(path):
    result = run_ssh_command_on_prod(f"mountpoint -q {path}")
    return result['success']


### Подкючается через ssh с использованием ключа со стандартным именем , расположенного в домашней директории пользователя
### и иницириует монтирование с удаленного сервера, на сервер резервного копирования с использованием ключа (на удаленном сервере)
### В данной цепочке используется 2 разных ключа, для подключения к серверу и для монтирования 
def mount_over_ssh():
    mainLog.info("[mount_over_ssh] Монтируем sshfs раздел.")

    # Создаем директорию для монтирования
    create_mount_dir()
    
    # Проверяем что она еще не примонтирована 
    if is_sshfs_mounted(REMOTE_SERVER_MOUNT_DIR):
        return True
    
    cmd = f"sshfs {BACKUP_SERVER}:{BACKUP_SERVER_SFTP_DIR} {REMOTE_SERVER_MOUNT_DIR} -p {BACKUP_SERVER_PORT} -o nonempty"

    result = run_ssh_command_on_prod(cmd)

    if result['success']:
        mainLog.info("[mount_over_ssh] sshfs смонтирован.")
        return True
    else:
        mainLog.error(f"[mount_over_ssh][Exception] {result}")
        send_telegram_message(f"[mount_over_ssh][Exception] {result}")
        exit()


### Размонтирование по аналогичной схеме
def umount_over_ssh():
    mainLog.info("[umount_over_ssh] Размонтируем sshfs раздел.")

    if not is_sshfs_mounted(REMOTE_SERVER_MOUNT_DIR):
        mainLog.warning("[umount_over_ssh] sshfs уже размонтирован.")
        return True

    result = run_ssh_command_on_prod(f'fusermount -u {REMOTE_SERVER_MOUNT_DIR}')

    if(result['success']):
        mainLog.info("[umount_over_ssh] sshfs размонтирован.")
        return True
    else:
        mainLog.error(f"[umount_over_ssh][ERROR] {result}")
        return False
################