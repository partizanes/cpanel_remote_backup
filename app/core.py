from datetime import datetime
from utils.log import mainLog

from notify.mail import alertToSupport
from report.report import get_total_report
from cleanup.cleanup import cleanup_outdated_backups
from utils.disk_utils import check_free_space
from remote.cpanel.api import get_account_dict
from remote.sshfs import mount_over_ssh, umount_over_ssh
from archive.archive import backup_removed_account, remove_outdated_archive
from utils.fs_utils import create_current_backup_dir, create_current_upload_dir, get_base_dir
from service.service import processing_account_data, create_additional_copy

from multiprocessing import Process, Manager

from config.const import RESELLER

# DEBUG TIMER START
startTime = datetime.now()

# Создаем директорию резервного копирования с текущей датой
create_current_backup_dir()

# Для pkgacct over ssh с текущей датой
create_current_upload_dir()

# Проверяем наличие свободного места
check_free_space()

# Монтируем sshfs зависимость для pkgacct
mount_over_ssh()

# Получаем список аккаунтов RESELLER сгрупированных по разделу
acc_partition_list = get_account_dict(RESELLER)

# Мультипроцессорный dict для сбора информации в отчет
manager = Manager()
shared_report_dict = manager.dict()

procs = []

mainLog.info("[MAIN] Запускаем каждый найденый раздел в отдельном процессе.")

for partition in acc_partition_list:
    proc = Process(target=processing_account_data, args=(acc_partition_list[partition], shared_report_dict,))
    procs.append(proc)
    proc.start()

for proc in procs:
    proc.join()

# Дополнительные резервные копии
create_additional_copy()

# Архивация удаленных аккаунтов
backup_removed_account()

# Удаление устаревших архивов (Зависит от значения в конфигурации ARCHIVE_LIFETIME_SECS)
remove_outdated_archive()

# Удаленние устаревших данных резервных копий
cleanup_outdated_backups()

# Считаем время исполнения скрипта
executionTime = datetime.now() - startTime

# Генерируем отчет
report = get_total_report(shared_report_dict, executionTime)
alertToSupport("Система резервного копирования", htmlText=report)

# DEBUG
with open(f"{get_base_dir()}/logs/report.html", "w") as f:
    f.write(report)

# Проводим размонтирование раздела sshfs
umount_over_ssh()

# # DEBUG TIMER END
mainLog.info(f"[MAIN] Скрипт завершен. Время выполнения: {executionTime}")