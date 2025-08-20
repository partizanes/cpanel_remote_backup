import sys
import subprocess
import threading
import logging

from pathlib import Path
from utils.log import mainLog
from utils.fs_utils import create_current_mysqldump_dir, get_base_dir
from utils.date_utils import get_current_date
from utils.logging_tools import log_execution

from remote.fs_utils import remote_dir_exists
from notify.tg import send_telegram_message

from config.const import MYSQL_DUMP_ENABLE, MYSQL_PATH, REMOTE_SERVER, REMOTE_SSH_PORT, MYSQL_DUMP_OPTIONS
from tenacity import retry, stop_after_attempt, retry_if_result, wait_random, before_sleep_log

# Для вывода stdout
def print_stream(stream, prefix):
    for line in iter(stream.readline, b''):
        print(f"{prefix}: {line.decode().rstrip()}")

# Для записи в файл stdout 
def write_stream_to_file(stream, filepath):
    with open(filepath, "ab") as f:
        for line in iter(stream.readline, b''):
            f.write(line)


@log_execution
def run_xtrabackup_stream(mysql_dump_path):

    additional_args = MYSQL_DUMP_OPTIONS or ""
    remote_cmd = f"/usr/bin/mariabackup --backup --compress --compress-threads=8 {additional_args} --stream=xbstream --datadir={MYSQL_PATH}"

    ssh_cmd = ["ssh", "-p", str(REMOTE_SSH_PORT), REMOTE_SERVER, remote_cmd]
	
    xbstream_cmd = ["/usr/bin/mbstream", "-x", "-C", mysql_dump_path]

    try:
        ssh_proc = subprocess.Popen(
            ssh_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        xbstream_proc = subprocess.Popen(
            xbstream_cmd,
            stdin=ssh_proc.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        # Для отладочных целей возможно писать лог из mariabackup в stdout или в файл
        #ssh_stderr_thread = threading.Thread(target=print_stream, args=(ssh_proc.stderr, "[mariabackup]"))
        ssh_stderr_thread = threading.Thread(target=write_stream_to_file, args=(ssh_proc.stderr, f"{get_base_dir()}/logs/xtrabackup-{get_current_date()}.log"))
        ssh_stderr_thread.start()

        out, err = xbstream_proc.communicate()

        ssh_stderr_thread.join()
        ssh_proc.wait()

        return {
            "success": xbstream_proc.returncode == 0 and ssh_proc.returncode == 0,
            "stdout": out.decode() if out else "",
            "stderr": err.decode() if err else ""
        }

    except Exception as e:
        return {"success": False, "stdout": "", "stderr": str(e)}


@log_execution
@retry(stop=stop_after_attempt(3), wait=wait_random(min=120, max=240), retry=retry_if_result(lambda x: x is False), before_sleep=before_sleep_log(mainLog, logging.WARNING))
def create_mysql_xtrabackup():

    if not MYSQL_DUMP_ENABLE:
        mainLog.info("[createMysqlCopy] Функция создания резервной копии всех баз данных отключена в конфигурации")
        return None

    if not remote_dir_exists(MYSQL_PATH):
        mainLog.error(f"[createMysqlCopy] {MYSQL_PATH} не существует на производственном сервере. Создание дампа отменено.")
        send_telegram_message(f"[createMysqlCopy] {MYSQL_PATH} не существует на производственном сервере. Создание дампа отменено.")
        return None
    
    xtrabackup_cur_path = create_current_mysqldump_dir()

    if not xtrabackup_cur_path:
        send_telegram_message(f"[createMysqlCopy] Произошла ошибка при создании директории для xtrabackup. Создание дампа отменено.")
        return None

    result = run_xtrabackup_stream(xtrabackup_cur_path)

    if not result["success"]:
        mainLog.error(f"[createMysqlCopy] Результат создания xtrabackup копии завершился с ошибокой. stderr: {result['stderr']}")
        send_telegram_message(f"[createMysqlCopy] Результат создания xtrabackup копии завершился с ошибокой. stderr: {result['stderr']}")
        # TODO Тут нужна реализация флага, что бы если копия завершилось с ошибкой , то во время очистки не удалять последную копию. 
        # Самой простой реализацией может быть создание текстового файла и удаление его в случае успеха.
        return False
    
    mainLog.info("[createMysqlCopy] Успешно завершен.")
    return True