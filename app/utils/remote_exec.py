import subprocess

from utils.log import mainLog

from config.const import REMOTE_SERVER, REMOTE_SSH_PORT

def run_ssh_command_on_prod(command: str, timeout: int = 300):
    """
    Запускает команду на удалённом сервере через SSH.

    Возвращает словарь с ключами:
    - 'success' (bool): True, если команда выполнилась успешно (код 0).
    - 'stdout' (str): стандартный вывод.
    - 'stderr' (str): стандартный поток ошибок.

    Аргумент command должен быть строкой — команда, которую нужно выполнить.
    """
    return run_ssh_command(REMOTE_SERVER, REMOTE_SSH_PORT, command, timeout)


def run_ssh_command(server: str, port: int, command: str, timeout: int = 300):
    """
    Запускает команду на удалённом сервере через SSH.

    Возвращает словарь с ключами:
    - 'success' (bool): True, если команда выполнилась успешно (код 0).
    - 'stdout' (str): стандартный вывод.
    - 'stderr' (str): стандартный поток ошибок.

    Аргумент command должен быть строкой — команда, которую нужно выполнить.
    """
    ssh_cmd = [
        "ssh",
        "-p", str(port),
        server,
        command
    ]

    try:
        result = subprocess.run(
            ssh_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
            check=False
        )

        success = (result.returncode == 0)

        mainLog.debug(f"[run_ssh_command] Server: {server}, Cmd: {command}, Return code: {result.returncode}")

        return {
            "success": success,
            "stdout": result.stdout,
            "stderr": result.stderr
        }

    except subprocess.TimeoutExpired:
        mainLog.error(f"[run_ssh_command] Timeout expired for command: {command}")
        return {"success": False, "stdout": "", "stderr": "Timeout expired"}

    except Exception as e:
        mainLog.error(f"[run_ssh_command] Exception: {e}")
        return {"success": False, "stdout": "", "stderr": str(e)}