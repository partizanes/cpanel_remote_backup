import subprocess

from utils.log import mainLog

def run_local_command(command: str, timeout: int = 600, capture_output: bool = True):
    """
    Запускает локальную shell-команду.

    Параметры:
    - command: строка команды (shell=True).
    - timeout: время ожидания в секундах.
    - capture_output: если True — возвращает вывод (stdout, stderr).

    Возвращает словарь:
    {
        'success': bool,
        'stdout': str,
        'stderr': str,
        'returncode': int
    }
    """
    try:
        result = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE if capture_output else None,
            stderr=subprocess.PIPE if capture_output else None,
            text=True,
            timeout=timeout,
            check=False
        )

        success = (result.returncode == 0)
        stdout = result.stdout if capture_output else ""
        stderr = result.stderr if capture_output else ""

        mainLog.debug(f"[run_local_command] Cmd: {command}, Return code: {result.returncode}, Success: {success}")
        if not success:
            mainLog.debug(f"[run_local_command] stdout: {stdout}")
            mainLog.debug(f"[run_local_command] stderr: {stderr}")

        return {
            "success": success,
            "stdout": stdout,
            "stderr": stderr,
            "returncode": result.returncode
        }

    except subprocess.TimeoutExpired:
        mainLog.error(f"[run_local_command] Timeout expired for command: {command}")
        return {"success": False, "stdout": "", "stderr": "Timeout expired", "returncode": -1}

    except Exception as e:
        mainLog.error(f"[run_local_command] Exception: {e}")
        return {"success": False, "stdout": "", "stderr": str(e), "returncode": -1}