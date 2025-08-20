from utils.remote_exec import run_ssh_command_on_prod

def remote_dir_exists(path: str) -> bool:
    result = run_ssh_command_on_prod(f'test -d "{path}"')
    return result["success"]