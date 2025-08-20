import json

from utils.log import mainLog
from remote.cpanel.account import CpanelAccount
from utils.exc_handler import get_current_func_name, log_and_send

from utils.remote_exec import run_ssh_command_on_prod

def get_account_count(reseller: str) -> tuple:
    """ Получение списка пользователей у основного ресселера """
    try:
        cmd = "/sbin/whmapi1 acctcounts user={0} --output=json".format(reseller)
        result = run_ssh_command_on_prod(cmd)

        if not result['success']:
            raise Exception(f"[get_account_count] Ошибка выполнения команды: {result['stderr']}")

        data = json.loads(result["stdout"])["data"]["reseller"]

        return (data["active"], data["suspended"])

    except Exception as exc:
        log_and_send(get_current_func_name(), exc)
        return ()

def get_account_list(reseller: str = None) -> list:
    """ Получение списка пользователей из whm api.
        Без указания параметра возвращает список всех пользователей.
        При указании параметра reseller возвращает пользователей реселлера."""
    try:
        args = "searchtype=owner search=^{0}$".format(reseller) if reseller else ""
        cmd = f"/sbin/whmapi1 listaccts {args} want=user --output=json"

        result = run_ssh_command_on_prod(cmd)

        if not result['success']:
            raise Exception(f"[get_account_dict] Ошибка выполнения команды: {result['stderr']}")
        
        data = json.loads(result["stdout"])["data"]["acct"]

        return [value["user"] for value in data]

    except Exception as exc:
        log_and_send(get_current_func_name(), exc)
        return []

def get_account_dict(reseller: str) -> dict:
    try:
        mainLog.info("[get_account_dict] Получаем список аккаунтов с whmapi1...")
        cmd = f"/sbin/whmapi1 listaccts searchtype=owner search=^{reseller}$ want=user,uid,partition,suspended --output=json"

        result = run_ssh_command_on_prod(cmd)
      
        if not result['success']:
            raise Exception(f"[get_account_dict] Ошибка выполнения команды: {result['stderr']}")
          
        data = json.loads(result["stdout"])["data"]["acct"]

        cpane_accounts_dict = {}

        for account in data:
            try:
                partition = 'hosting/home' if (account["partition"] == 'hosting') else account["partition"]

                try:
                    cpane_accounts_dict[partition]
                except KeyError:
                    cpane_accounts_dict[partition] = {}

                try:
                    cpane_accounts_dict[partition][account["user"]]
                except KeyError:
                    cpane_accounts_dict[partition][account["user"]] = []

                cpane_accounts_dict[partition][account["user"]].append(CpanelAccount(account["user"], partition, account["suspended"], account["uid"]))

            except Exception as exc:
                pass

        countAccount = sum(len(cpane_accounts_dict[partition]) for partition in cpane_accounts_dict)
        mainLog.info(f"[get_account_dict] Найдено {countAccount} аккаунтов")
                   
        if not countAccount:
            raise Exception('Found 0 Accounts')

        return cpane_accounts_dict

    except Exception as exc:
        log_and_send(get_current_func_name(), exc)
        exit()