import socket
from os import path
from utils.log import mainLog
from notify.mail import alertToSupport
from notify.tg import send_telegram_message
from utils.date_utils import get_current_date
from utils.fs_utils import get_list_dirs, get_list_files

from remote.cpanel.api import get_account_count, get_account_list

from config.const import LOCAL_DIST, RESELLER

outputHtml = """
<!DOCTYPE html>
<html>
<head>
<style>
table {{
  font-family: arial, sans-serif;
  border-collapse: collapse;
  border-spacing: 1px;
  width: 30%;
}}

td, th {{
  border: 1px solid #dddddd;
  text-align: left;
  padding: 2px;
}}

tr {{
    
}}

</style>
</head>
<body>

<h4>Резервное копирование {CurrentDate}. Сервер бекапа: {backupServer}. Время выполнения: {executionTime}</h4>

<p>Всего в панели хостинга:                  {accountTotalList} пользователей.</p>
<p>Активных у ресселера {reseller}:          {resellerActiveUserCount} пользователей.</p>
<p>Приостановленных у ресселера {reseller}:  {resellerSuspendUserCount} пользователей.</p>
<p>В текущей резервной копии:                {CurrentBackupUserCount} пользователей.</p>

<table>
  <tr>
    <th bgcolor="#FAFAD2">Username</th>
    <th bgcolor="#FAFAD2">HomeDirFiles</th>
    <th bgcolor="#FAFAD2">MysqlFiles</th>
    <th bgcolor="#FAFAD2">ExecutionTime</th>
  </tr>

  {tablePart}
</table>

</body>
</html>
"""

def get_total_report(shared_report_dict, executionTime):
    output = ""

    try:
        # Путь к директории содержащей текущую резервную копию
        backups_current_path = f"{LOCAL_DIST}/{get_current_date()}"

        if not path.exists(backups_current_path):
            raise Exception(f"Директория с текущей резервной копией не обнаружена: {backups_current_path}")

        # Получение количества аккаунтов у основного реселлера
        accounts_active_count, accounts_susped_count = get_account_count(RESELLER)

        # Получение списка всех аккаунтов из whm
        accounts_total_list = get_account_list()

        # Получение списка всех аккаунтов у основного реселлера
        accounts_reseller_list = get_account_list(RESELLER)

        # Получение списка пользователей в текущей резервной копии
        accounts_current_backup = get_list_dirs(backups_current_path)

        # Отсутствуют в резервной копии
        accounts_without_backup_list = set(accounts_reseller_list) - set(accounts_current_backup)
        mainLog.debug(f"Резервные копии отсутствуют для: {' '.join(accounts_without_backup_list)}")

        # Удаленные аккаунты
        accounts_deleted_list = set(accounts_current_backup) - set(accounts_reseller_list)
        mainLog.debug(f"Аккаунты были удалены: {' '.join(accounts_deleted_list)}")

        if not accounts_current_backup:
            raise Exception(f"Пользователи не обнаружены в директории с текущей резервной копией: {backups_current_path}")

        if (len(accounts_current_backup) / len(accounts_reseller_list) * 100) < 95:
            raise Exception(f"Количество пользователей с резервной копией менее 90 процентов.\n Активных пользователей в whm:    \
                            {accounts_active_count}.\nПриостановленных пользователей в whm:    {accounts_susped_count}.\nПользователей на диске: {len(accounts_current_backup)}")

        success = 'bgcolor="#32CD32"'
        error = 'bgcolor="#ee4c50"'

        mainLog.debug(f"Количество значений в shared_report_dict: {len(shared_report_dict)}")

        for username in accounts_reseller_list:
            account_homedir_path = f"{backups_current_path}/{username}/homedir"
            account_mysql_path = f"{backups_current_path}/{username}/mysql" 

            account_homedir_files_count = len(get_list_files(account_homedir_path))
            account_mysql_files_count = len(get_list_files(account_mysql_path))

            line_color = error if not account_homedir_files_count else success

            add = f"""
            <tr>
                <td {line_color}>{username}</td>
                <td {line_color}>{account_homedir_files_count}</td>
                <td {line_color}>{account_mysql_files_count}</td>
                <td {line_color}>{shared_report_dict.get(username)}</td>
            </tr>
            """

            if(line_color == error):
                output = add + output
            else:
                output = output + add

        return outputHtml.format(CurrentDate=get_current_date(), backupServer=socket.gethostname(), executionTime=executionTime, accountTotalList=len(accounts_total_list), 
                                 resellerActiveUserCount=accounts_active_count, resellerSuspendUserCount=accounts_susped_count, CurrentBackupUserCount=len(accounts_current_backup), tablePart=output, reseller=RESELLER)

    except Exception as exc:
        mainLog.error(f"[get_total_report][Exception] {exc.args}")
        send_telegram_message("[get_total_report]", f"[Exception] {exc.args}")
        alertToSupport("[get_total_report]", f"[Exception] {exc.args}")