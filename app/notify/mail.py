import smtplib

from utils.log import mainLog
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from config.const import EMAIL_ENABLE, EMAIL_FROM, MAIL_TO, SMTP_SERVER, REMOTE_SERVER

def alertToSupport(_subject, plainText=None, htmlText=None):
    try:
        if not EMAIL_ENABLE:
            mainLog.info(f"Отправка почты не активирована в конфигурационном файле. Отправка отменена.")
            return False

        msg = MIMEMultipart('alternative')

        msg['Subject'] = "[{0}] {1}".format(REMOTE_SERVER.split('@')[1], _subject)
        msg['From']    = EMAIL_FROM
        msg['To']      = ", ".join(MAIL_TO)

        if(plainText):
            plainText = MIMEText(plainText, 'plain')
            msg.attach(plainText)

        if(htmlText):
            htmlText = MIMEText(htmlText, 'html')
            msg.attach(htmlText)

        s = smtplib.SMTP(SMTP_SERVER)
        s.sendmail(msg['From'], MAIL_TO, msg.as_string())
        s.quit()

        mainLog.debug("[alertToSupport] Сообщение отправлено.")
        return True

    except Exception as exc:
        mainLog.error("[alertToSupport] При отправке сообщения произошла ошибка: {0}".format(exc.args))
        return False

