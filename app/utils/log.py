import sys
from pathlib import Path
from datetime import datetime
from logzero import LogFormatter, setup_logger

# Путь до основного исполняемого файла (скрипта)
base_dir = Path(sys.argv[0]).resolve().parent.parent
log_dir = base_dir / "logs"
log_dir.mkdir(parents=True, exist_ok=True)

log_filename = f"mainlog_{datetime.today().strftime('%Y_%m_%d')}.log"
log_path = log_dir / log_filename

log_formatter = LogFormatter(
    fmt='%(color)s[%(asctime)s][%(levelname)s] %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S'
)

mainLog = setup_logger(name="mainLog", logfile=str(log_path), formatter=log_formatter)