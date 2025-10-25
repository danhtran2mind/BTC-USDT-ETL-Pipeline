import os
import sys
import subprocess
import time

PROJECT_PATH = os.getcwd()
AIRFLOW_HOME = os.path.join(PROJECT_PATH, "airflow")
LOG_DIR = os.path.join(AIRFLOW_HOME, "logs")

def run(cmd, cwd=None):
    p = subprocess.Popen(
        cmd, shell=True, 
        # cwd=cwd,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        universal_newlines=True
    )
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        raise subprocess.CalledProcessError(p.returncode, cmd, output=stderr)
    return stdout

os.environ["AIRFLOW_HOME"] = AIRFLOW_HOME
os.makedirs(AIRFLOW_HOME, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)          # ensure logs folder exists

# Get Python version
python_version = ".".join(str(el) for el in list(sys.version_info[:2]))
# Install Airflow
run(
    'pip install "apache-airflow[async,celery,postgres,cncf.kubernetes]" '
    f'--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-{python_version}.txt"'
)

run("airflow db init")
run(
    "airflow users create --username admin --firstname Admin --lastname User "
    "--role Admin --email admin@example.com --password admin",
)
run("airflow users list")

# Background scheduler
subprocess.Popen(
    f"nohup airflow scheduler > {os.path.join(LOG_DIR, 'scheduler.log')} 2>&1 &",
    shell=True,
)

time.sleep(2)

# Background webserver on port 8081
subprocess.Popen(
    f"airflow webserver --port 8081 > {os.path.join(LOG_DIR, 'webserver.log')} 2>&1 &",
    shell=True,
)

time.sleep(10)

print(f"Setup at {AIRFLOW_HOME}, Login: admin/admin at http://localhost:8081")
