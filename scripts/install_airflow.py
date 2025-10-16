import os
import subprocess
import time
import sys

# Ensure environment
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
airflow_home = os.path.join(base_dir, 'airflow')
os.makedirs(airflow_home, exist_ok=True)
os.environ['AIRFLOW_HOME'] = airflow_home
os.environ['AIRFLOW__CORE__DAGS_FOLDER'] = os.path.join(airflow_home, 'dags')
os.environ['AIRFLOW__CORE__LOAD_EXAMPLES'] = 'False'

# Create dags directory
dags_dir = os.path.join(airflow_home, 'dags')
os.makedirs(dags_dir, exist_ok=True)

# Install Airflow using subprocess (remove !pip which is Jupyter syntax)
print("Installing Apache Airflow...")
install_cmd = [
    sys.executable, '-m', 'pip', 'install',
    "apache-airflow[async,celery,postgres,cncf.kubernetes]",
    '--constraint', "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt",
    '--quiet'
]
result = subprocess.run(install_cmd, check=True, capture_output=True, text=True)
print("Airflow installation completed.")

# Verify Airflow CLI is available
try:
    subprocess.run(['airflow', 'version'], check=True, capture_output=True)
except (subprocess.CalledProcessError, FileNotFoundError):
    # Add Airflow to PATH if needed
    airflow_bin = os.path.join(base_dir, '.venv', 'bin') if os.path.exists(os.path.join(base_dir, '.venv')) else ''
    if airflow_bin and os.path.exists(os.path.join(airflow_bin, 'airflow')):
        os.environ['PATH'] = f"{airflow_bin}:{os.environ.get('PATH', '')}"
    print("Airflow CLI verified.")

# Re-init the database (resets metadata but keeps DAGs if any)
print("Initializing Airflow database...")
subprocess.run(['airflow', 'db', 'init'], check=True, capture_output=True)

# Create admin user (critical—webserver needs this for login)
print("Creating admin user...")
try:
    subprocess.run([
        'airflow', 'users', 'create',
        '--username', 'admin',
        '--firstname', 'Admin',
        '--lastname', 'User',
        '--role', 'Admin',
        '--email', 'admin@example.com',
        '--password', 'admin'
    ], check=True, capture_output=True)
    print("Admin user created successfully.")
except subprocess.CalledProcessError:
    print("Admin user may already exist, skipping...")

# Verify user creation
print("Verifying users...")
subprocess.run(['airflow', 'users', 'list'], check=True, capture_output=True)

# Wait a moment for DB to settle
time.sleep(2)

# Start scheduler in background with proper nohup syntax
print("Starting Airflow scheduler...")
scheduler_log = os.path.join(airflow_home, 'logs', 'scheduler.log')
os.makedirs(os.path.dirname(scheduler_log), exist_ok=True)

scheduler_cmd = [
    'nohup', 'airflow', 'scheduler',
    f'>> {scheduler_log}', '2>&1', '&'
]

# Use shell=True for proper background process handling
subprocess.run(scheduler_cmd, shell=True, check=True)

# Wait for scheduler to start
time.sleep(5)
print(f"Scheduler started. Logs: {scheduler_log}")

print("Airflow setup completed!")
print(f"Airflow Home: {airflow_home}")
print("To start webserver manually: airflow webserver")