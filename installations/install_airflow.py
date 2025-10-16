import os
import subprocess
import time

# Ensure environment
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
airflow_home = os.path.join(base_dir, 'airflow')
os.environ['AIRFLOW_HOME'] = airflow_home

# Re-init the database (resets metadata but keeps DAGs if any)
subprocess.run(['airflow', 'db', 'init'], check=True)

# Create admin user (critical—webserver needs this for login)
subprocess.run([
    'airflow', 'users', 'create',
    '--username', 'admin',
    '--firstname', 'Admin',
    '--lastname', 'User',
    '--role', 'Admin',
    '--email', 'admin@example.com',
    '--password', 'admin'
], check=True)

# Verify user creation
subprocess.run(['airflow', 'users', 'list'], check=True)

# Start scheduler first (it needs DB)
scheduler_log = os.path.join(airflow_home, 'scheduler.log')
subprocess.run([
    'nohup', 'airflow', 'scheduler',
    '>', scheduler_log, '2>&1', '&'
], check=True)