# MinIO Server Setup Guide

This guide provides step-by-step instructions to set up and run a MinIO server on a Linux system.

## Prerequisites
- Python 3.x installed
- Required Python packages: `python-dotenv`, `wget` (install using `pip install python-dotenv wget`)
- A `minio.env` environment file
- Administrative privileges for file permissions and port usage
- Free ports for MinIO API (default: 9000) and WebUI (default: 9001)

## Setup Instructions

### 1. Download and Prepare MinIO Binary
Run the following commands to download the MinIO server binary and set up the data directory:

```bash
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
mkdir -p ~/minio-data
mkdir -p ~/minio-logs
```

### 2. Configure Environment Variables
Create a `minio.env` file in the same directory as the MinIO binary with the following content, replacing placeholders with your desired values:

```
MINIO_ROOT_USER=<your_username>
MINIO_ROOT_PASSWORD=<your_password>
MINIO_HOST=localhost:<minio_port>
MINIO_CONSOLE_ADDRESS=localhost:<minio_web_port>
```

- `<your_username>`: Choose a secure username for the MinIO admin account.
- `<your_password>`: Use a strong password (at least 8 characters).
- Ensure ports `<minio_port>` (API) and `<minio_web_port>` (WebUI) are free, or update them to available ports.

### 3. Start the MinIO Server
Run the following command to start the MinIO server in the background, using the environment variables from `minio.env`:

```bash
export MINIO_ROOT_USER=<your_username>
export MINIO_ROOT_PASSWORD=<your_password>
MINIO_ROOT_USER=$MINIO_ROOT_USER MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD \
./minio server ~/minio-data --address :<minio_port> --console-address :<minio_web_port> > ~/minio-logs/minio_server.log 2>&1 &
```

- This command exports environment variables and starts the MinIO server.
- Logs are saved to `~/minio-logs/minio_server.log` for troubleshooting.

### 4. Access MinIO
- **API Access**: Connect to `http://localhost:<minio_port>` for programmatic access.
- **WebUI Access**: Open `http://localhost:<minio_web_port>` in a browser and log in with `<your_username>` and `<your_password>`.

## Notes
- **Stopping the Server**: To stop the MinIO server, find its process ID using `ps aux | grep minio` and terminate it with `kill <pid>`.
- **Port Conflicts**: If ports `<minio_port>` or `<minio_web_port>` are in use, modify `MINIO_ADDRESS` and `MINIO_CONSOLE_ADDRESS` in `minio.env` to use different ports.
- **Security**: Store `minio.env` securely and avoid exposing sensitive credentials.
- **Data Directory**: The `~/minio-data` directory stores MinIO buckets and objects. Ensure it has sufficient disk space.