# MinIO Server Setup Guide

This guide explains how to set up and run a MinIO server with optional ngrok tunneling for external access.

## Prerequisites
- Python 3.x
- Required Python packages: `python-dotenv`, `pyngrok`
- `wget` installed on the system
- Environment files: `minio.env` and `tunneling.env` (for ngrok)

## Setup Instructions

1. **Install Dependencies**
   Ensure Python and required packages are installed:
   ```bash:disable-run
   pip install python-dotenv pyngrok
   ```

2. **Configure Environment Variables**
   Create a `minio.env` file with the following:
   ```
   MINIO_ROOT_USER=your_username
   MINIO_ROOT_PASSWORD=your_password
   ```
   If using ngrok, create a `tunneling.env` file:
   ```
   NGROK_TOKEN=your_ngrok_authtoken
   ```

3. **Run the Script**
   Execute the provided Python script to:
   - Download and set up the MinIO binary
   - Start the MinIO server on ports `12345` (API) and `12346` (WebUI)
   - Optionally create an ngrok tunnel for external access

   ```bash
   python minio_setup.py
   ```

4. **Access MinIO**
   - **Local Access**:
     - API: `http://localhost:12345`
     - WebUI: `http://localhost:12346`
   - **Ngrok Access** (if enabled):
     - HTTPS URL: Printed in the console (e.g., `https://abcd1234.ngrok.io`)
     - Use the same credentials from `minio.env`

## Notes
- The MinIO server runs in the background. To stop it, terminate the process manually.
- Ngrok tunneling requires a valid `NGROK_TOKEN`. Sign up at [ngrok.com](https://ngrok.com) to obtain one.
- Ensure ports `12345` and `12346` are free or modify them in the script if needed.
