requirements.txt for backup-hdbscan.py as app.py
Flask
pandas
pyodbc
azure-storage-blob
Werkzeug
hdbscan
numpy
gunicorn

Test locally:
python3 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt
python3 app.py

