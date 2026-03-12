@echo off
cd /d %~dp0
echo Starte Syscara Python API Server...
echo -----------------------------------
call ..\.venv\Scripts\activate
pip install -r requirements.txt
python main.py
pause
