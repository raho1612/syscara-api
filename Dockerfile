FROM python:3.11-slim

WORKDIR /app

# Nur requirements kopieren und installieren
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App-Code kopieren (ohne Cache-Dateien)
COPY main.py .
COPY employee_names.json .
COPY api ./api
COPY core ./core
COPY services ./services
COPY shared ./shared
RUN date -u '+%Y%m%d-%H%M' > /app/api_version.txt

ENV PORT=5000
EXPOSE 5000

CMD ["python", "main.py"]
