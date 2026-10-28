FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 WM_DB_PATH=/app/data/wm.db
WORKDIR /app
RUN mkdir -p /app/data
# Primero las dependencias: si solo cambia el código, Docker reutiliza esta capa.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY common/ ./common/
COPY WM_Central.py WM_WS_M.py gestor_riegos.py ./
COPY scripts/gestionar_bd.py ./scripts/gestionar_bd.py

EXPOSE 9999
CMD ["python", "WM_Central.py"]
