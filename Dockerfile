FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 WM_DB_PATH=/app/data/wm.db
WORKDIR /app
RUN mkdir -p /app/data
# Primero las dependencias: si solo cambia el código, Docker reutiliza esta capa.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY common/ ./common/
COPY WM_Central/ ./WM_Central/
# El Monitor solo se usa en la prueba opcional de docker-compose (perfil demo).
COPY WM_WS/WM_WS_M.py ./WM_WS/WM_WS_M.py
COPY scripts/gestionar_bd.py ./scripts/gestionar_bd.py

# 9999: sockets de los Monitores. 8080: panel web.
EXPOSE 9999 8080
CMD ["python", "WM_Central/WM_Central.py"]
