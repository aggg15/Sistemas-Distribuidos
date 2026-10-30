FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 WM_DB_PATH=/app/data/wm.db
WORKDIR /app
RUN mkdir -p /app/data
# Primero las dependencias: si solo cambia el código, Docker reutiliza esta capa.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY common/ ./common/
COPY WM_Central/ ./WM_Central/
# Una sola imagen para todos los módulos: por defecto arranca CENTRAL y en el
# laboratorio se indica el módulo al lanzarla (docs/DESPLIEGUE_AWS.md, apartado 7).
COPY WM_WS/ ./WM_WS/
COPY WM_FO/ ./WM_FO/
COPY scripts/gestionar_bd.py ./scripts/gestionar_bd.py

# 9999: sockets de los Monitores. 8080: panel web.
EXPOSE 9999 8080
CMD ["python", "WM_Central/WM_Central.py"]
