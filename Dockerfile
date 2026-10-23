FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY common/ ./common/
COPY WM_Central.py WM_WS_M.py ./

EXPOSE 9999
CMD ["python", "WM_Central.py"]
