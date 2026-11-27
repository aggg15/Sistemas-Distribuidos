# WM_Registry (Node.js + Express). Se construye desde esta carpeta:
#   docker compose build registry
FROM node:24-slim

ENV NODE_ENV=production WM_DB_PATH=/app/data/wm.db \
    WM_TLS_CERT=/app/certs/servidor.pem WM_TLS_KEY=/app/certs/servidor.key
WORKDIR /app/WM_Registry
# Primero las dependencias: si solo cambia el código, Docker reutiliza esta capa.
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY *.js ./

# La BD (volumen wm-datos, compartido con CENTRAL) y los certificados (certs/ de
# la máquina, de solo lectura) se montan desde docker-compose.yml: no van en la imagen.
EXPOSE 8443
CMD ["node", "WM_Registry.js"]
