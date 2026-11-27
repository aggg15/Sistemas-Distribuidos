# WM_Front (Node.js + Express). Se construye desde esta carpeta:
#   docker compose build front
FROM node:24-slim

ENV NODE_ENV=production WM_API_CENTRAL_URL=http://central:8080
WORKDIR /app/WM_Front
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY *.js ./
COPY public/ ./public/

EXPOSE 8000
# Sin privilegios: el Front no escribe nada en disco.
USER node
CMD ["node", "WM_Front.js"]
