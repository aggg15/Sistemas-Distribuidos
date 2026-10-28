#!/usr/bin/env bash
# Arranca o actualiza CENTRAL + Kafka en la instancia EC2.
#
#   bash deploy/arrancar_aws.sh            usa la IP pública de la instancia
#   bash deploy/arrancar_aws.sh 3.80.1.2   usa la IP indicada (por ejemplo, una Elastic IP)
#
# Si el fichero .env define WM_CENTRAL_IMAGE (usuario/wm-central:latest), se
# descarga la imagen que GitHub Actions ha subido a Docker Hub. Si no, se
# construye aquí a partir del código.
#
# Kafka necesita saber su dirección PÚBLICA para comunicársela a los clientes
# de los laboratorios (advertised listeners). Si se anunciara la IP privada,
# los FO y Engine conectarían al principio pero luego no podrían seguir.
set -euo pipefail

cd "$(dirname "$0")/.."

if [ -f .env ]; then
    set -a
    . ./.env
    set +a
fi

if [ $# -ge 1 ]; then
    IP_PUBLICA="$1"
else
    # Servicio de metadatos de EC2 (IMDSv2): primero un token y luego la consulta.
    TOKEN=$(curl -sf -X PUT "http://169.254.169.254/latest/api/token" \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
    IP_PUBLICA=$(curl -sf -H "X-aws-ec2-metadata-token: $TOKEN" \
        "http://169.254.169.254/latest/meta-data/public-ipv4")
fi

if [ -z "$IP_PUBLICA" ]; then
    echo "No se pudo averiguar la IP pública. Pásala como argumento." >&2
    exit 1
fi

echo "Actualizando docker-compose.yml y scripts desde GitHub..."
git pull --ff-only

export KAFKA_HOST="$IP_PUBLICA"
if [ -n "${WM_CENTRAL_IMAGE:-}" ]; then
    echo "Descargando $WM_CENTRAL_IMAGE de Docker Hub..."
    docker compose pull central
    docker compose up -d --no-build central kafka
else
    echo "WM_CENTRAL_IMAGE no definida: se construye la imagen en esta máquina..."
    docker compose up -d --build central kafka
fi

docker compose ps
echo
echo "Listo. Desde los laboratorios:"
echo "  CENTRAL (sockets): $IP_PUBLICA:9999"
echo "  Kafka:             $IP_PUBLICA:9092"
echo "  Panel web:         http://$IP_PUBLICA:8080"
