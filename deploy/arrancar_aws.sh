#!/usr/bin/env bash
# Arranca o actualiza CENTRAL + Kafka en la instancia EC2.
#
#   bash deploy/arrancar_aws.sh            usa la IP pública de la instancia
#   bash deploy/arrancar_aws.sh 3.80.1.2   usa la IP indicada (por ejemplo, una Elastic IP)
#
# Si el fichero .env define WM_CENTRAL_IMAGE (usuario/practica-aws:latest), se
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

# Si el usuario no está en el grupo docker, se usa sudo. sudo no hereda las
# variables de entorno: KAFKA_HOST se le pasa explícitamente.
compose() {
    if docker info >/dev/null 2>&1; then
        KAFKA_HOST="$IP_PUBLICA" docker compose "$@"
    else
        sudo KAFKA_HOST="$IP_PUBLICA" docker compose "$@"
    fi
}

# Certificado TLS de los sockets de CENTRAL para la IP actual. La CA de
# certs/ se reutiliza: los laboratorios no tienen que volver a copiar ca.pem
# aunque cambie la IP. Se genera con la propia imagen (no hace falta Python aquí).
certificados() {
    mkdir -p certs
    local imagen=${WM_CENTRAL_IMAGE:-wm-central:local}
    local orden=(docker run --rm -v "$PWD/certs:/app/certs" "$imagen"
                 python scripts/generar_certificados.py "$IP_PUBLICA" --carpeta /app/certs)
    if docker info >/dev/null 2>&1; then "${orden[@]}"; else sudo "${orden[@]}"; fi
}

if [ -n "${WM_CENTRAL_IMAGE:-}" ]; then
    echo "Descargando $WM_CENTRAL_IMAGE de Docker Hub..."
    compose pull central
    certificados
    compose up -d --no-build kafka
    # Se recrea para que cargue el certificado recién emitido.
    compose up -d --no-build --no-deps --force-recreate central
else
    echo "WM_CENTRAL_IMAGE no definida: se construye la imagen en esta máquina..."
    compose build central
    certificados
    compose up -d kafka
    compose up -d --no-deps --force-recreate central
fi

compose ps
echo
echo "Listo. Desde los laboratorios:"
echo "  CENTRAL (sockets TLS): $IP_PUBLICA:9999"
echo "  CA para los Monitores: $PWD/certs/ca.pem  (cópiala a certs/ca.pem en cada PC)"
echo "  Kafka:             $IP_PUBLICA:9092"
echo "  Panel web:         http://$IP_PUBLICA:8080"
