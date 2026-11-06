#!/usr/bin/env bash
# En la instancia EC2: arranca (o actualiza) WM_Central y Kafka y da de alta
# los operarios del caso base.
#
#   scripts/central.sh             usa la IP pública de la instancia
#   scripts/central.sh 3.80.1.2    usa la IP indicada (por ejemplo, una Elastic IP)
#
# Operarios: WM_OPERARIOS="FO1:Ana FO2:Luis" scripts/central.sh cambia la lista.
# Repetir el alta de un operario solo actualiza su nombre.
set -euo pipefail
cd "$(dirname "$0")/.."

bash deploy/arrancar_aws.sh "$@"

DOCKER=docker
if ! docker info >/dev/null 2>&1; then
    DOCKER="sudo docker"
fi

echo
echo "Alta de operarios:"
for operario in ${WM_OPERARIOS:-FO1:Ana FO2:Luis FO3:Marta FO4:Pablo}; do
    $DOCKER compose exec -T central \
        python scripts/gestionar_bd.py operario "${operario%%:*}" "${operario#*:}"
done
