#!/usr/bin/env bash
# En la instancia EC2: arranca (o actualiza) WM_Central y Kafka y da de alta
# las estaciones y los operarios del caso base. CENTRAL solo acepta los que
# estén dados de alta.
#
#   scripts/central.sh             usa la IP pública de la instancia
#   scripts/central.sh 3.80.1.2    usa la IP indicada (por ejemplo, una Elastic IP)
#
# Operarios: WM_OPERARIOS="FO1:Ana FO2:Luis" scripts/central.sh cambia la lista.
# Estaciones: WM_ESTACIONES, una por línea con el formato ID;Ubicación.
# Repetir un alta solo actualiza el nombre o la ubicación.
set -euo pipefail
cd "$(dirname "$0")/.."

bash deploy/arrancar_aws.sh "$@"

DOCKER=docker
if ! docker info >/dev/null 2>&1; then
    DOCKER="sudo docker"
fi

echo
echo "Alta de estaciones:"
while IFS=';' read -r ws_id ubicacion; do
    [ -n "$ws_id" ] || continue
    # </dev/null: exec no debe leerse las líneas que quedan de la lista.
    $DOCKER compose exec -T central \
        python scripts/gestionar_bd.py estacion "$ws_id" "$ubicacion" </dev/null
done <<< "${WM_ESTACIONES:-WS-04;River Park
WS-05;Central Park
WS-07;Riverside Gardens
WS-11;North Roundabout}"

echo
echo "Alta de operarios:"
for operario in ${WM_OPERARIOS:-FO1:Ana FO2:Luis FO3:Marta FO4:Pablo}; do
    $DOCKER compose exec -T central \
        python scripts/gestionar_bd.py operario "${operario%%:*}" "${operario#*:}"
done
