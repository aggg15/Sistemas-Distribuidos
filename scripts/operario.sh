#!/usr/bin/env bash
# Lanza un operario (WM_FO) en este PC. Debe estar dado de alta en CENTRAL.
#
#   scripts/operario.sh IP_KAFKA FO_ID [FICHERO_PETICIONES]
#
#   scripts/operario.sh 13.49.77.131 FO1
#   scripts/operario.sh 13.49.77.131 FO2 WM_FO/peticiones_ejemplo.txt
set -euo pipefail
cd "$(dirname "$0")/.."

if [ $# -lt 2 ]; then
    echo "Uso: scripts/operario.sh IP_KAFKA FO_ID [FICHERO_PETICIONES]" >&2
    exit 1
fi
IP=$1
FO_ID=$2
FICHERO=${3:-}

. scripts/lanzar.sh
opciones=""
argumentos=(WM_FO/WM_FO.py "$IP:9092" "$FO_ID")
if [ -n "$FICHERO" ]; then
    if [ ! -f "$FICHERO" ]; then
        echo "No existe el fichero de peticiones: $FICHERO" >&2
        exit 1
    fi
    if usa_docker; then
        # El contenedor no ve los ficheros del PC: se monta en solo lectura.
        opciones="-v $(cd "$(dirname "$FICHERO")" && pwd)/$(basename "$FICHERO"):/tmp/peticiones.txt:ro"
        argumentos+=(--fichero /tmp/peticiones.txt)
    else
        argumentos+=(--fichero "$FICHERO")
    fi
fi
lanzar "wm-fo-$FO_ID" "$opciones" "${argumentos[@]}"
