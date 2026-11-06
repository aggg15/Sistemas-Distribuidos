# Funciones comunes de monitor.sh, engine.sh y operario.sh (se cargan con ".").
#
# Cada módulo se ejecuta en un contenedor con la imagen que publica GitHub
# Actions. Si el PC no tiene Docker, se ejecuta con Python desde el repositorio
# (necesita kafka-python instalado: pip install --user -r requirements.txt).

IMAGEN=${WM_IMAGEN:-antoniogg03/practica-aws:latest}

usa_docker() {
    command -v docker >/dev/null 2>&1
}

# lanzar NOMBRE_CONTENEDOR "OPCIONES_DOCKER" PROGRAMA [ARGUMENTOS...]
lanzar() {
    local nombre=$1 opciones=$2
    shift 2
    if usa_docker; then
        # --rm: el contenedor se borra al pararlo, así se puede relanzar con el
        # mismo nombre. -it: terminal interactiva para los menús. --network
        # host: el contenedor usa la red del PC (el Engine encuentra a su
        # Monitor en localhost). --name: permite simular una caída con
        # "docker kill NOMBRE".
        # shellcheck disable=SC2086  # las opciones se separan a propósito
        exec docker run --rm -it --network host --name "$nombre" $opciones \
            "$IMAGEN" python "$@"
    fi
    echo "Docker no disponible: se ejecuta con Python" >&2
    exec python3 "$@"
}
