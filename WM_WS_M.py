"""Base del Monitor: registro y latidos TCP. La supervisión del Engine se añadirá después."""

import argparse
import logging
import os
import signal
import socket
import threading

from common.configuracion import campo_valido, positivo, puerto
from common.protocolo import Canal, ConexionCerrada, ErrorProtocolo, empaquetar

LOG = logging.getLogger("WM_WS_M")


class RegistroRechazado(Exception):
    """Un KO de registro requiere corregir los datos o el ID duplicado."""


def mantener_sesion(conexion, ws_id, ubicacion, parada, intervalo=2.0, timeout=3.0,
                    registro_confirmado=None):
    canal = Canal(conexion, timeout=timeout)
    try:
        canal.saludar()
        respuesta = canal.peticion(f"REGISTRO#{ws_id}#{ubicacion}")
        if respuesta.startswith("KO#"):
            raise RegistroRechazado(respuesta[3:])
        if respuesta != f"OK#{ws_id}":
            raise ErrorProtocolo(f"Respuesta de registro inesperada: {respuesta!r}")
        if registro_confirmado is not None:
            registro_confirmado.set()
        LOG.info("Registro confirmado: %s | %s", ws_id, ubicacion)
        while not parada.wait(intervalo):
            respuesta = canal.peticion(f"PING#{ws_id}")
            if respuesta != "OK":
                raise ErrorProtocolo(f"Respuesta a PING inesperada: {respuesta!r}")
            LOG.info("Latido confirmado: %s", ws_id)
    finally:
        canal.cerrar()


def ejecutar(host, puerto_central, ws_id, ubicacion, parada, intervalo=2.0,
             timeout=3.0, reintento=2.0):
    registro_confirmado = threading.Event()
    while not parada.is_set():
        try:
            LOG.info("Conectando con CENTRAL en %s:%s", host, puerto_central)
            with socket.create_connection((host, puerto_central), timeout=timeout) as conexion:
                mantener_sesion(conexion, ws_id, ubicacion, parada, intervalo, timeout,
                                registro_confirmado)
        except RegistroRechazado as error:
            if str(error) == "ID_EN_USO" and registro_confirmado.is_set():
                # Tras un corte de red, CENTRAL puede conservar la sesión vieja
                # hasta que expire su timeout. No se expulsa a otro monitor.
                LOG.warning("La sesión anterior sigue activa; se reintentará el registro")
            else:
                LOG.error("Registro rechazado: %s", error)
                return 1
        except (OSError, ConexionCerrada, ErrorProtocolo) as error:
            if not parada.is_set():
                LOG.warning("Se perdió la sesión (%s). Reintento en %.1f s",
                            str(error) or "EOT recibido", reintento)
        if not parada.is_set():
            parada.wait(reintento)
    LOG.info("Monitor detenido")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host", nargs="?", default=os.getenv("WM_CENTRAL_HOST"))
    parser.add_argument("puerto", nargs="?", type=puerto,
                        default=os.getenv("WM_CENTRAL_PORT"))
    parser.add_argument("ws_id", nargs="?", default=os.getenv("WM_WS_ID"))
    parser.add_argument("ubicacion", nargs="?", default=os.getenv("WM_WS_LOCATION"))
    parser.add_argument("--timeout", type=positivo,
                        default=os.getenv("WM_SOCKET_TIMEOUT", "3"))
    parser.add_argument("--intervalo-ping", type=positivo,
                        default=os.getenv("WM_PING_INTERVAL", "2"))
    parser.add_argument("--reintento", type=positivo,
                        default=os.getenv("WM_RECONNECT_DELAY", "2"))
    args = parser.parse_args(argv)
    if not args.host or args.puerto is None:
        parser.error("Indica el host y puerto de CENTRAL por argumentos o variables de entorno")
    if not campo_valido(args.ws_id or "") or not campo_valido(args.ubicacion or ""):
        parser.error("ID y ubicación obligatorios, sin #, controles ni espacios en los extremos")
    try:
        empaquetar(f"REGISTRO#{args.ws_id}#{args.ubicacion}")
    except ValueError as error:
        parser.error(str(error))
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parada = threading.Event()
    for senal in (signal.SIGINT, signal.SIGTERM):
        signal.signal(senal, lambda *_: parada.set())
    return ejecutar(args.host, args.puerto, args.ws_id, args.ubicacion, parada,
                    args.intervalo_ping, args.timeout, args.reintento)


if __name__ == "__main__":
    raise SystemExit(main())
