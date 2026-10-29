"""Monitor de una estación: sesión con CENTRAL y vigilancia de la salud del Engine."""

import argparse
import logging
import os
import signal
import socket
import threading
import time

from common.configuracion import campo_valido, positivo, puerto
from common.protocolo import Canal, ConexionCerrada, ErrorProtocolo, empaquetar

LOG = logging.getLogger("WM_WS_M")

# Cada cuánto se revisa si la salud ha cambiado mientras se espera el próximo PING.
INTERVALO_REVISION = 0.2


class RegistroRechazado(Exception):
    """Un KO de registro requiere corregir los datos o el ID duplicado."""


class SaludEngine:
    """Salud del Engine compartida entre el hilo que lo vigila y el de CENTRAL.

    Empieza en KO: mientras el Engine no conteste, la estación no puede regar
    y el enunciado pide notificarlo a CENTRAL como avería.
    """

    def __init__(self, ok=False):
        self._ok = ok
        self._lock = threading.Lock()

    @property
    def ok(self):
        with self._lock:
            return self._ok

    def actualizar(self, ok):
        with self._lock:
            cambiado = ok != self._ok
            self._ok = ok
        if cambiado and ok:
            LOG.info("Salud del Engine: OK")
        elif cambiado:
            LOG.warning("Salud del Engine: KO (avería)")


def notificar_salud(canal, ws_id, ok):
    operacion = "RESUELTA" if ok else "AVERIA"
    respuesta = canal.peticion(f"{operacion}#{ws_id}")
    if respuesta != "OK":
        raise ErrorProtocolo(f"Respuesta a {operacion} inesperada: {respuesta!r}")
    LOG.info("CENTRAL informada: %s", operacion)


def mantener_sesion(conexion, ws_id, ubicacion, parada, intervalo=2.0, timeout=3.0,
                    registro_confirmado=None, salud=None):
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

        # Tras un registro, CENTRAL considera que la estación no tiene avería.
        notificado_ok = True
        proximo_ping = time.monotonic() + intervalo
        # Este hilo es el único que usa el canal: los avisos de salud se envían
        # desde aquí, intercalados con los PING, nunca desde el hilo del Engine.
        while not parada.is_set():
            if salud is not None and salud.ok != notificado_ok:
                notificado_ok = salud.ok
                notificar_salud(canal, ws_id, notificado_ok)
                continue
            restante = proximo_ping - time.monotonic()
            if restante <= 0:
                respuesta = canal.peticion(f"PING#{ws_id}")
                if respuesta != "OK":
                    raise ErrorProtocolo(f"Respuesta a PING inesperada: {respuesta!r}")
                LOG.info("Latido confirmado: %s", ws_id)
                proximo_ping = time.monotonic() + intervalo
                continue
            parada.wait(min(restante, INTERVALO_REVISION))
    finally:
        canal.cerrar()


def atender_engine(conexion, ws_id, salud, parada, registro_confirmado,
                   intervalo=1.0, timeout=3.0):
    """Da el ID al Engine y le pide su estado cada segundo (CHECK → OK/KO)."""
    canal = Canal(conexion, timeout=timeout)
    try:
        canal.esperar_saludo()
        # El ID solo se entrega cuando CENTRAL lo ha confirmado.
        while not registro_confirmado.wait(INTERVALO_REVISION):
            if parada.is_set():
                return
        respuesta = canal.peticion(f"ID#{ws_id}")
        if respuesta != "OK":
            raise ErrorProtocolo(f"El Engine no aceptó el ID: {respuesta!r}")
        LOG.info("Engine conectado y asignado a %s", ws_id)
        while not parada.is_set():
            respuesta = canal.peticion("CHECK")
            if respuesta not in ("OK", "KO"):
                raise ErrorProtocolo(f"Respuesta a CHECK inesperada: {respuesta!r}")
            salud.actualizar(respuesta == "OK")
            parada.wait(intervalo)
    finally:
        canal.cerrar()


def vigilar_engine(servidor, ws_id, salud, parada, registro_confirmado,
                   intervalo=1.0, timeout=3.0):
    """Atiende al Engine de esta estación; si se cae, espera a que vuelva."""
    servidor.settimeout(0.25)
    while not parada.is_set():
        try:
            conexion, direccion = servidor.accept()
        except socket.timeout:
            continue
        LOG.info("Conexión del Engine desde %s", direccion)
        with conexion:
            try:
                atender_engine(conexion, ws_id, salud, parada, registro_confirmado,
                               intervalo, timeout)
            except ConexionCerrada:
                LOG.warning("El Engine cerró la conexión")
            except (OSError, ErrorProtocolo) as error:
                # Incluye TimeoutError: el Engine no contesta.
                LOG.warning("El Engine no responde: %s", error)
            finally:
                # Sin respuesta del Engine se considera avería (enunciado).
                salud.actualizar(False)


def ejecutar(host, puerto_central, ws_id, ubicacion, parada, intervalo=2.0,
             timeout=3.0, reintento=2.0, registro_confirmado=None, salud=None):
    if registro_confirmado is None:
        registro_confirmado = threading.Event()
    while not parada.is_set():
        try:
            LOG.info("Conectando con CENTRAL en %s:%s", host, puerto_central)
            with socket.create_connection((host, puerto_central), timeout=timeout) as conexion:
                mantener_sesion(conexion, ws_id, ubicacion, parada, intervalo, timeout,
                                registro_confirmado, salud)
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
                LOG.warning("Imposible conectar con WM_Central (%s). Reintento en %.1f s",
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
    parser.add_argument("--puerto-engine", type=puerto,
                        default=os.getenv("WM_ENGINE_PORT"),
                        help="Puerto donde espera al WM_WS_E. Sin él no se vigila el Engine")
    parser.add_argument("--intervalo-check", type=positivo,
                        default=os.getenv("WM_CHECK_INTERVAL", "1"))
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

    registro_confirmado = threading.Event()
    salud = None
    if args.puerto_engine is not None:
        salud = SaludEngine()
        try:
            servidor = socket.create_server(("0.0.0.0", args.puerto_engine))
        except OSError as error:
            parser.exit(1, f"No se pudo abrir el puerto del Engine: {error}\n")
        LOG.info("Esperando al Engine en el puerto %s", args.puerto_engine)
        threading.Thread(
            target=vigilar_engine,
            args=(servidor, args.ws_id, salud, parada, registro_confirmado,
                  args.intervalo_check, args.timeout),
            daemon=True,
        ).start()
    return ejecutar(args.host, args.puerto, args.ws_id, args.ubicacion, parada,
                    args.intervalo_ping, args.timeout, args.reintento,
                    registro_confirmado, salud)


if __name__ == "__main__":
    raise SystemExit(main())
