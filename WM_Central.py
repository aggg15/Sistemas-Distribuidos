"""Base TCP de WM_Central: sesiones persistentes de monitores, aún sin SQLite."""

import argparse
from dataclasses import dataclass
import logging
import os
import signal
import socket
import threading

from common.configuracion import campo_valido, entero_positivo, positivo, puerto
from common.protocolo import Canal, ConexionCerrada, ErrorProtocolo, NACK

LOG = logging.getLogger("WM_Central")


@dataclass
class Estacion:
    ubicacion: str
    sesion: object
    averia: bool = False


class RegistroConexiones:
    """Estado en memoria del transporte; conectado no significa listo para regar."""

    def __init__(self):
        self._estaciones = {}
        self._lock = threading.Lock()

    def registrar(self, ws_id, ubicacion, sesion):
        with self._lock:
            anterior = self._estaciones.get(ws_id)
            if anterior is not None and anterior.sesion is not None:
                return False
            # Tras reconectar habrá que conocer de nuevo la salud del Engine.
            self._estaciones[ws_id] = Estacion(ubicacion, sesion)
            return True

    def desconectar(self, ws_id, sesion):
        with self._lock:
            estacion = self._estaciones.get(ws_id)
            if estacion is not None and estacion.sesion is sesion:
                estacion.sesion = None

    def avisar_averia(self, ws_id, sesion, averia):
        with self._lock:
            estacion = self._estaciones[ws_id]
            if estacion.sesion is not sesion:
                raise ValueError("La sesión ya no pertenece a la estación")
            estacion.averia = averia

    def estado(self):
        with self._lock:
            return {
                ws_id: {"ubicacion": e.ubicacion, "conectada": e.sesion is not None,
                        "averia": e.averia}
                for ws_id, e in self._estaciones.items()
            }


class SesionMonitor:
    def __init__(self, registro):
        self.registro = registro
        self.ws_id = None

    def procesar(self, mensaje):
        campos = mensaje.split("#")
        operacion = campos[0]
        if self.ws_id is None:
            if operacion != "REGISTRO" or len(campos) != 3:
                return "KO#Primero debe enviar REGISTRO con ID y ubicacion"
            ws_id, ubicacion = campos[1:]
            if not campo_valido(ws_id) or not campo_valido(ubicacion):
                return "KO#ID o ubicacion no validos"
            if not self.registro.registrar(ws_id, ubicacion, self):
                return "KO#ID_EN_USO"
            self.ws_id = ws_id
            LOG.info("Monitor conectado: %s | %s", ws_id, ubicacion)
            return f"OK#{ws_id}"

        if len(campos) != 2 or campos[1] != self.ws_id:
            return "KO#La peticion debe contener el ID de esta sesion"
        if operacion == "PING":
            LOG.debug("PING de %s", self.ws_id)
            return "OK"
        if operacion in ("AVERIA", "RESUELTA"):
            self.registro.avisar_averia(self.ws_id, self, operacion == "AVERIA")
            LOG.info("%s: %s", self.ws_id, operacion)
            return "OK"
        return "KO#Operacion desconocida"

    def cerrar(self):
        if self.ws_id is not None:
            self.registro.desconectar(self.ws_id, self)
            LOG.info("Monitor desconectado: %s", self.ws_id)


class CentralTCP:
    def __init__(self, host="0.0.0.0", puerto_escucha=9999, timeout=6.0,
                 max_conexiones=64):
        self.host = host
        self.puerto = puerto_escucha
        self.timeout = timeout
        self.max_conexiones = max_conexiones
        self.registro = RegistroConexiones()
        self.parada = threading.Event()
        self.lista = threading.Event()
        self._lock = threading.Lock()
        self._conexiones = {}

    def atender(self, conexion, direccion):
        sesion = SesionMonitor(self.registro)
        canal = Canal(conexion, timeout=self.timeout)
        with conexion:
            try:
                canal.esperar_saludo()
                while not self.parada.is_set():
                    canal.responder(sesion.procesar)
                    if sesion.ws_id is None:
                        break  # Registro rechazado: respuesta KO y cierre.
            except ConexionCerrada:
                LOG.info("Cierre ordenado desde %s", direccion)
            except TimeoutError:
                LOG.warning("Tiempo de espera agotado: %s", direccion)
            except (OSError, ErrorProtocolo, ValueError) as error:
                if not self.parada.is_set():
                    LOG.warning("Conexión interrumpida %s: %s", direccion, error)
            finally:
                sesion.cerrar()
                canal.cerrar()
                with self._lock:
                    self._conexiones.pop(conexion, None)

    def detener(self):
        # La espera de accept está acotada. Los sockets los cierra su hilo.
        self.parada.set()

    def servir(self):
        familia = socket.AF_INET6 if ":" in self.host else socket.AF_INET
        try:
            with socket.socket(familia, socket.SOCK_STREAM) as servidor:
                servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                servidor.bind((self.host, self.puerto))
                servidor.listen()
                servidor.settimeout(0.25)
                self.puerto = servidor.getsockname()[1]
                LOG.info("TCP escuchando en %s:%s", self.host, self.puerto)
                self.lista.set()
                while not self.parada.is_set():
                    try:
                        conexion, direccion = servidor.accept()
                    except TimeoutError:
                        continue
                    with self._lock:
                        lleno = len(self._conexiones) >= self.max_conexiones
                        if not lleno:
                            hilo = threading.Thread(target=self.atender,
                                                    args=(conexion, direccion))
                            self._conexiones[conexion] = hilo
                    if lleno:
                        LOG.warning("Límite de conexiones alcanzado: %s", direccion)
                        with conexion:
                            conexion.settimeout(self.timeout)
                            try:
                                conexion.sendall(NACK)
                            except OSError:
                                pass
                    else:
                        hilo.start()
        finally:
            self.parada.set()
            with self._lock:
                pendientes = list(self._conexiones.items())
            for conexion, _ in pendientes:
                try:
                    # Desbloquea recv conservando la escritura para enviar EOT.
                    conexion.shutdown(socket.SHUT_RD)
                except OSError:
                    pass
            for _, hilo in pendientes:
                hilo.join()
            LOG.info("Central detenida")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("puerto", nargs="?", type=puerto,
                        default=os.getenv("WM_SOCKET_PORT", os.getenv("PORT", "9999")))
    parser.add_argument("--host", default=os.getenv("WM_SOCKET_HOST", "0.0.0.0"))
    parser.add_argument("--timeout", type=positivo,
                        default=os.getenv("WM_SOCKET_TIMEOUT", "6"))
    parser.add_argument("--max-conexiones", type=entero_positivo,
                        default=os.getenv("WM_MAX_CONEXIONES", "64"))
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    central = CentralTCP(args.host, args.puerto, args.timeout, args.max_conexiones)
    for senal in (signal.SIGINT, signal.SIGTERM):
        signal.signal(senal, lambda *_: central.detener())
    try:
        central.servir()
    except OSError as error:
        parser.exit(1, f"Error en la central: {error}\n")


if __name__ == "__main__":
    main()
