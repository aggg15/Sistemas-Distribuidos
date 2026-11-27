"""WM_Central: sesiones TCP de monitores, SQLite y ciclo de riego por Kafka."""

import argparse
from dataclasses import dataclass
import logging
import os
from pathlib import Path
import signal
import socket
import sqlite3
import ssl
import sys
import threading
import time

# Permite importar "common" (en la raíz del repositorio) se lance desde donde se lance.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common import consola  # noqa: E402
from common.base_datos import BaseDatos, RUTA_BD  # noqa: E402
from common.configuracion import campo_valido, entero_positivo, positivo, puerto  # noqa: E402
from common.protocolo import Canal, ConexionCerrada, ErrorProtocolo, NACK  # noqa: E402
from common.seguridad import contexto_tls_servidor  # noqa: E402
from common.topics import TOPIC_EVENTOS_WS, TOPIC_PETICIONES  # noqa: E402
from gestor_riegos import GestorRiegos  # noqa: E402
from panel_remoto import crear_panel_remoto  # noqa: E402
from panel_web import RegistroEventos, crear_panel  # noqa: E402

LOG = logging.getLogger("WM_Central")

INTERVALO_ESTADO_RED_S = 2.0
# Certificado del canal TLS con los Monitores (scripts/generar_certificados.py).
RAIZ = Path(__file__).resolve().parent.parent
CERT_TLS = str(RAIZ / "certs" / "servidor.pem")
CLAVE_TLS = str(RAIZ / "certs" / "servidor.key")


@dataclass
class Estacion:
    ubicacion: str
    sesion: object
    averia: bool = False
    bloqueada: bool = False
    ultima_conexion: str = ""
    riego: object = None  # RiegoEnCurso mientras está reservada o regando

    def estado(self):
        # Prioridad fija del diseño (docs/DISENO.md, apartado 2).
        if self.sesion is None:
            return "DESCONECTADA"
        if self.averia:
            return "FUGA"
        if self.bloqueada:
            return "FUERA_SERVICIO"
        if self.riego is not None and self.riego.activo:
            return "REGANDO"
        return "DISPONIBLE"


class RegistroConexiones:
    """Recupera datos duraderos; sesiones y salud se vuelven a conocer en cada arranque."""

    def __init__(self, bd):
        self.bd = bd
        self._estaciones = {
            fila["id"]: Estacion(fila["ubicacion"], None,
                                  bloqueada=bool(fila["bloqueada"]),
                                  ultima_conexion=fila["ultima_conexion"])
            for fila in bd.listar_estaciones()
        }
        self._lock = threading.Lock()

    def registrar(self, ws_id, ubicacion, sesion):
        """Autentica una estación. Devuelve None si se acepta o el motivo del rechazo."""
        with self._lock:
            anterior = self._estaciones.get(ws_id)
            if anterior is not None and anterior.sesion is not None:
                return "ID_EN_USO"
            # Solo se aceptan estaciones dadas de alta en la BD (gestionar_bd.py).
            # El OK se enviará únicamente después de confirmar la transacción.
            fila = self.bd.conectar_estacion(ws_id, ubicacion)
            if fila is None:
                return "WS_NO_REGISTRADA"
            self._estaciones[ws_id] = Estacion(
                ubicacion, sesion, bloqueada=bool(fila["bloqueada"]),
                ultima_conexion=fila["ultima_conexion"],
                # Si la desconexión anterior aún no ha cerrado su riego, se conserva
                # para que el hilo que lo está cerrando lo encuentre.
                riego=anterior.riego if anterior is not None else None,
            )
            return None

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
            resultado = {}
            for ws_id, e in self._estaciones.items():
                datos = {"ubicacion": e.ubicacion, "conectada": e.sesion is not None,
                         "averia": e.averia, "bloqueada": e.bloqueada,
                         "ultima_conexion": e.ultima_conexion, "estado": e.estado()}
                if datos["estado"] == "REGANDO":
                    datos["riego"] = {"riego_id": e.riego.riego_id, "fo_id": e.riego.fo_id,
                                      "origen": e.riego.origen,
                                      "caudal_lpm": e.riego.caudal_lpm,
                                      "volumen_l": e.riego.volumen_l,
                                      "segundos": e.riego.segundos}
                resultado[ws_id] = datos
            return resultado

    def ids(self):
        with self._lock:
            return sorted(self._estaciones)

    # --- Reserva de la estación durante un riego ----------------------------

    def reservar(self, ws_id, riego):
        """Asigna el riego si la estación está DISPONIBLE. Devuelve el motivo si no."""
        with self._lock:
            estacion = self._estaciones.get(ws_id)
            if estacion is None:
                return "La estación no existe"
            if estacion.riego is not None:
                return "La estación ya tiene un riego en curso"
            estado = estacion.estado()
            if estado != "DISPONIBLE":
                return f"La estación no está disponible ({estado})"
            estacion.riego = riego
            return None

    def liberar(self, ws_id, riego):
        """Quita ese riego concreto de la estación. Devuelve None si ya no estaba."""
        with self._lock:
            estacion = self._estaciones.get(ws_id)
            if estacion is None or estacion.riego is not riego:
                return None
            estacion.riego = None
            return riego

    def riego_de(self, ws_id):
        with self._lock:
            estacion = self._estaciones.get(ws_id)
            return estacion.riego if estacion is not None else None

    def bloquear(self, ws_id, bloqueada):
        with self._lock:
            estacion = self._estaciones.get(ws_id)
            if estacion is None:
                return False
            self.bd.guardar_bloqueo(ws_id, bloqueada)  # se conserva tras un reinicio
            estacion.bloqueada = bloqueada
            return True


class SesionMonitor:
    def __init__(self, registro, gestor=None):
        self.registro = registro
        self.gestor = gestor
        self.ws_id = None

    def _interrumpir_riego(self, motivo):
        # En otro hilo: publicar en Kafka puede tardar y este hilo debe
        # contestar al Monitor dentro de su plazo.
        if self.gestor is not None:
            threading.Thread(target=self.gestor.interrumpir, args=(self.ws_id, motivo),
                             daemon=True).start()

    def procesar(self, mensaje):
        campos = mensaje.split("#")
        operacion = campos[0]
        if self.ws_id is None:
            if operacion != "REGISTRO" or len(campos) != 3:
                return "KO#Primero debe enviar REGISTRO con ID y ubicacion"
            ws_id, ubicacion = campos[1:]
            if not campo_valido(ws_id) or not campo_valido(ubicacion):
                return "KO#ID o ubicacion no validos"
            try:
                rechazo = self.registro.registrar(ws_id, ubicacion, self)
            except sqlite3.Error:
                LOG.exception("No se pudo guardar el registro de %s", ws_id)
                return "KO#ERROR_BD"
            if rechazo is not None:
                LOG.warning("Registro denegado: %s | %s (%s)", ws_id, ubicacion, rechazo)
                return f"KO#{rechazo}"
            self.ws_id = ws_id
            LOG.info("Monitor conectado: %s | %s", ws_id, ubicacion, extra={"color": "verde"})
            return f"OK#{ws_id}"

        if len(campos) != 2 or campos[1] != self.ws_id:
            return "KO#La peticion debe contener el ID de esta sesion"
        if operacion == "PING":
            LOG.debug("PING de %s", self.ws_id)
            return "OK"
        if operacion in ("AVERIA", "RESUELTA"):
            self.registro.avisar_averia(self.ws_id, self, operacion == "AVERIA")
            LOG.info("%s: %s", self.ws_id, operacion,
                     extra={"color": "rojo" if operacion == "AVERIA" else "verde"})
            if operacion == "AVERIA":
                self._interrumpir_riego("FUGA")
            return "OK"
        return "KO#Operacion desconocida"

    def cerrar(self):
        if self.ws_id is not None:
            self.registro.desconectar(self.ws_id, self)
            LOG.info("Monitor desconectado: %s", self.ws_id, extra={"color": "amarillo"})
            self._interrumpir_riego("DESCONEXION")


class CentralTCP:
    def __init__(self, host="0.0.0.0", puerto_escucha=9999, timeout=6.0,
                 max_conexiones=64, ruta_bd=RUTA_BD, servidor_kafka=None,
                 puerto_http=None, contexto_tls=None):
        self.host = host
        # Con contexto TLS, todo el protocolo con los Monitores viaja cifrado
        # (ARCHITECTURE.md D2). Sin él, sockets en claro como en Release 1.
        self.contexto_tls = contexto_tls
        self.puerto = puerto_escucha
        self.puerto_http = puerto_http
        self.timeout = timeout
        self.max_conexiones = max_conexiones
        self.servidor_kafka = servidor_kafka
        self.productor = None
        self.bd = BaseDatos(ruta_bd)
        self.registro = RegistroConexiones(self.bd)
        self.gestor = GestorRiegos(self.registro, self.bd, self.publicar)
        LOG.info("SQLite: %s | %s operarios", self.bd.ruta, len(self.bd.listar_operarios()))
        for ws_id, estado in self.registro.estado().items():
            LOG.info("Estación recuperada: %s | %s | DESCONECTADA", ws_id, estado["ubicacion"])
        self.parada = threading.Event()
        self.lista = threading.Event()
        self._lock = threading.Lock()
        self._conexiones = {}

    # --- Kafka --------------------------------------------------------------

    def publicar(self, topic, mensaje, clave):
        """Devuelve True solo si Kafka confirma la entrega."""
        if self.productor is None:
            LOG.warning("Kafka no disponible: no se ha enviado %s", mensaje.get("tipo"))
            return False
        from kafka.errors import KafkaError
        from common.kafka_utils import enviar
        try:
            enviar(self.productor, topic, mensaje, clave=clave)
            return True
        except (KafkaError, OSError) as error:
            LOG.warning("No se pudo enviar %s a Kafka: %s", mensaje.get("tipo"), error)
            return False

    def escuchar_kafka(self):
        # Importación aquí: sin --kafka, CENTRAL funciona sin tener kafka-python.
        from kafka.errors import KafkaError
        from common.kafka_utils import crear_consumidor, crear_productor
        while not self.parada.is_set():
            try:
                if self.productor is None:
                    self.productor = crear_productor(self.servidor_kafka)
                consumidor = crear_consumidor(self.servidor_kafka,
                                              [TOPIC_PETICIONES, TOPIC_EVENTOS_WS], "central",
                                              confirmacion_manual=True)
            except (KafkaError, OSError) as error:
                LOG.warning("Kafka no disponible (%s). Reintento en 2 s", error)
                self.parada.wait(2)
                continue
            LOG.info("Conectado a Kafka en %s", self.servidor_kafka, extra={"color": "verde"})
            try:
                while not self.parada.is_set():
                    lotes = consumidor.poll(timeout_ms=500)
                    for registros in lotes.values():
                        for registro in registros:
                            self._procesar_registro_kafka(registro)
                    if lotes:
                        # Se marca como leído solo lo ya procesado: si CENTRAL
                        # cae antes, al volver se relee (no se pierde nada).
                        consumidor.commit()
            except (KafkaError, OSError, ValueError) as error:
                LOG.warning("Error leyendo de Kafka: %s", error)
            finally:
                consumidor.close()

    def _procesar_registro_kafka(self, registro):
        try:
            if registro.topic == TOPIC_PETICIONES:
                # registro.timestamp: milisegundos desde 1970 en que Kafka lo
                # guardó (hora del broker, ver CONFIG_TOPICS en kafka_utils).
                antiguedad_s = time.time() - registro.timestamp / 1000
                self.gestor.procesar_peticion(registro.value, antiguedad_s)
            else:
                self.gestor.procesar_evento(registro.value)
        except (sqlite3.Error, ValueError, TypeError, AttributeError):
            # Un mensaje mal formado no debe parar la lectura de los demás.
            LOG.exception("Error procesando un mensaje de %s", registro.topic)

    def tareas_periodicas(self):
        siguiente_estado = 0.0
        while not self.parada.wait(0.5):
            self.gestor.revisar_plazos()
            # Sin productor no hay a dónde enviarlo: se evita un aviso cada 2 s.
            if self.productor is not None and time.monotonic() >= siguiente_estado:
                self.gestor.publicar_estado_red()
                siguiente_estado = time.monotonic() + INTERVALO_ESTADO_RED_S

    # --- Sockets de los Monitores -------------------------------------------

    def _cifrar(self, conexion, direccion):
        """Handshake TLS como servidor. Devuelve el socket cifrado o None si falla.

        Se hace en el hilo de la conexión, no en el bucle de accept: un cliente
        lento o que no hable TLS no bloquea a los demás.
        """
        conexion.settimeout(self.timeout)
        try:
            segura = self.contexto_tls.wrap_socket(conexion, server_side=True)
        except (ssl.SSLError, OSError) as error:
            LOG.warning("Handshake TLS fallido con %s: %s", direccion, error)
            self._auditar_tls_fallido(direccion, error)
            conexion.close()
            with self._lock:
                self._conexiones.pop(conexion, None)
            return None
        with self._lock:
            # wrap_socket deja inservible el socket original: se registra el nuevo
            # para que servir() pueda desbloquearlo al parar.
            self._conexiones[segura] = self._conexiones.pop(conexion, None)
        LOG.debug("TLS con %s: %s %s", direccion, segura.version(), segura.cipher()[0])
        return segura

    def _auditar_tls_fallido(self, direccion, error):
        try:
            self.bd.insertar_auditoria("WS", "AUTH_KO", "KO", ip=str(direccion[0]),
                                       detalle={"motivo": "HANDSHAKE_TLS",
                                                "error": str(error)[:200]})
        except sqlite3.Error as fallo:
            LOG.warning("No se pudo auditar el fallo TLS: %s", fallo)

    def atender(self, conexion, direccion):
        if self.contexto_tls is not None:
            conexion = self._cifrar(conexion, direccion)
            if conexion is None:
                return
        sesion = SesionMonitor(self.registro, self.gestor)
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

    def arrancar_panel(self):
        """Panel web en su propio hilo. Devuelve el servidor para poder pararlo."""
        eventos = RegistroEventos()
        LOG.addHandler(eventos)
        panel = crear_panel(self, eventos, puerto=self.puerto_http)
        threading.Thread(target=panel.serve_forever, daemon=True).start()
        LOG.info("Panel web en http://localhost:%s", panel.server_address[1])
        return panel, eventos

    def servir(self):
        familia = socket.AF_INET6 if ":" in self.host else socket.AF_INET
        panel = eventos = None
        if self.puerto_http:
            panel, eventos = self.arrancar_panel()
        try:
            with socket.socket(familia, socket.SOCK_STREAM) as servidor:
                servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                servidor.bind((self.host, self.puerto))
                servidor.listen()
                servidor.settimeout(0.25)
                self.puerto = servidor.getsockname()[1]
                LOG.info("TCP escuchando en %s:%s (%s)", self.host, self.puerto,
                         "TLS" if self.contexto_tls is not None else "SIN cifrar")
                if self.servidor_kafka:
                    for objetivo in (self.escuchar_kafka, self.tareas_periodicas):
                        threading.Thread(target=objetivo, daemon=True).start()
                else:
                    LOG.warning("Sin Kafka: solo se atienden los Monitores")
                self.lista.set()
                while not self.parada.is_set():
                    try:
                        conexion, direccion = servidor.accept()
                    except socket.timeout:
                        # socket.timeout y no TimeoutError: en Python 3.9 son distintos.
                        continue
                    with self._lock:
                        lleno = len(self._conexiones) >= self.max_conexiones
                        if not lleno:
                            hilo = threading.Thread(target=self.atender,
                                                    args=(conexion, direccion))
                            self._conexiones[conexion] = hilo
                    if lleno and self.contexto_tls is not None:
                        # Sin handshake no se le puede mandar nada: se cierra.
                        LOG.warning("Límite de conexiones alcanzado: %s", direccion)
                        conexion.close()
                    elif lleno:
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
                    # Se llama al shutdown de socket.socket: el de SSLSocket
                    # desactivaría el cifrado y el EOT saldría en claro.
                    socket.socket.shutdown(conexion, socket.SHUT_RD)
                except OSError:
                    pass
            for _, hilo in pendientes:
                hilo.join()
            if panel is not None:
                panel.shutdown()
                panel.server_close()
                LOG.removeHandler(eventos)
            LOG.info("Central detenida")


def servir_panel_remoto(parser, url_central, puerto_http):
    consola.escribir(f"WM_Central · solo panel web en el puerto {puerto_http}, "
                     f"datos de {url_central}", "negrita")
    try:
        servidor = crear_panel_remoto(url_central, puerto=puerto_http)
    except OSError as error:
        parser.exit(1, f"No se pudo abrir el panel: {error}\n")
    # ECS para el contenedor con SIGTERM: se sale de serve_forever sin traza.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    try:
        servidor.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        servidor.server_close()


def cargar_tls(parser, args):
    """Contexto TLS de CENTRAL, o None solo con --sin-tls.

    En Release 2 el canal con los Monitores va siempre cifrado: si faltan el
    certificado o la clave, CENTRAL no arranca en lugar de caer a texto en claro.
    """
    if args.sin_tls:
        LOG.warning("TLS desactivado (--sin-tls): los Monitores hablarán en claro")
        return None
    cert = args.cert or CERT_TLS
    # Como en el ejemplo de la asignatura, certificado y clave pueden ir en un mismo PEM.
    clave = args.key or (args.cert if args.cert else CLAVE_TLS)
    if not (Path(cert).is_file() and Path(clave).is_file()):
        parser.exit(1, f"Faltan el certificado o la clave TLS ({cert}, {clave}).\n"
                       "Genéralos con: python scripts/generar_certificados.py IP_PUBLICA\n"
                       "o arranca sin cifrado (solo pruebas) con --sin-tls\n")
    try:
        contexto = contexto_tls_servidor(cert, clave)
    except (ssl.SSLError, OSError) as error:
        parser.exit(1, f"Certificado TLS no válido ({cert}): {error}\n")
    LOG.info("TLS activo con los Monitores (certificado %s)", cert)
    return contexto


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("puerto", nargs="?", type=puerto,
                        default=os.getenv("WM_SOCKET_PORT", os.getenv("PORT", "9999")))
    parser.add_argument("kafka", nargs="?", default=os.getenv("WM_KAFKA_SERVER"),
                        help="ip:puerto del broker de Kafka. Sin él no hay ciclo de riego")
    parser.add_argument("--host", default=os.getenv("WM_SOCKET_HOST", "0.0.0.0"))
    parser.add_argument("--timeout", type=positivo,
                        default=os.getenv("WM_SOCKET_TIMEOUT", "6"))
    parser.add_argument("--max-conexiones", type=entero_positivo,
                        default=os.getenv("WM_MAX_CONEXIONES", "64"))
    parser.add_argument("--db", default=os.getenv("WM_DB_PATH", RUTA_BD),
                        help="Ruta del fichero SQLite (por defecto data/wm.db)")
    parser.add_argument("--http", type=int, default=os.getenv("WM_HTTP_PORT", "8080"),
                        help="Puerto del panel web (0 para no abrirlo)")
    parser.add_argument("--remota", default=os.getenv("WM_CENTRAL_REMOTA"),
                        help="URL del panel de una CENTRAL en otra máquina "
                             "(http://IP:8080): solo se sirve su panel (ECS)")
    parser.add_argument("--cert", default=os.getenv("WM_TLS_CERT"),
                        help="Certificado TLS para los Monitores (por defecto certs/servidor.pem)")
    parser.add_argument("--key", default=os.getenv("WM_TLS_KEY"),
                        help="Clave privada del certificado (por defecto certs/servidor.key). "
                             "Puede ser el mismo fichero que --cert si contiene ambos")
    parser.add_argument("--sin-tls", action="store_true",
                        default=os.getenv("WM_SIN_TLS", "") == "1",
                        help="Sockets en claro, como en Release 1 (solo pruebas)")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)
    consola.configurar(logging.DEBUG if args.verbose else logging.INFO)
    if args.remota:
        servir_panel_remoto(parser, args.remota, args.http)
        return
    contexto_tls = cargar_tls(parser, args)
    consola.escribir(f"WM_Central · sockets en el puerto {args.puerto}, "
                     f"Kafka en {args.kafka or '(sin Kafka)'}", "negrita")
    try:
        central = CentralTCP(args.host, args.puerto, args.timeout, args.max_conexiones,
                             args.db, args.kafka, args.http, contexto_tls)
        for senal in (signal.SIGINT, signal.SIGTERM):
            signal.signal(senal, lambda *_: central.detener())
        central.servir()
    except (OSError, sqlite3.Error, ValueError) as error:
        parser.exit(1, f"Error en la central: {error}\n")


if __name__ == "__main__":
    main()
