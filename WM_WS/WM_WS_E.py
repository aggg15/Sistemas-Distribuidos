"""Engine de una estación: electroválvula, caudalímetro y averías simuladas.

Hilos:
  - Monitor: socket TCP; recibe el ID y contesta a CHECK con OK o KO.
  - Kafka: recibe órdenes de CENTRAL (wm.ordenes).
  - Riego: cada segundo mide el caudal y publica la telemetría (wm.eventos_ws).
  - Principal: menú de la estación.
"""

import argparse
from dataclasses import asdict, dataclass
import itertools
import json
import logging
import os
from pathlib import Path
import random
import signal
import socket
import sys
import threading
import time

from kafka.errors import KafkaError

# Permite importar "common" (en la raíz del repositorio) se lance desde donde se lance.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.configuracion import positivo, puerto  # noqa: E402
from common.kafka_utils import (  # noqa: E402
    TOPIC_ESTADO_RED, TOPIC_EVENTOS_WS, TOPIC_ORDENES, TOPIC_PETICIONES,
    crear_consumidor, crear_productor, enviar)
from common.latido import MENSAJE_CAIDA, PLAZO_S, LatidoCentral  # noqa: E402
from common.protocolo import Canal, ConexionCerrada, ErrorProtocolo  # noqa: E402

LOG = logging.getLogger("WM_WS_E")

CAUDAL_MIN_LPM = 10.0
CAUDAL_MAX_LPM = 20.0
# Una orden de riego más antigua (p. ej. enviada mientras el Engine estaba
# caído) ya la canceló CENTRAL por falta de respuesta: no se ejecuta.
CADUCIDAD_ORDEN_S = 5.0


@dataclass
class Riego:
    riego_id: int
    fo_id: object  # None si el riego no lo pidió un operario
    duracion_s: int
    segundos: int = 0
    volumen_l: float = 0.0
    caudal_lpm: float = 0.0


class EstacionSimulada:
    """Estado de la estación. No usa red: publica a través de la función publicar,
    que devuelve True si el mensaje se ha entregado.

    Con carpeta_datos, el riego en curso y los resultados aún no entregados se
    guardan en disco: si el Engine se cae, al volver envía lo que llegó a regar.
    """

    def __init__(self, publicar, medir_caudal=None, carpeta_datos=None):
        self.publicar = publicar
        self.medir_caudal = medir_caudal or (
            lambda: random.uniform(CAUDAL_MIN_LPM, CAUDAL_MAX_LPM))
        self.carpeta_datos = carpeta_datos
        self.ws_id = None
        self.id_asignado = threading.Event()
        self.monitor_conectado = False
        self.latido = LatidoCentral()
        self.averia = False
        self.bloqueada = False
        self.riego = None
        self._pendientes = []  # RIEGO_FINALIZADO que Kafka aún no ha confirmado
        self._ruta = None
        self._lock = threading.Lock()
        self._peticiones = itertools.count(1)

    # --- Monitor -------------------------------------------------------------

    def responder_monitor(self, texto):
        campos = texto.split("#")
        if campos[0] == "ID" and len(campos) == 2 and campos[1]:
            with self._lock:
                if self.ws_id not in (None, campos[1]):
                    return "KO#Este Engine ya pertenece a otra estacion"
                self.ws_id = campos[1]
            self._monitor_presente()
            if not self.id_asignado.is_set():
                LOG.info("ID asignado por el Monitor: %s", self.ws_id)
                self._recuperar()
                self.id_asignado.set()
            return "OK"
        if texto == "CHECK":
            self._monitor_presente()
            return "KO" if self.averia else "OK"
        return "KO#Operacion desconocida"

    def _monitor_presente(self):
        with self._lock:
            volvio = not self.monitor_conectado
            self.monitor_conectado = True
        if volvio:
            LOG.info("Monitor conectado: la estación vuelve a estar operativa")

    def perder_monitor(self):
        """Sin Monitor, CENTRAL ve la estación DESCONECTADA: no se riega."""
        with self._lock:
            estaba = self.monitor_conectado
            self.monitor_conectado = False
        if estaba:
            LOG.warning("AVERIADA: sin conexión con el Monitor. No se admiten riegos")
            self.finalizar("DESCONEXION")

    # --- Persistencia local --------------------------------------------------

    def _guardar(self):
        """Llamar con el lock tomado. Escritura atómica: fichero temporal y replace."""
        if self._ruta is None:
            return
        datos = {"riego": asdict(self.riego) if self.riego else None,
                 "pendientes": self._pendientes}
        temporal = self._ruta.with_suffix(".tmp")
        temporal.write_text(json.dumps(datos), encoding="utf-8")
        os.replace(temporal, self._ruta)

    def _recuperar(self):
        """Al conocer el ID: si quedó un riego a medias por una caída, se da por
        terminado con lo que se llegó a regar y se deja pendiente de enviar."""
        if self.carpeta_datos is None:
            return
        ruta = Path(self.carpeta_datos) / f"engine_{self.ws_id}.json"
        ruta.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            self._ruta = ruta
            try:
                datos = json.loads(ruta.read_text(encoding="utf-8"))
            except FileNotFoundError:
                return
            except (OSError, ValueError) as error:
                LOG.warning("No se pudo leer %s: %s", ruta, error)
                return
            self._pendientes = datos.get("pendientes", [])
            anterior = datos.get("riego")
            if anterior:
                LOG.warning("Riego %s interrumpido por una caída del Engine: %.2f L en %s s",
                            anterior["riego_id"], anterior["volumen_l"], anterior["segundos"])
                self._pendientes.append(self._fin(Riego(**anterior), "FUGA", recuperado=True))
            self._guardar()

    def reenviar_pendientes(self):
        """Envía los resultados que no llegaron a CENTRAL. Se llama cada segundo."""
        with self._lock:
            pendientes = list(self._pendientes)
        for mensaje in pendientes:
            if not self.publicar(TOPIC_EVENTOS_WS, mensaje):
                return  # se reintentará en la próxima llamada
            with self._lock:
                self._pendientes.remove(mensaje)
                self._guardar()
            LOG.info("Estado final del riego %s enviado a CENTRAL", mensaje["riego_id"])

    # --- Órdenes de CENTRAL --------------------------------------------------

    def procesar_orden(self, orden):
        if not isinstance(orden, dict) or orden.get("ws_id") != self.ws_id:
            return  # Todas las estaciones leen el topic; cada una se queda con lo suyo.
        tipo = orden.get("tipo")
        LOG.info("Orden de CENTRAL: %s", tipo)
        if tipo == "INICIAR_RIEGO":
            self._iniciar_riego(orden)
        elif tipo == "PARAR":
            # Con riego_id solo se para ese riego: una orden atrasada no corta otro nuevo.
            self.finalizar(orden.get("motivo", "MANUAL"), orden.get("riego_id"))
        elif tipo == "BLOQUEAR":
            with self._lock:
                self.bloqueada = True
            LOG.warning("Estación FUERA DE SERVICIO por orden de CENTRAL")
            self.finalizar("BLOQUEO")
        elif tipo == "ACTIVAR":
            with self._lock:
                self.bloqueada = False
            LOG.info("Estación ACTIVADA por orden de CENTRAL")
        elif tipo == "DENEGADO":
            LOG.warning("CENTRAL deniega el riego solicitado: %s", orden.get("texto"))
        else:
            LOG.warning("Orden desconocida: %s", orden)

    def _iniciar_riego(self, orden):
        try:
            riego = Riego(int(orden["riego_id"]), orden.get("fo_id"), int(orden["duracion_s"]))
            if riego.duracion_s <= 0:
                raise ValueError("duración no positiva")
        except (KeyError, TypeError, ValueError):
            self._rechazar(orden.get("riego_id"), "ORDEN_INVALIDA")
            return
        with self._lock:
            if not self.monitor_conectado:
                motivo = "SIN_MONITOR"
            elif self.averia:
                motivo = "FUGA"
            elif self.bloqueada:
                motivo = "BLOQUEADA"
            elif self.riego is not None:
                motivo = "OCUPADA"
            else:
                motivo = None
                self.riego = riego
                self._guardar()
        if motivo is not None:
            self._rechazar(riego.riego_id, motivo)
            return
        LOG.info("Electroválvula ABIERTA: riego %s durante %s s (operario %s)",
                 riego.riego_id, riego.duracion_s, riego.fo_id or "-")
        self.publicar(TOPIC_EVENTOS_WS, {
            "tipo": "RIEGO_INICIADO", "ws_id": self.ws_id, "riego_id": riego.riego_id,
            "fo_id": riego.fo_id, "duracion_s": riego.duracion_s,
        })

    def _rechazar(self, riego_id, motivo):
        LOG.warning("Riego %s rechazado: %s", riego_id, motivo)
        self.publicar(TOPIC_EVENTOS_WS, {
            "tipo": "RIEGO_RECHAZADO", "ws_id": self.ws_id, "riego_id": riego_id,
            "motivo": motivo,
        })

    # --- Riego ---------------------------------------------------------------

    def avanzar_segundo(self):
        """Simula un segundo de riego: mide el caudal y acumula el volumen."""
        with self._lock:
            riego = self.riego
            if riego is None:
                return
            riego.caudal_lpm = self.medir_caudal()
            riego.volumen_l += riego.caudal_lpm / 60  # litros por minuto → por segundo
            riego.segundos += 1
            terminado = riego.segundos >= riego.duracion_s
            self._guardar()  # lo regado no se pierde aunque el Engine caiga ahora
            telemetria = {
                "tipo": "TELEMETRIA", "ws_id": self.ws_id, "riego_id": riego.riego_id,
                "caudal_lpm": round(riego.caudal_lpm, 2),
                "volumen_l": round(riego.volumen_l, 2), "segundos": riego.segundos,
            }
        LOG.info("Regando: %.2f L/min | %.2f L acumulados | %s/%s s",
                 riego.caudal_lpm, riego.volumen_l, riego.segundos, riego.duracion_s)
        self.publicar(TOPIC_EVENTOS_WS, telemetria)
        if terminado:
            self.finalizar("TIEMPO", riego.riego_id)

    def finalizar(self, motivo, riego_id=None):
        """Cierra la electroválvula. Con riego_id solo termina ese riego concreto."""
        with self._lock:
            riego = self.riego
            if riego is None or (riego_id is not None and riego.riego_id != riego_id):
                return False
            self.riego = None
            self._guardar()
        LOG.info("Electroválvula CERRADA (%s): riego %s | %.2f L en %s s",
                 motivo, riego.riego_id, riego.volumen_l, riego.segundos)
        mensaje = self._fin(riego, motivo)
        if not self.publicar(TOPIC_EVENTOS_WS, mensaje):
            # No se pierde: se reenvía en cuanto Kafka vuelva (reenviar_pendientes).
            with self._lock:
                self._pendientes.append({**mensaje, "recuperado": True})
                self._guardar()
        return True

    def _fin(self, riego, motivo, recuperado=False):
        mensaje = {"tipo": "RIEGO_FINALIZADO", "ws_id": self.ws_id,
                   "riego_id": riego.riego_id, "motivo": motivo,
                   "volumen_l": round(riego.volumen_l, 2), "duracion_s": riego.segundos}
        if recuperado:
            mensaje["recuperado"] = True
        return mensaje

    # --- Menú de la estación -------------------------------------------------

    def simular_averia(self, activa):
        with self._lock:
            self.averia = activa
        if activa:
            LOG.warning("AVERÍA simulada: el próximo CHECK responderá KO")
            # El enunciado exige cortar inmediatamente un riego en curso.
            self.finalizar("FUGA")
        else:
            LOG.info("Avería resuelta: el próximo CHECK responderá OK")

    def solicitar_riego(self, duracion_s):
        """Riego manual: se pide a CENTRAL, que es quien lo autoriza."""
        if self.ws_id is None:
            raise ValueError("La estación aún no tiene ID")
        if not self.monitor_conectado:
            raise ValueError("Estación AVERIADA: sin conexión con el Monitor")
        if not self.latido.disponible:
            raise ValueError(MENSAJE_CAIDA)
        peticion = {
            "tipo": "SOLICITUD", "peticion_id": f"{self.ws_id}-{next(self._peticiones)}",
            "origen": "WS", "ws_id": self.ws_id, "duracion_s": duracion_s,
        }
        if not self.publicar(TOPIC_PETICIONES, peticion):
            raise ValueError("Kafka no ha confirmado el envío")
        return peticion["peticion_id"]

    def resumen(self):
        with self._lock:
            if self.ws_id is None:
                return "Sin ID: esperando al Monitor"
            if not self.monitor_conectado:
                estado = "AVERIADA (sin conexión con el Monitor)"
            elif self.averia:
                estado = "AVERÍA (KO)"
            elif self.bloqueada:
                estado = "FUERA DE SERVICIO"
            elif self.riego is not None:
                r = self.riego
                estado = (f"REGANDO riego {r.riego_id}: {r.caudal_lpm:.2f} L/min, "
                          f"{r.volumen_l:.2f} L, {r.segundos}/{r.duracion_s} s")
            else:
                estado = "EN REPOSO"
            return f"{self.ws_id}: {estado}"


class Engine:
    """Conecta la estación simulada con el Monitor (sockets) y con Kafka."""

    def __init__(self, servidor_kafka, monitor_host, monitor_puerto, timeout=3.0,
                 reintento=2.0, carpeta_datos=None):
        self.servidor_kafka = servidor_kafka
        self.monitor = (monitor_host, monitor_puerto)
        self.timeout = timeout
        self.reintento = reintento
        self.parada = threading.Event()
        self.productor = None
        self.estacion = EstacionSimulada(self.publicar, carpeta_datos=carpeta_datos)

    def publicar(self, topic, mensaje):
        """Devuelve True solo si Kafka confirma la entrega."""
        if self.productor is None:
            LOG.warning("Kafka no disponible: no se ha enviado %s", mensaje.get("tipo"))
            return False
        try:
            enviar(self.productor, topic, mensaje, clave=self.estacion.ws_id)
            return True
        except (KafkaError, OSError) as error:
            LOG.warning("No se pudo enviar %s a Kafka: %s", mensaje.get("tipo"), error)
            return False

    def conectar_monitor(self):
        while not self.parada.is_set():
            try:
                with socket.create_connection(self.monitor, timeout=self.timeout) as conexion:
                    canal = Canal(conexion, timeout=self.timeout)
                    canal.saludar()
                    LOG.info("Conectado al Monitor en %s:%s", *self.monitor)
                    try:
                        while not self.parada.is_set():
                            canal.responder(self.estacion.responder_monitor)
                    finally:
                        canal.cerrar()
            except ConexionCerrada:
                LOG.warning("El Monitor cerró la conexión")
            except (OSError, ErrorProtocolo) as error:
                LOG.warning("Sin conexión con el Monitor (%s). Reintento en %.1f s",
                            error, self.reintento)
            if not self.parada.is_set():
                self.estacion.perder_monitor()
            self.parada.wait(self.reintento)

    def escuchar_ordenes(self):
        # Sin ID no se sabe qué órdenes son de esta estación.
        while not self.estacion.id_asignado.wait(0.25):
            if self.parada.is_set():
                return
        grupo = f"engine-{self.estacion.ws_id}"
        while not self.parada.is_set():
            try:
                if self.productor is None:
                    self.productor = crear_productor(self.servidor_kafka)
                consumidor = crear_consumidor(self.servidor_kafka,
                                              [TOPIC_ORDENES, TOPIC_ESTADO_RED], grupo)
            except (KafkaError, OSError) as error:
                LOG.warning("Kafka no disponible (%s). Reintento en %.1f s",
                            error, self.reintento)
                self.parada.wait(self.reintento)
                continue
            LOG.info("Conectado a Kafka en %s", self.servidor_kafka)
            try:
                while not self.parada.is_set():
                    # poll con plazo para poder comprobar la parada periódicamente.
                    for registros in consumidor.poll(timeout_ms=500).values():
                        for registro in registros:
                            self._recibir(registro)
            except (KafkaError, OSError, ValueError) as error:
                LOG.warning("Error leyendo órdenes de Kafka: %s", error)
            finally:
                consumidor.close()

    def _recibir(self, registro):
        # registro.timestamp: milisegundos desde 1970 en que se envió.
        antiguedad_s = time.time() - registro.timestamp / 1000
        mensaje = registro.value
        if registro.topic == TOPIC_ESTADO_RED:
            # Los antiguos (leídos al arrancar) no dicen nada de CENTRAL ahora.
            if antiguedad_s < PLAZO_S and self.estacion.latido.recibido():
                LOG.info("WM_Central disponible de nuevo")
            return
        if (isinstance(mensaje, dict) and mensaje.get("tipo") == "INICIAR_RIEGO"
                and antiguedad_s > CADUCIDAD_ORDEN_S):
            LOG.warning("Orden de riego %s caducada: se ignora", mensaje.get("riego_id"))
            return
        self.estacion.procesar_orden(mensaje)

    def simular_riego(self):
        while not self.parada.wait(1.0):
            self.estacion.avanzar_segundo()
            if self.productor is not None:
                self.estacion.reenviar_pendientes()
            if self.estacion.latido.revisar():
                LOG.warning("%s: no se admiten nuevos riegos (el riego en curso continúa)",
                            MENSAJE_CAIDA)

    def arrancar(self):
        for objetivo in (self.conectar_monitor, self.escuchar_ordenes, self.simular_riego):
            threading.Thread(target=objetivo, daemon=True).start()

    def detener(self):
        self.parada.set()
        # Un Engine que se apaga no puede seguir regando: se informa a CENTRAL.
        self.estacion.finalizar("DESCONEXION")
        if self.productor is not None:
            self.productor.close(timeout=2)


MENU = """
--- WM_WS_E ---
1. Solicitar riego manual
2. Finalizar riego en curso
3. Simular avería (KO)
4. Resolver avería (OK)
5. Ver estado
0. Salir"""


def menu(estacion):
    while True:
        print(MENU)
        opcion = input("Opción: ").strip()
        if opcion == "1":
            try:
                duracion = int(input("Duración en segundos: "))
                if duracion <= 0:
                    raise ValueError("debe ser positiva")
                peticion_id = estacion.solicitar_riego(duracion)
                print(f"Solicitud {peticion_id} enviada a CENTRAL")
            except ValueError as error:
                print(f"No se puede solicitar: {error}")
        elif opcion == "2":
            if not estacion.finalizar("MANUAL"):
                print("No hay ningún riego en curso")
        elif opcion == "3":
            estacion.simular_averia(True)
        elif opcion == "4":
            estacion.simular_averia(False)
        elif opcion == "5":
            print(estacion.resumen())
        elif opcion == "0":
            return
        else:
            print("Opción no válida")


def interrumpir(*_):
    # Convierte SIGTERM (docker stop) en lo mismo que Ctrl+C para salir del input().
    raise KeyboardInterrupt


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("kafka", nargs="?", default=os.getenv("WM_KAFKA_SERVER"),
                        help="ip:puerto del broker de Kafka")
    parser.add_argument("monitor_host", nargs="?", default=os.getenv("WM_MONITOR_HOST"))
    parser.add_argument("monitor_puerto", nargs="?", type=puerto,
                        default=os.getenv("WM_MONITOR_PORT"))
    parser.add_argument("--timeout", type=positivo, default=os.getenv("WM_SOCKET_TIMEOUT", "3"))
    parser.add_argument("--reintento", type=positivo,
                        default=os.getenv("WM_RECONNECT_DELAY", "2"))
    parser.add_argument("--datos", default=os.getenv("WM_ENGINE_DATA", "data"),
                        help="Carpeta donde se guarda el riego en curso (por defecto data)")
    args = parser.parse_args(argv)
    if not args.kafka or not args.monitor_host or args.monitor_puerto is None:
        parser.error("Indica Kafka (ip:puerto) y el host y puerto del Monitor")
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    # kafka-python registra cada intento de conexión; solo interesan sus avisos.
    logging.getLogger("kafka").setLevel(logging.WARNING)

    engine = Engine(args.kafka, args.monitor_host, args.monitor_puerto,
                    args.timeout, args.reintento, args.datos)
    signal.signal(signal.SIGTERM, interrumpir)
    engine.arrancar()
    try:
        try:
            menu(engine.estacion)
        except EOFError:
            # Sin teclado (por ejemplo, Docker sin -it): se sigue funcionando sin menú.
            LOG.info("Sin entrada estándar: el Engine funciona sin menú")
            # Espera con plazo para que Ctrl+C funcione también en Windows.
            while not engine.parada.wait(1):
                pass
    except KeyboardInterrupt:
        pass
    finally:
        engine.detener()
        LOG.info("Engine detenido")


if __name__ == "__main__":
    main()
