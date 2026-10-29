"""Aplicación del operario (Field Operator): pide riegos a CENTRAL y sigue su evolución.

Envía solicitudes a wm.peticiones y escucha wm.notificaciones (solo las suyas)
y wm.estado_red (todas las estaciones). Las peticiones de un fichero se lanzan
de una en una: tras terminar cada una (con éxito o no) se esperan 4 segundos.
"""

import argparse
import logging
import os
from pathlib import Path
import queue
import signal
import sys
import threading
import time

from kafka.errors import KafkaError

# Permite importar "common" (en la raíz del repositorio) se lance desde donde se lance.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.configuracion import campo_valido, positivo  # noqa: E402
from common.kafka_utils import (  # noqa: E402
    TOPIC_ESTADO_RED, TOPIC_NOTIFICACIONES, TOPIC_PETICIONES,
    crear_consumidor, crear_productor, enviar)
from common.latido import MENSAJE_CAIDA, PLAZO_S, LatidoCentral  # noqa: E402

LOG = logging.getLogger("WM_FO")

ESPERA_ENTRE_PETICIONES_S = 4.0
# Si CENTRAL pasa este tiempo sin decir nada de una petición, se da por fallida.
# Mientras se riega llega telemetría cada segundo, así que no salta en un riego largo.
SILENCIO_MAXIMO_S = 10.0
FINALES = ("DENEGADO", "RESUMEN")
MARCAS = {"PASO": "[...]", "AUTORIZADO": "[ OK]", "DENEGADO": "[ NO]",
          "RESUMEN": "[FIN]"}


def leer_fichero(ruta):
    """Una petición por línea: ws_id;duracion_en_segundos. Ignora vacías y #comentarios."""
    peticiones = []
    with open(ruta, encoding="utf-8") as fichero:
        for numero, linea in enumerate(fichero, start=1):
            linea = linea.strip()
            if not linea or linea.startswith("#"):
                continue
            try:
                ws_id, duracion = (campo.strip() for campo in linea.split(";"))
                duracion_s = int(duracion)
                if not campo_valido(ws_id) or duracion_s <= 0:
                    raise ValueError
            except ValueError:
                raise ValueError(f"Línea {numero} no válida: {linea!r} "
                                 "(formato ws_id;segundos)") from None
            peticiones.append((ws_id.upper(), duracion_s))
    return peticiones


def formatear(notificacion):
    tipo = notificacion.get("tipo")
    ws_id = notificacion.get("ws_id")
    if tipo == "TELEMETRIA":
        return (f"      {ws_id} regando: {notificacion.get('caudal_lpm', 0):.2f} L/min | "
                f"{notificacion.get('volumen_l', 0):.2f} L | {notificacion.get('segundos')} s")
    return f"{MARCAS.get(tipo, '[ ? ]')} {ws_id}: {notificacion.get('texto')}"


class Operario:
    def __init__(self, servidor_kafka, fo_id):
        self.servidor_kafka = servidor_kafka
        self.fo_id = fo_id
        self.parada = threading.Event()
        self.listo = threading.Event()
        self.productor = None
        self.estaciones = []
        self.latido = LatidoCentral()
        self._en_curso = None          # peticion_id que se está siguiendo
        self._eventos = queue.Queue()  # notificaciones de esa petición

    # --- Kafka ---------------------------------------------------------------

    def escuchar(self):
        # Grupo fijo por operario: Kafka recuerda hasta dónde leyó. Si el FO se
        # cierra durante un riego, al volver recibe lo que se envió mientras
        # tanto (por ejemplo el RESUMEN). Por eso no conviene abrir dos FO con el
        # mismo ID a la vez: en un mismo grupo se reparten los mensajes.
        grupo = f"fo-{self.fo_id}"
        while not self.parada.is_set():
            try:
                if self.productor is None:
                    self.productor = crear_productor(self.servidor_kafka)
                consumidor = crear_consumidor(
                    self.servidor_kafka, [TOPIC_NOTIFICACIONES, TOPIC_ESTADO_RED], grupo)
            except (KafkaError, OSError) as error:
                LOG.warning("Kafka no disponible (%s). Reintento en 2 s", error)
                self.parada.wait(2)
                continue
            try:
                while not self.parada.is_set():
                    lotes = consumidor.poll(timeout_ms=500)
                    if not self.listo.is_set() and consumidor.assignment():
                        # Con "latest", lo enviado antes de tener particiones se
                        # pierde: no se piden riegos hasta estar escuchando.
                        for particion in consumidor.assignment():
                            consumidor.position(particion)
                        LOG.info("Conectado a Kafka en %s", self.servidor_kafka)
                        self.latido.empezar()
                        self.listo.set()
                    for registros in lotes.values():
                        for registro in registros:
                            # registro.timestamp: milisegundos desde 1970.
                            antiguedad_s = time.time() - registro.timestamp / 1000
                            self._recibir(registro.topic, registro.value, antiguedad_s)
                    if self.listo.is_set():
                        self.revisar_central()
            except (KafkaError, OSError, ValueError) as error:
                LOG.warning("Error leyendo de Kafka: %s", error)
            finally:
                consumidor.close()

    def revisar_central(self):
        if self.latido.revisar():
            print(f"[ NO] {MENSAJE_CAIDA}: no se pueden solicitar riegos", flush=True)

    def _recibir(self, topic, mensaje, antiguedad_s=0.0):
        if not isinstance(mensaje, dict):
            return
        if topic == TOPIC_ESTADO_RED:
            self.estaciones = mensaje.get("estaciones", [])
            # Un ESTADO_RED antiguo (leído al volver) no dice nada de CENTRAL ahora.
            if antiguedad_s < PLAZO_S and self.latido.recibido():
                print("[ OK] WM_Central disponible de nuevo", flush=True)
        elif mensaje.get("fo_id") == self.fo_id:
            if mensaje.get("tipo") == "TELEMETRIA" and antiguedad_s > PLAZO_S:
                return  # telemetría atrasada de mientras el FO estaba cerrado
            previo = "(mientras estabas desconectado) " if antiguedad_s > PLAZO_S else ""
            print(previo + formatear(mensaje), flush=True)
            if mensaje.get("peticion_id") == self._en_curso:
                self._eventos.put(mensaje)

    # --- Peticiones ----------------------------------------------------------

    def solicitar(self, ws_id, duracion_s):
        """Pide un riego y espera a que termine. Devuelve True si llegó a regar."""
        if not self.latido.disponible:
            print(f"[ NO] {MENSAJE_CAIDA}: inténtalo cuando vuelva")
            return False
        peticion_id = f"{self.fo_id}-{int(time.time() * 1000)}"
        self._eventos = queue.Queue()
        self._en_curso = peticion_id
        print(f"\nSolicitando riego en {ws_id} durante {duracion_s} s ({peticion_id})")
        try:
            enviar(self.productor, TOPIC_PETICIONES, {
                "tipo": "SOLICITUD", "peticion_id": peticion_id, "origen": "FO",
                "fo_id": self.fo_id, "ws_id": ws_id, "duracion_s": duracion_s,
            }, clave=self.fo_id)
        except (KafkaError, OSError) as error:
            print(f"[ NO] No se pudo enviar la solicitud: {error}")
            return False
        try:
            while True:
                try:
                    evento = self._eventos.get(timeout=SILENCIO_MAXIMO_S)
                except queue.Empty:
                    if not self.latido.disponible:
                        # CENTRAL caída: el riego sigue en la estación y el
                        # resultado llegará cuando CENTRAL vuelva.
                        continue
                    print(f"[ NO] {MENSAJE_CAIDA} ({SILENCIO_MAXIMO_S:.0f} s sin noticias "
                          "de la solicitud)")
                    return False
                if evento["tipo"] in FINALES:
                    return evento["tipo"] == "RESUMEN"
        finally:
            self._en_curso = None

    def ejecutar_fichero(self, ruta):
        peticiones = leer_fichero(ruta)
        print(f"{len(peticiones)} peticiones en {ruta}")
        resultados = []
        for indice, (ws_id, duracion_s) in enumerate(peticiones):
            resultados.append(self.solicitar(ws_id, duracion_s))
            if indice < len(peticiones) - 1:
                print(f"Esperando {ESPERA_ENTRE_PETICIONES_S:.0f} s para la siguiente...")
                time.sleep(ESPERA_ENTRE_PETICIONES_S)
        print(f"Fichero terminado: {sum(resultados)} de {len(resultados)} riegos realizados")
        return resultados

    def mostrar_estaciones(self):
        if not self.estaciones:
            print("Aún no se ha recibido el estado de la red desde CENTRAL")
            return
        print(f"\n{'ESTACIÓN':<10} {'UBICACIÓN':<25} ESTADO")
        for estacion in self.estaciones:
            marca = "  <- disponible" if estacion["estado"] == "DISPONIBLE" else ""
            print(f"{estacion['ws_id']:<10} {estacion['ubicacion']:<25} "
                  f"{estacion['estado']}{marca}")


MENU = """
--- WM_FO ({fo_id}) ---
1. Ver estaciones
2. Solicitar riego
3. Ejecutar peticiones de un fichero
0. Salir"""


def menu(operario):
    while True:
        print(MENU.format(fo_id=operario.fo_id))
        opcion = input("Opción: ").strip()
        if opcion == "1":
            operario.mostrar_estaciones()
        elif opcion == "2":
            # Los IDs de estación se usan siempre en mayúsculas (ws-04 = WS-04).
            ws_id = input("ID de la estación: ").strip().upper()
            try:
                duracion_s = int(input("Duración en segundos: "))
                if not campo_valido(ws_id) or duracion_s <= 0:
                    raise ValueError
            except ValueError:
                print("Datos no válidos")
                continue
            operario.solicitar(ws_id, duracion_s)
        elif opcion == "3":
            try:
                operario.ejecutar_fichero(input("Ruta del fichero: ").strip())
            except (OSError, ValueError) as error:
                print(f"No se puede usar el fichero: {error}")
        elif opcion == "0":
            return
        else:
            print("Opción no válida")


def interrumpir(*_):
    raise KeyboardInterrupt


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("kafka", nargs="?", default=os.getenv("WM_KAFKA_SERVER"),
                        help="ip:puerto del broker de Kafka")
    parser.add_argument("fo_id", nargs="?", default=os.getenv("WM_FO_ID"),
                        help="ID del operario dado de alta en CENTRAL")
    parser.add_argument("--fichero", help="Peticiones a lanzar al arrancar (ws_id;segundos)")
    parser.add_argument("--espera-kafka", type=positivo, default="30",
                        help="Segundos máximos esperando a conectar con Kafka")
    args = parser.parse_args(argv)
    if not args.kafka or not campo_valido(args.fo_id or ""):
        parser.error("Indica Kafka (ip:puerto) y el ID del operario")
    if args.fichero:
        try:
            leer_fichero(args.fichero)  # se valida antes de conectar
        except (OSError, ValueError) as error:
            parser.error(f"Fichero de peticiones: {error}")
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("kafka").setLevel(logging.WARNING)

    operario = Operario(args.kafka, args.fo_id)
    signal.signal(signal.SIGTERM, interrumpir)
    threading.Thread(target=operario.escuchar, daemon=True).start()
    try:
        print(f"Conectando con Kafka en {args.kafka}...")
        if not operario.listo.wait(args.espera_kafka):
            parser.exit(1, "No se pudo conectar con Kafka\n")
        if args.fichero:
            operario.ejecutar_fichero(args.fichero)
        menu(operario)
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        operario.parada.set()
        if operario.productor is not None:
            operario.productor.close(timeout=2)


if __name__ == "__main__":
    main()
