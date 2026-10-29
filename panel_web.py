"""Panel web de CENTRAL: estado de la red en tiempo real y órdenes del punto 11.

Usa solo la biblioteca estándar (http.server). Rutas:
  GET  /             página del panel (panel/index.html)
  GET  /api/estado   estaciones, últimos eventos y si hay conexión con Kafka
  POST /api/orden    {"accion": "INICIAR"|"BLOQUEAR"|"ACTIVAR",
                      "ws_id": "WS-04" o null para todas, "duracion_s": 30}
"""

from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
from pathlib import Path
import threading

LOG = logging.getLogger("WM_Central")

PAGINA = Path(__file__).resolve().parent / "panel" / "index.html"
MAX_CUERPO = 10_000      # bytes: una orden es un JSON pequeño
MAX_DURACION_S = 3600
ACCIONES = ("INICIAR", "BLOQUEAR", "ACTIVAR")


class RegistroEventos(logging.Handler):
    """Guarda los últimos mensajes de CENTRAL para mostrarlos en el panel."""

    def __init__(self, maximo=100):
        super().__init__(level=logging.INFO)
        self.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s",
                                            datefmt="%H:%M:%S"))
        self._eventos = deque(maxlen=maximo)
        self._lock_eventos = threading.Lock()

    def emit(self, registro):
        texto = self.format(registro)
        with self._lock_eventos:
            self._eventos.append({"nivel": registro.levelname, "texto": texto})

    def ultimos(self):
        with self._lock_eventos:
            return list(self._eventos)


class PeticionNoValida(Exception):
    pass


def validar_orden(datos, estaciones):
    if not isinstance(datos, dict):
        raise PeticionNoValida("Se esperaba un objeto JSON")
    accion = datos.get("accion")
    if accion not in ACCIONES:
        raise PeticionNoValida(f"Acción no válida: usa {', '.join(ACCIONES)}")
    ws_id = datos.get("ws_id")
    if ws_id is not None and ws_id not in estaciones:
        raise PeticionNoValida("La estación no existe")
    duracion_s = None
    if accion == "INICIAR":
        duracion_s = datos.get("duracion_s")
        # bool es un int en Python: se descarta explícitamente.
        if (not isinstance(duracion_s, int) or isinstance(duracion_s, bool)
                or not 0 < duracion_s <= MAX_DURACION_S):
            raise PeticionNoValida(f"Duración entre 1 y {MAX_DURACION_S} segundos")
    return accion, ws_id, duracion_s


def crear_manejador(central, eventos):
    class ManejadorPanel(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/":
                self._responder(200, PAGINA.read_bytes(), "text/html; charset=utf-8")
            elif self.path == "/api/estado":
                totales = central.bd.volumen_por_estacion()
                estaciones = [{"ws_id": ws_id, **datos,
                               "volumen_total_l": totales.get(ws_id, 0.0)}
                              for ws_id, datos in sorted(central.registro.estado().items())]
                self._json(200, {"estaciones": estaciones, "eventos": eventos.ultimos(),
                                 "kafka": central.productor is not None,
                                 "broker": central.servidor_kafka})
            else:
                self._json(404, {"error": "No encontrado"})

        def do_POST(self):
            if self.path != "/api/orden":
                self._json(404, {"error": "No encontrado"})
                return
            try:
                longitud = int(self.headers.get("Content-Length", 0))
                if not 0 < longitud <= MAX_CUERPO:
                    raise PeticionNoValida("Cuerpo vacío o demasiado grande")
                datos = json.loads(self.rfile.read(longitud).decode("utf-8"))
                accion, ws_id, duracion_s = validar_orden(datos, central.registro.ids())
            except (PeticionNoValida, ValueError) as error:
                self._json(400, {"error": str(error)})
                return
            LOG.info("Panel: orden %s sobre %s", accion, ws_id or "todas las estaciones")
            central.gestor.ordenar(accion, ws_id, duracion_s)
            self._json(200, {"ok": True})

        def _json(self, codigo, datos):
            self._responder(codigo, json.dumps(datos, ensure_ascii=False).encode("utf-8"),
                            "application/json; charset=utf-8")

        def _responder(self, codigo, cuerpo, tipo):
            self.send_response(codigo)
            self.send_header("Content-Type", tipo)
            self.send_header("Content-Length", str(len(cuerpo)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(cuerpo)

        def log_message(self, formato, *args):
            # Cada consulta de /api/estado (una por segundo) llenaría la consola.
            pass

    return ManejadorPanel


def crear_panel(central, eventos, host="0.0.0.0", puerto=8080):
    """Devuelve el servidor sin arrancar: servidor.serve_forever() en un hilo."""
    return ThreadingHTTPServer((host, puerto), crear_manejador(central, eventos))
