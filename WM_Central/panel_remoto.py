"""Panel web de una CENTRAL que corre en otra máquina (despliegue en ECS).

ECS Express solo publica HTTP, así que CENTRAL (sockets, Kafka y SQLite) corre
en EC2. Este servidor sirve la misma página del panel y reenvía sus dos
llamadas (/api/estado y /api/orden) al panel de esa CENTRAL.
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import urllib.error
import urllib.request

from panel_web import MAX_CUERPO, PAGINA

TIMEOUT_S = 5
TIPO_JSON = "application/json; charset=utf-8"


def crear_manejador(url_central):
    url_central = url_central.rstrip("/")

    class ManejadorRemoto(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/":
                self._responder(200, PAGINA.read_bytes(), "text/html; charset=utf-8")
            elif self.path == "/api/estado":
                self._reenviar()
            else:
                self._error(404, "No encontrado")

        def do_POST(self):
            if self.path != "/api/orden":
                self._error(404, "No encontrado")
                return
            try:
                longitud = int(self.headers.get("Content-Length", 0))
            except ValueError:
                longitud = 0
            if not 0 < longitud <= MAX_CUERPO:
                self._error(400, "Cuerpo vacío o demasiado grande")
                return
            self._reenviar(self.rfile.read(longitud))

        def _reenviar(self, cuerpo=None):
            # Con cuerpo, urllib hace un POST; sin él, un GET.
            peticion = urllib.request.Request(
                url_central + self.path, data=cuerpo,
                headers={"Content-Type": "application/json"} if cuerpo else {})
            try:
                with urllib.request.urlopen(peticion, timeout=TIMEOUT_S) as respuesta:
                    self._responder(respuesta.status, respuesta.read(), TIPO_JSON)
            except urllib.error.HTTPError as error:
                # 400 o 404 de CENTRAL: se pasa tal cual para que el panel muestre el motivo.
                with error:
                    self._responder(error.code, error.read(), TIPO_JSON)
            except OSError as error:
                # URLError y los timeouts heredan de OSError: CENTRAL no contesta.
                self._error(502, f"Sin conexión con WM_Central ({error})")

        def _error(self, codigo, texto):
            self._responder(codigo, json.dumps({"error": texto}, ensure_ascii=False)
                            .encode("utf-8"), TIPO_JSON)

        def _responder(self, codigo, cuerpo, tipo):
            self.send_response(codigo)
            self.send_header("Content-Type", tipo)
            self.send_header("Content-Length", str(len(cuerpo)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(cuerpo)

        def log_message(self, formato, *args):
            pass

    return ManejadorRemoto


def crear_panel_remoto(url_central, host="0.0.0.0", puerto=8080):
    """Devuelve el servidor sin arrancar, como crear_panel."""
    return ThreadingHTTPServer((host, puerto), crear_manejador(url_central))
