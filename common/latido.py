"""Saber si WM_Central sigue viva sin hablar directamente con ella.

CENTRAL publica ESTADO_RED cada 2 s. Quien lo escucha (FO, Engine) considera
que CENTRAL está caída si pasa más de PLAZO_S sin recibirlo.
"""

import threading
import time

PLAZO_S = 6.0
MENSAJE_CAIDA = "Imposible conectar con WM_Central"


class LatidoCentral:
    def __init__(self, plazo_s=PLAZO_S, reloj=time.monotonic):
        self.plazo_s = plazo_s
        self.reloj = reloj
        self._ultimo = reloj()  # se da un plazo inicial antes de avisar
        self._disponible = None  # None: aún no se sabe
        self._lock = threading.Lock()

    def recibido(self):
        """Llamar con cada ESTADO_RED reciente. Devuelve True si CENTRAL acaba de volver."""
        with self._lock:
            self._ultimo = self.reloj()
            volvio = self._disponible is False
            self._disponible = True
            return volvio

    def revisar(self):
        """Llamar periódicamente. Devuelve True si CENTRAL acaba de dejar de responder."""
        with self._lock:
            if self._disponible is not False and self.reloj() - self._ultimo > self.plazo_s:
                self._disponible = False
                return True
            return False

    @property
    def disponible(self):
        with self._lock:
            return self._disponible is not False
