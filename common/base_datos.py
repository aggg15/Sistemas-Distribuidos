"""Persistencia SQLite: cada operación abre y cierra su propia conexión."""

from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
import sqlite3

from common.configuracion import campo_valido

RUTA_BD = "data/wm.db"

ESQUEMA = """
CREATE TABLE IF NOT EXISTS operarios (
    id TEXT PRIMARY KEY NOT NULL,
    nombre TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS estaciones (
    id TEXT PRIMARY KEY NOT NULL,
    ubicacion TEXT NOT NULL,
    bloqueada INTEGER NOT NULL DEFAULT 0 CHECK (bloqueada IN (0, 1)),
    ultima_conexion TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS riegos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ws_id TEXT NOT NULL REFERENCES estaciones(id),
    fo_id TEXT REFERENCES operarios(id),
    origen TEXT NOT NULL CHECK (origen IN ('FO', 'WS', 'CENTRAL')),
    inicio TEXT,
    fin TEXT,
    duracion_s INTEGER CHECK (duracion_s >= 0),
    volumen_l REAL CHECK (volumen_l >= 0),
    motivo_fin TEXT CHECK (motivo_fin IN
        ('TIEMPO', 'MANUAL', 'FUGA', 'BLOQUEO', 'DESCONEXION')),
    CHECK (origen != 'FO' OR fo_id IS NOT NULL)
);
"""


def _ahora():
    # Se guarda en UTC; scripts/gestionar_bd.py lo muestra en hora de Madrid.
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class BaseDatos:
    def __init__(self, ruta=RUTA_BD):
        if str(ruta) == ":memory:":
            raise ValueError("Usa un fichero SQLite: las conexiones son independientes")
        self.ruta = Path(ruta)
        self.ruta.parent.mkdir(parents=True, exist_ok=True)
        with self._conexion() as conexion:
            conexion.execute("PRAGMA journal_mode=WAL")
            conexion.executescript(ESQUEMA)

    @contextmanager
    def _conexion(self):
        # No se comparte una Connection entre hilos. El plazo de los bloqueos
        # SQLite es menor que el del protocolo para poder comunicar el error.
        conexion = sqlite3.connect(str(self.ruta), timeout=1.0)
        try:
            conexion.row_factory = sqlite3.Row
            conexion.execute("PRAGMA foreign_keys=ON")
            with conexion:
                yield conexion
        finally:
            conexion.close()

    def registrar_estacion(self, ws_id, ubicacion):
        if not campo_valido(ws_id) or not campo_valido(ubicacion):
            raise ValueError("ID o ubicación de estación no válidos")
        instante = _ahora()
        with self._conexion() as conexion:
            # No se reemplaza la fila: se conservan bloqueo y claves del historial.
            conexion.execute(
                """INSERT INTO estaciones (id, ubicacion, ultima_conexion)
                   VALUES (?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                       ubicacion=excluded.ubicacion,
                       ultima_conexion=excluded.ultima_conexion""",
                (ws_id, ubicacion, instante),
            )
            return dict(conexion.execute(
                "SELECT * FROM estaciones WHERE id=?", (ws_id,)
            ).fetchone())

    def listar_estaciones(self):
        with self._conexion() as conexion:
            return [dict(fila) for fila in conexion.execute(
                "SELECT * FROM estaciones ORDER BY id"
            )]

    def guardar_operario(self, fo_id, nombre):
        if not campo_valido(fo_id) or not campo_valido(nombre):
            raise ValueError("ID o nombre de operario no válidos")
        with self._conexion() as conexion:
            conexion.execute(
                """INSERT INTO operarios (id, nombre) VALUES (?, ?)
                   ON CONFLICT(id) DO UPDATE SET nombre=excluded.nombre""",
                (fo_id, nombre),
            )

    def listar_operarios(self):
        with self._conexion() as conexion:
            return [dict(fila) for fila in conexion.execute(
                "SELECT id, nombre FROM operarios ORDER BY id"
            )]

    def operario_existe(self, fo_id):
        with self._conexion() as conexion:
            return conexion.execute(
                "SELECT 1 FROM operarios WHERE id=?", (fo_id,)
            ).fetchone() is not None

    def guardar_bloqueo(self, ws_id, bloqueada):
        with self._conexion() as conexion:
            conexion.execute("UPDATE estaciones SET bloqueada=? WHERE id=?",
                             (1 if bloqueada else 0, ws_id))

    # --- Historial de riegos ------------------------------------------------
    # Ciclo de una fila: crear_riego (al autorizar) → iniciar_riego (cuando el
    # Engine abre la electroválvula) → cerrar_riego. Si el Engine lo rechaza o
    # no contesta, borrar_riego: nunca llegó a regar.

    def crear_riego(self, ws_id, fo_id, origen):
        with self._conexion() as conexion:
            cursor = conexion.execute(
                "INSERT INTO riegos (ws_id, fo_id, origen) VALUES (?, ?, ?)",
                (ws_id, fo_id, origen),
            )
            return cursor.lastrowid

    def iniciar_riego(self, riego_id):
        with self._conexion() as conexion:
            conexion.execute("UPDATE riegos SET inicio=? WHERE id=?", (_ahora(), riego_id))

    def cerrar_riego(self, riego_id, duracion_s, volumen_l, motivo):
        # COALESCE: si el Engine envía después sus datos definitivos de un riego
        # que CENTRAL ya cerró, se corrigen volumen y duración pero no la hora de fin.
        with self._conexion() as conexion:
            conexion.execute(
                """UPDATE riegos SET fin=COALESCE(fin, ?), duracion_s=?, volumen_l=?,
                   motivo_fin=? WHERE id=?""",
                (_ahora(), duracion_s, volumen_l, motivo, riego_id),
            )

    def obtener_riego(self, riego_id):
        with self._conexion() as conexion:
            fila = conexion.execute("SELECT * FROM riegos WHERE id=?", (riego_id,)).fetchone()
            return dict(fila) if fila is not None else None

    def volumen_por_estacion(self):
        """Litros suministrados en total por cada estación (riegos ya cerrados)."""
        with self._conexion() as conexion:
            return {fila["ws_id"]: fila["total"] for fila in conexion.execute(
                """SELECT ws_id, COALESCE(SUM(volumen_l), 0) AS total
                   FROM riegos WHERE fin IS NOT NULL GROUP BY ws_id"""
            )}

    def borrar_riego(self, riego_id):
        with self._conexion() as conexion:
            conexion.execute("DELETE FROM riegos WHERE id=?", (riego_id,))

    def listar_riegos(self):
        with self._conexion() as conexion:
            return [dict(fila) for fila in conexion.execute(
                "SELECT * FROM riegos ORDER BY id"
            )]
