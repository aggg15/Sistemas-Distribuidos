"""Persistencia SQLite: cada operación abre y cierra su propia conexión."""

from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3

from common.configuracion import campo_valido

RUTA_BD = "data/wm.db"


class EstacionYaRegistrada(Exception):
    """Alta en el Registry de una estación que ya está en ALTA con credencial."""

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

-- Release 2: eventos de seguridad y operación (docs/release2/ARCHITECTURE.md §3.6).
-- Sin clave foránea: también se auditan IDs que no existen (altas o accesos fallidos).
CREATE TABLE IF NOT EXISTS auditoria (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fecha TEXT NOT NULL,
    origen_tipo TEXT NOT NULL,
    origen_id TEXT,
    ip TEXT,
    accion TEXT NOT NULL,
    resultado TEXT NOT NULL CHECK (resultado IN ('OK', 'KO')),
    detalle TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS auditoria_fecha ON auditoria (fecha);
CREATE INDEX IF NOT EXISTS auditoria_accion ON auditoria (accion);
"""

# Columnas de Release 2 en estaciones. La tabla se crea con el esquema de
# Release 1 y estas se añaden si faltan: así una BD nueva y una BD de R1
# migrada quedan exactamente iguales.
COLUMNAS_R2 = {
    "estado_registro": "TEXT NOT NULL DEFAULT 'ALTA' CHECK (estado_registro IN ('ALTA', 'BAJA'))",
    "credencial_hash": "TEXT",
    "fecha_alta": "TEXT",
    "fecha_baja": "TEXT",
    "clave_cifrado": "TEXT",
    "clave_emitida": "TEXT",
    "alerta_clima": "INTEGER NOT NULL DEFAULT 0 CHECK (alerta_clima IN (0, 1))",
    "alerta_detalle": "TEXT",
}

# Lo que se puede mostrar de una estación (consola, API, Front). La credencial
# y la clave nunca salen de aquí: solo si existen.
COLUMNAS_PUBLICAS = """id, ubicacion, bloqueada, ultima_conexion, estado_registro,
    fecha_alta, fecha_baja, clave_emitida, alerta_clima, alerta_detalle,
    credencial_hash IS NOT NULL AS tiene_credencial,
    clave_cifrado IS NOT NULL AS tiene_clave"""
COLUMNAS_SECRETAS = "credencial_hash, clave_cifrado"


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
        self._migrar()

    def _migrar(self):
        """Añade las columnas de Release 2 que falten, sin tocar los datos.

        CENTRAL y WM_Registry pueden arrancar a la vez sobre el mismo fichero:
        BEGIN IMMEDIATE hace que solo uno migre y el otro vea las columnas ya
        creadas al volver a leer la tabla.
        """
        with self._conexion() as conexion:
            conexion.execute("BEGIN IMMEDIATE")
            existentes = {fila["name"] for fila in conexion.execute("PRAGMA table_info(estaciones)")}
            for columna, definicion in COLUMNAS_R2.items():
                if columna not in existentes:
                    conexion.execute(f"ALTER TABLE estaciones ADD COLUMN {columna} {definicion}")

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
        """Alta de una estación en la red (o cambio de su ubicación). La hace el
        administrador con scripts/gestionar_bd.py, no el Monitor al conectarse."""
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
                f"SELECT {COLUMNAS_PUBLICAS} FROM estaciones WHERE id=?", (ws_id,)
            ).fetchone())

    def conectar_estacion(self, ws_id, ubicacion):
        """El Monitor de una estación dada de alta se ha autenticado: se guardan
        su ubicación y la hora. Devuelve la fila, o None si no está dada de alta
        (no existe o WM_Registry le dio de baja: CENTRAL responde KO#WS_NO_REGISTRADA)."""
        with self._conexion() as conexion:
            cursor = conexion.execute(
                """UPDATE estaciones SET ubicacion=?, ultima_conexion=?
                   WHERE id=? AND estado_registro='ALTA'""",
                (ubicacion, _ahora(), ws_id),
            )
            if cursor.rowcount == 0:
                return None
            return dict(conexion.execute(
                f"SELECT {COLUMNAS_PUBLICAS} FROM estaciones WHERE id=?", (ws_id,)
            ).fetchone())

    def listar_estaciones(self):
        """Datos públicos de las estaciones (sin credencial ni clave)."""
        with self._conexion() as conexion:
            return [dict(fila) for fila in conexion.execute(
                f"SELECT {COLUMNAS_PUBLICAS} FROM estaciones ORDER BY id"
            )]

    def obtener_estacion(self, ws_id, secretos=False):
        """Una estación o None. Con secretos=True incluye credencial_hash y
        clave_cifrado: solo para autenticar y cifrar, nunca para mostrar."""
        columnas = COLUMNAS_PUBLICAS + (", " + COLUMNAS_SECRETAS if secretos else "")
        with self._conexion() as conexion:
            fila = conexion.execute(f"SELECT {columnas} FROM estaciones WHERE id=?",
                                    (ws_id,)).fetchone()
            return dict(fila) if fila is not None else None

    # --- Registro de Release 2 (WM_Registry) --------------------------------

    def alta_registry(self, ws_id, ubicacion, credencial_hash):
        """Alta de una estación desde WM_Registry. Devuelve sus datos públicos.

        Se admite la re-alta de una estación en BAJA y la de una fila de
        Release 1 sin credencial (alta hecha con gestionar_bd.py). Si ya está en
        ALTA con credencial, lanza EstacionYaRegistrada y no cambia nada.
        """
        if not campo_valido(ws_id) or not campo_valido(ubicacion):
            raise ValueError("ID o ubicación de estación no válidos")
        if not credencial_hash:
            raise ValueError("Falta el hash de la credencial")
        instante = _ahora()
        with self._conexion() as conexion:
            # Leer y escribir en la misma transacción: dos altas simultáneas del
            # mismo ID no pueden pasar ambas la comprobación.
            conexion.execute("BEGIN IMMEDIATE")
            fila = conexion.execute(
                "SELECT estado_registro, credencial_hash FROM estaciones WHERE id=?", (ws_id,)
            ).fetchone()
            if fila is None:
                conexion.execute(
                    """INSERT INTO estaciones (id, ubicacion, ultima_conexion, estado_registro,
                                               credencial_hash, fecha_alta)
                       VALUES (?, ?, ?, 'ALTA', ?, ?)""",
                    (ws_id, ubicacion, instante, credencial_hash, instante),
                )
            elif fila["estado_registro"] == "ALTA" and fila["credencial_hash"] is not None:
                raise EstacionYaRegistrada(ws_id)
            else:
                # Se conservan bloqueo e historial; la clave anterior deja de valer.
                conexion.execute(
                    """UPDATE estaciones SET ubicacion=?, estado_registro='ALTA',
                           credencial_hash=?, fecha_alta=?, fecha_baja=NULL,
                           clave_cifrado=NULL, clave_emitida=NULL
                       WHERE id=?""",
                    (ubicacion, credencial_hash, instante, ws_id),
                )
            return dict(conexion.execute(
                f"SELECT {COLUMNAS_PUBLICAS} FROM estaciones WHERE id=?", (ws_id,)
            ).fetchone())

    def baja_registry(self, ws_id):
        """Baja lógica: la fila se conserva porque riegos la referencia.
        Borra credencial y clave. Devuelve False si no estaba en ALTA."""
        with self._conexion() as conexion:
            cursor = conexion.execute(
                """UPDATE estaciones SET estado_registro='BAJA', fecha_baja=?,
                       credencial_hash=NULL, clave_cifrado=NULL, clave_emitida=NULL
                   WHERE id=? AND estado_registro='ALTA'""",
                (_ahora(), ws_id),
            )
            return cursor.rowcount > 0

    # --- Clave simétrica de cada estación -----------------------------------

    def guardar_clave(self, ws_id, clave):
        """Guarda la clave emitida para una estación en ALTA. False si no lo está."""
        if not clave:
            raise ValueError("Clave vacía")
        with self._conexion() as conexion:
            cursor = conexion.execute(
                """UPDATE estaciones SET clave_cifrado=?, clave_emitida=?
                   WHERE id=? AND estado_registro='ALTA'""",
                (clave, _ahora(), ws_id),
            )
            return cursor.rowcount > 0

    def borrar_clave(self, ws_id):
        """Revocación: la estación queda sin clave. False si no tenía."""
        with self._conexion() as conexion:
            cursor = conexion.execute(
                """UPDATE estaciones SET clave_cifrado=NULL, clave_emitida=NULL
                   WHERE id=? AND clave_cifrado IS NOT NULL""",
                (ws_id,),
            )
            return cursor.rowcount > 0

    # --- Alerta climática ---------------------------------------------------

    def guardar_alerta_clima(self, ws_id, activa, detalle=None):
        """Persiste la alerta para que sobreviva a un reinicio de CENTRAL.
        detalle: diccionario (motivo, ciudad, temperatura...). False si la WS no existe."""
        texto = json.dumps(detalle, ensure_ascii=False) if activa and detalle else None
        with self._conexion() as conexion:
            cursor = conexion.execute(
                "UPDATE estaciones SET alerta_clima=?, alerta_detalle=? WHERE id=?",
                (1 if activa else 0, texto, ws_id),
            )
            return cursor.rowcount > 0

    # --- Auditoría ----------------------------------------------------------

    def insertar_auditoria(self, origen_tipo, accion, resultado, origen_id=None, ip=None,
                           detalle=None, fecha=None):
        """Guarda un evento y devuelve su id. El filtrado de secretos lo hace
        el módulo de auditoría antes de llamar aquí."""
        if not origen_tipo or not accion:
            raise ValueError("Faltan el tipo de origen o la acción")
        with self._conexion() as conexion:
            cursor = conexion.execute(
                """INSERT INTO auditoria (fecha, origen_tipo, origen_id, ip, accion,
                                          resultado, detalle)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (fecha or _ahora(), origen_tipo, origen_id, ip, accion, resultado,
                 json.dumps(detalle or {}, ensure_ascii=False)),
            )
            return cursor.lastrowid

    def listar_auditoria(self, desde=None, hasta=None, accion=None, origen_id=None, limite=100):
        """Eventos más recientes primero. desde/hasta: ISO 8601 en UTC."""
        condiciones, parametros = [], []
        for columna, operador, valor in (("fecha", ">=", desde), ("fecha", "<=", hasta),
                                         ("accion", "=", accion), ("origen_id", "=", origen_id)):
            if valor is not None:
                condiciones.append(f"{columna} {operador} ?")
                parametros.append(valor)
        donde = f"WHERE {' AND '.join(condiciones)}" if condiciones else ""
        with self._conexion() as conexion:
            filas = conexion.execute(
                f"SELECT * FROM auditoria {donde} ORDER BY id DESC LIMIT ?",
                (*parametros, max(1, int(limite))),
            ).fetchall()
        eventos = []
        for fila in filas:
            evento = dict(fila)
            evento["detalle"] = json.loads(evento["detalle"])
            eventos.append(evento)
        return eventos

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
