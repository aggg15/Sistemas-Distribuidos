"""Esquema de Release 2: migración desde Release 1, registro, claves, alertas y auditoría (#1)."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest

from common.base_datos import COLUMNAS_R2, BaseDatos, EstacionYaRegistrada

# Esquema exacto de Release 1 (commit 07a4dbd), congelado para probar la migración.
ESQUEMA_R1 = """
CREATE TABLE operarios (id TEXT PRIMARY KEY NOT NULL, nombre TEXT NOT NULL);
CREATE TABLE estaciones (
    id TEXT PRIMARY KEY NOT NULL,
    ubicacion TEXT NOT NULL,
    bloqueada INTEGER NOT NULL DEFAULT 0 CHECK (bloqueada IN (0, 1)),
    ultima_conexion TEXT NOT NULL
);
CREATE TABLE riegos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ws_id TEXT NOT NULL REFERENCES estaciones(id),
    fo_id TEXT REFERENCES operarios(id),
    origen TEXT NOT NULL CHECK (origen IN ('FO', 'WS', 'CENTRAL')),
    inicio TEXT, fin TEXT,
    duracion_s INTEGER CHECK (duracion_s >= 0),
    volumen_l REAL CHECK (volumen_l >= 0),
    motivo_fin TEXT CHECK (motivo_fin IN ('TIEMPO', 'MANUAL', 'FUGA', 'BLOQUEO', 'DESCONEXION')),
    CHECK (origen != 'FO' OR fo_id IS NOT NULL)
);
INSERT INTO operarios VALUES ('FO1', 'Ana');
INSERT INTO estaciones VALUES ('WS-04', 'River Park', 1, '2026-10-29T10:00:00+00:00');
INSERT INTO riegos (ws_id, fo_id, origen, inicio, fin, duracion_s, volumen_l, motivo_fin)
    VALUES ('WS-04', 'FO1', 'FO', '2026-10-29T10:01:00+00:00',
            '2026-10-29T10:01:20+00:00', 20, 5.0, 'TIEMPO');
"""

SECRETOS = ("credencial_hash", "clave_cifrado")


class Temporal(unittest.TestCase):
    def setUp(self):
        temporal = tempfile.TemporaryDirectory()
        self.addCleanup(temporal.cleanup)
        self.ruta = Path(temporal.name) / "wm.db"


class PruebasMigracion(Temporal):
    def crear_bd_r1(self):
        with closing(sqlite3.connect(self.ruta)) as conexion:
            conexion.executescript(ESQUEMA_R1)

    def columnas(self):
        with closing(sqlite3.connect(self.ruta)) as conexion:
            return {fila[1] for fila in conexion.execute("PRAGMA table_info(estaciones)")}

    def test_bd_de_release1_se_migra_sin_perder_datos(self):
        self.crear_bd_r1()
        bd = BaseDatos(self.ruta)
        self.assertTrue(set(COLUMNAS_R2) <= self.columnas())
        estacion = bd.listar_estaciones()[0]
        self.assertEqual((estacion["id"], estacion["ubicacion"], estacion["bloqueada"]),
                         ("WS-04", "River Park", 1))
        self.assertEqual(estacion["ultima_conexion"], "2026-10-29T10:00:00+00:00")
        # Las filas de R1 quedan en ALTA pero sin credencial: deben pasar por el Registry.
        self.assertEqual(estacion["estado_registro"], "ALTA")
        self.assertEqual((estacion["tiene_credencial"], estacion["tiene_clave"]), (0, 0))
        self.assertEqual(estacion["alerta_clima"], 0)
        self.assertEqual(bd.listar_operarios(), [{"id": "FO1", "nombre": "Ana"}])
        self.assertEqual(bd.listar_riegos()[0]["volumen_l"], 5.0)
        self.assertEqual(bd.volumen_por_estacion(), {"WS-04": 5.0})

    def test_abrir_varias_veces_no_duplica_columnas(self):
        self.crear_bd_r1()
        for _ in range(3):
            BaseDatos(self.ruta)
        self.assertEqual(len(self.columnas()), 4 + len(COLUMNAS_R2))

    def test_bd_nueva_y_bd_migrada_tienen_las_mismas_columnas(self):
        BaseDatos(self.ruta)
        nueva = self.columnas()
        self.ruta.unlink()
        self.crear_bd_r1()
        BaseDatos(self.ruta)
        self.assertEqual(self.columnas(), nueva)

    def test_central_y_registry_arrancando_a_la_vez(self):
        self.crear_bd_r1()
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: BaseDatos(self.ruta), range(8)))
        self.assertEqual(len(self.columnas()), 4 + len(COLUMNAS_R2))

    def test_arranque_espera_si_otro_proceso_bloquea_la_bd(self):
        # Otro proceso (CENTRAL o Registry) tiene la BD bloqueada más tiempo que
        # el timeout de una conexión (1 s): el arranque reintenta en lugar de fallar.
        self.crear_bd_r1()
        bloqueo = sqlite3.connect(self.ruta, isolation_level=None, check_same_thread=False)
        bloqueo.execute("BEGIN EXCLUSIVE")
        liberar = threading.Timer(1.5, lambda: (bloqueo.execute("COMMIT"), bloqueo.close()))
        liberar.start()
        try:
            BaseDatos(self.ruta)
        finally:
            liberar.join()
        self.assertEqual(len(self.columnas()), 4 + len(COLUMNAS_R2))

    def test_tabla_de_auditoria_creada(self):
        self.crear_bd_r1()
        BaseDatos(self.ruta)
        with closing(sqlite3.connect(self.ruta)) as conexion:
            tablas = {fila[0] for fila in conexion.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertIn("auditoria", tablas)


class PruebasRegistry(Temporal):
    def setUp(self):
        super().setUp()
        self.bd = BaseDatos(self.ruta)

    def test_alta_nueva(self):
        fila = self.bd.alta_registry("WS-01", "Alicante", "hash-1")
        self.assertEqual((fila["estado_registro"], fila["tiene_credencial"]), ("ALTA", 1))
        self.assertIsNotNone(fila["fecha_alta"])
        self.assertTrue(set(SECRETOS).isdisjoint(fila))
        self.assertEqual(self.bd.obtener_estacion("WS-01", secretos=True)["credencial_hash"], "hash-1")

    def test_alta_repetida_se_rechaza_sin_cambiar_nada(self):
        self.bd.alta_registry("WS-01", "Alicante", "hash-1")
        with self.assertRaises(EstacionYaRegistrada):
            self.bd.alta_registry("WS-01", "Elche", "hash-2")
        fila = self.bd.obtener_estacion("WS-01", secretos=True)
        self.assertEqual((fila["ubicacion"], fila["credencial_hash"]), ("Alicante", "hash-1"))

    def test_alta_de_una_fila_de_release1_sin_credencial(self):
        self.bd.registrar_estacion("WS-04", "River Park")
        fila = self.bd.alta_registry("WS-04", "Alicante", "hash-1")
        self.assertEqual((fila["ubicacion"], fila["tiene_credencial"]), ("Alicante", 1))
        self.assertEqual(len(self.bd.listar_estaciones()), 1)

    def test_baja_logica_conserva_historial_y_borra_secretos(self):
        self.bd.alta_registry("WS-01", "Alicante", "hash-1")
        self.bd.guardar_clave("WS-01", "clave-1")
        riego = self.bd.crear_riego("WS-01", None, "WS")
        self.bd.cerrar_riego(riego, 10, 2.5, "TIEMPO")
        self.assertTrue(self.bd.baja_registry("WS-01"))
        fila = self.bd.obtener_estacion("WS-01", secretos=True)
        self.assertEqual(fila["estado_registro"], "BAJA")
        self.assertIsNotNone(fila["fecha_baja"])
        self.assertIsNone(fila["credencial_hash"])
        self.assertIsNone(fila["clave_cifrado"])
        self.assertEqual(len(self.bd.listar_riegos()), 1)
        # Una segunda baja no hace nada.
        self.assertFalse(self.bd.baja_registry("WS-01"))
        self.assertFalse(self.bd.baja_registry("WS-99"))

    def test_re_alta_tras_baja_conserva_bloqueo_e_historial(self):
        self.bd.alta_registry("WS-01", "Alicante", "hash-1")
        self.bd.guardar_bloqueo("WS-01", True)
        self.bd.crear_riego("WS-01", None, "WS")
        self.bd.baja_registry("WS-01")
        fila = self.bd.alta_registry("WS-01", "Elche", "hash-2")
        self.assertEqual((fila["estado_registro"], fila["ubicacion"], fila["bloqueada"]),
                         ("ALTA", "Elche", 1))
        self.assertIsNone(fila["fecha_baja"])
        self.assertEqual(len(self.bd.listar_riegos()), 1)

    def test_altas_simultaneas_del_mismo_id_solo_una_gana(self):
        def alta(n):
            try:
                self.bd.alta_registry("WS-01", "Alicante", f"hash-{n}")
                return True
            except EstacionYaRegistrada:
                return False
        with ThreadPoolExecutor(max_workers=6) as pool:
            resultados = list(pool.map(alta, range(12)))
        self.assertEqual(resultados.count(True), 1)

    def test_datos_invalidos(self):
        for ws_id, ubicacion, hash_ in (("", "A", "h"), ("WS#1", "A", "h"), ("WS-1", " A", "h"),
                                        ("WS-1", "A", "")):
            with self.subTest(ws_id=ws_id), self.assertRaises(ValueError):
                self.bd.alta_registry(ws_id, ubicacion, hash_)
        self.assertEqual(self.bd.listar_estaciones(), [])


class PruebasClavesYAlertas(Temporal):
    def setUp(self):
        super().setUp()
        self.bd = BaseDatos(self.ruta)
        self.bd.alta_registry("WS-01", "Alicante", "hash-1")

    def test_guardar_y_borrar_clave(self):
        self.assertTrue(self.bd.guardar_clave("WS-01", "clave-1"))
        fila = self.bd.obtener_estacion("WS-01", secretos=True)
        self.assertEqual(fila["clave_cifrado"], "clave-1")
        self.assertEqual(fila["tiene_clave"], 1)
        self.assertIsNotNone(fila["clave_emitida"])
        self.assertTrue(self.bd.borrar_clave("WS-01"))
        self.assertFalse(self.bd.borrar_clave("WS-01"))
        fila = self.bd.obtener_estacion("WS-01", secretos=True)
        self.assertIsNone(fila["clave_cifrado"])
        self.assertIsNone(fila["clave_emitida"])

    def test_no_se_emite_clave_a_estacion_de_baja_o_inexistente(self):
        self.bd.baja_registry("WS-01")
        self.assertFalse(self.bd.guardar_clave("WS-01", "clave-1"))
        self.assertFalse(self.bd.guardar_clave("WS-99", "clave-1"))
        with self.assertRaises(ValueError):
            self.bd.guardar_clave("WS-01", "")

    def test_ningun_listado_publico_contiene_secretos(self):
        self.bd.guardar_clave("WS-01", "clave-secreta")
        self.bd.registrar_estacion("WS-02", "Parque")
        publicos = [*self.bd.listar_estaciones(), self.bd.obtener_estacion("WS-01"),
                    self.bd.conectar_estacion("WS-01", "Alicante"),
                    self.bd.registrar_estacion("WS-02", "Parque")]
        for fila in publicos:
            self.assertTrue(set(SECRETOS).isdisjoint(fila))
            self.assertNotIn("clave-secreta", str(fila))
            self.assertNotIn("hash-1", str(fila))

    def test_alerta_climatica_persistente(self):
        detalle = {"motivo": "HELADA", "ciudad": "Teruel", "temperatura_c": -3.5}
        self.assertTrue(self.bd.guardar_alerta_clima("WS-01", True, detalle))
        fila = BaseDatos(self.ruta).obtener_estacion("WS-01")
        self.assertEqual(fila["alerta_clima"], 1)
        self.assertEqual(fila["alerta_detalle"],
                         '{"motivo": "HELADA", "ciudad": "Teruel", "temperatura_c": -3.5}')
        self.assertTrue(self.bd.guardar_alerta_clima("WS-01", False))
        fila = self.bd.obtener_estacion("WS-01")
        self.assertEqual((fila["alerta_clima"], fila["alerta_detalle"]), (0, None))
        self.assertFalse(self.bd.guardar_alerta_clima("WS-99", True))


class PruebasAuditoria(Temporal):
    def setUp(self):
        super().setUp()
        self.bd = BaseDatos(self.ruta)

    def test_insertar_y_consultar(self):
        id_ = self.bd.insertar_auditoria("WS", "AUTH_KO", "KO", origen_id="WS-99",
                                         ip="10.0.0.5", detalle={"motivo": "WS_NO_REGISTRADA"})
        evento = self.bd.listar_auditoria()[0]
        self.assertEqual(evento["id"], id_)
        self.assertEqual((evento["origen_tipo"], evento["origen_id"], evento["ip"],
                          evento["accion"], evento["resultado"]),
                         ("WS", "WS-99", "10.0.0.5", "AUTH_KO", "KO"))
        self.assertEqual(evento["detalle"], {"motivo": "WS_NO_REGISTRADA"})
        self.assertRegex(evento["fecha"], r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\+00:00$")

    def test_filtros_orden_y_limite(self):
        fechas = ["2026-11-01T10:00:00+00:00", "2026-11-02T10:00:00+00:00",
                  "2026-11-03T10:00:00+00:00"]
        for n, fecha in enumerate(fechas):
            self.bd.insertar_auditoria("WS", "AUTH_OK" if n else "FUGA", "OK",
                                       origen_id=f"WS-0{n}", fecha=fecha)
        self.assertEqual([e["origen_id"] for e in self.bd.listar_auditoria()],
                         ["WS-02", "WS-01", "WS-00"])
        self.assertEqual(len(self.bd.listar_auditoria(accion="AUTH_OK")), 2)
        self.assertEqual(len(self.bd.listar_auditoria(desde=fechas[1])), 2)
        self.assertEqual(len(self.bd.listar_auditoria(hasta=fechas[0])), 1)
        self.assertEqual(self.bd.listar_auditoria(origen_id="WS-01")[0]["accion"], "AUTH_OK")
        self.assertEqual(len(self.bd.listar_auditoria(limite=1)), 1)

    def test_resultado_y_campos_obligatorios(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.bd.insertar_auditoria("WS", "AUTH_OK", "QUIZA")
        with self.assertRaises(ValueError):
            self.bd.insertar_auditoria("", "AUTH_OK", "OK")
        self.assertEqual(self.bd.listar_auditoria(), [])


class PruebasScriptAdmin(Temporal):
    def ejecutar(self, *args):
        script = Path(__file__).resolve().parent.parent / "scripts" / "gestionar_bd.py"
        return subprocess.run([sys.executable, str(script), "--db", str(self.ruta), *args],
                               capture_output=True, text=True, timeout=5)

    def test_listados_sin_secretos(self):
        bd = BaseDatos(self.ruta)
        bd.alta_registry("WS-01", "Alicante", "hash-secreto")
        bd.guardar_clave("WS-01", "clave-secreta")
        bd.insertar_auditoria("REGISTRY", "REGISTRY_ALTA", "OK", origen_id="WS-01")
        salida = self.ejecutar("estaciones")
        self.assertEqual(salida.returncode, 0)
        self.assertIn('"tiene_clave": 1', salida.stdout)
        self.assertNotIn("secreto", salida.stdout)
        self.assertNotIn("secreta", salida.stdout)
        salida = self.ejecutar("auditoria", "--limite", "5")
        self.assertIn('"accion": "REGISTRY_ALTA"', salida.stdout)
        # Fechas en hora de Madrid, como el resto de listados.
        self.assertRegex(salida.stdout, r'"fecha": "\d{4}-\d\d-\d\dT[\d:]+\+0[12]:00"')


if __name__ == "__main__":
    unittest.main()
