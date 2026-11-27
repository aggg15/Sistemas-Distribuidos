"""WM_Registry (Node.js) por HTTPS real, visto desde Python (Issue #3).

Es lo que hará el Monitor (#4): llamar al Registry con la CA del proyecto y
usar la credencial recibida para autenticarse en CENTRAL, que la verifica con
common/seguridad.py. Se omite si no hay Node.js o no se ha hecho `npm ci` en
WM_Registry/ (en GitHub Actions sí se ejecuta).
"""

import http.client
import json
from pathlib import Path
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import time
import unittest

from common.base_datos import BaseDatos

RAIZ = Path(__file__).resolve().parent.parent
REGISTRY = RAIZ / "WM_Registry"
NODE = shutil.which("node")

try:
    from common.certificados import generar_en
    from common.seguridad import contexto_tls_cliente, verificar_credencial
    HAY_CRYPTOGRAPHY = True
except ImportError:
    HAY_CRYPTOGRAPHY = False


def arrancar_registry(ruta_bd, certs, espera_s=15):
    """Lanza WM_Registry en un puerto libre y devuelve (proceso, puerto)."""
    proceso = subprocess.Popen(
        [NODE, str(REGISTRY / "WM_Registry.js"), "--puerto", "0", "--host", "127.0.0.1",
         "--db", str(ruta_bd), "--cert", str(certs["cert"]), "--key", str(certs["key"])],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8")
    puerto = []

    def leer():
        for linea in proceso.stdout:
            if "escuchando por HTTPS en el puerto" in linea and not puerto:
                puerto.append(int(linea.rsplit(" ", 1)[1]))

    threading.Thread(target=leer, daemon=True).start()
    limite = time.monotonic() + espera_s
    while not puerto and time.monotonic() < limite and proceso.poll() is None:
        time.sleep(0.05)
    if not puerto:
        proceso.kill()
        proceso.wait(5)
        proceso.stdout.close()
        raise AssertionError("WM_Registry no arrancó")
    return proceso, puerto[0]


def parar(proceso):
    proceso.terminate()
    try:
        proceso.wait(5)
    finally:
        proceso.stdout.close()


@unittest.skipUnless(NODE and (REGISTRY / "node_modules" / "express").exists(),
                     "Hace falta Node.js y `npm ci` en WM_Registry/")
@unittest.skipUnless(HAY_CRYPTOGRAPHY, "Hace falta cryptography (requirements.txt)")
class PruebasRegistryHttps(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.carpeta = Path(tempfile.mkdtemp())
        cls.certs = generar_en(cls.carpeta / "certs", ["127.0.0.1", "localhost"])
        cls.ruta_bd = cls.carpeta / "wm.db"
        cls.bd = BaseDatos(cls.ruta_bd)
        cls.proceso, cls.puerto = arrancar_registry(cls.ruta_bd, cls.certs)

    @classmethod
    def tearDownClass(cls):
        parar(cls.proceso)
        shutil.rmtree(cls.carpeta, ignore_errors=True)

    def peticion(self, metodo, ruta, cuerpo=None, credencial=None, contexto=None):
        contexto = contexto or contexto_tls_cliente(self.certs["ca"])
        conexion = http.client.HTTPSConnection("127.0.0.1", self.puerto, context=contexto,
                                               timeout=5)
        cabeceras = {"Content-Type": "application/json"}
        if credencial:
            cabeceras["Authorization"] = f"Bearer {credencial}"
        try:
            conexion.request(metodo, "/registry/v1" + ruta,
                             body=json.dumps(cuerpo) if cuerpo is not None else None,
                             headers=cabeceras)
            respuesta = conexion.getresponse()
            datos = respuesta.read()
            return respuesta.status, json.loads(datos) if datos else None
        finally:
            conexion.close()

    def test_ciclo_alta_autenticacion_baja(self):
        estado, datos = self.peticion("PUT", "/estaciones/WS-04", {"ubicacion": "River Park"})
        self.assertEqual(estado, 201)
        credencial = datos["credencial"]
        # CENTRAL (Python) verifica la credencial que generó y guardó el Registry (Node.js).
        fila = self.bd.obtener_estacion("WS-04", secretos=True)
        self.assertTrue(verificar_credencial(credencial, fila["credencial_hash"]))
        self.assertIsNotNone(self.bd.conectar_estacion("WS-04", "River Park"))
        estado, _ = self.peticion("POST", "/estaciones/WS-04/autenticacion", credencial=credencial)
        self.assertEqual(estado, 200)
        estado, _ = self.peticion("DELETE", "/estaciones/WS-04", credencial=credencial)
        self.assertEqual(estado, 204)
        # Tras la baja, CENTRAL deniega la estación (KO#WS_NO_REGISTRADA).
        self.assertIsNone(self.bd.conectar_estacion("WS-04", "River Park"))

    def test_cliente_con_otra_ca_no_conecta(self):
        otra = generar_en(self.carpeta / "otra", ["127.0.0.1"])
        with self.assertRaises(ssl.SSLCertVerificationError):
            self.peticion("GET", "/estaciones", contexto=contexto_tls_cliente(otra["ca"]))

    def test_http_sin_tls_no_es_atendido(self):
        with socket.create_connection(("127.0.0.1", self.puerto), timeout=5) as conexion:
            conexion.sendall(b"GET /registry/v1/estaciones HTTP/1.1\r\nHost: x\r\n\r\n")
            try:
                respuesta = conexion.recv(1024)
            except ConnectionResetError:
                respuesta = b""
        # El servidor TLS no entiende HTTP plano: corta o responde con una alerta TLS.
        self.assertNotIn(b"HTTP/1.1 200", respuesta)
        self.assertNotIn(b"WS-", respuesta)


@unittest.skipUnless(NODE and (REGISTRY / "node_modules" / "express").exists(),
                     "Hace falta Node.js y `npm ci` en WM_Registry/")
@unittest.skipUnless(HAY_CRYPTOGRAPHY, "Hace falta cryptography (requirements.txt)")
class PruebasArranqueRegistry(unittest.TestCase):
    def test_espera_a_que_central_cree_la_bd(self):
        carpeta = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, carpeta, True)
        certs = generar_en(carpeta / "certs", ["127.0.0.1"])
        ruta = carpeta / "wm.db"
        # Como CENTRAL arrancando unos segundos después que el Registry.
        threading.Timer(1.0, BaseDatos, args=(ruta,)).start()
        proceso, _ = arrancar_registry(ruta, certs)
        parar(proceso)


if __name__ == "__main__":
    unittest.main()
