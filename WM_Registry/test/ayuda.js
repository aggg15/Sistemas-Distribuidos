"use strict";
// Utilidades de las pruebas: BD temporal con el esquema real y servidor en un puerto libre.

const { execFileSync } = require("node:child_process");
const fs = require("node:fs");
const http = require("node:http");
const os = require("node:os");
const path = require("node:path");

const RAIZ = path.resolve(__dirname, "..", "..");
const PYTHON = process.env.PYTHON || (process.platform === "win32" ? "python" : "python3");

/**
 * Crea una BD con common/base_datos.py, el mismo código que usa WM_Central:
 * si el esquema de Python cambia, estas pruebas lo notan.
 */
function crearBdTemporal() {
  const carpeta = fs.mkdtempSync(path.join(os.tmpdir(), "wm-registry-"));
  const ruta = path.join(carpeta, "wm.db");
  execFileSync(PYTHON, ["-c",
    "import sys; sys.path.insert(0, sys.argv[1]);"
    + "from common.base_datos import BaseDatos; BaseDatos(sys.argv[2])",
    RAIZ, ruta]);
  return { ruta, borrar: () => fs.rmSync(carpeta, { recursive: true, force: true }) };
}

/** Python con la BD: devuelve lo que imprima el código (para comprobar desde el lado de CENTRAL). */
function python(codigo, ...args) {
  return execFileSync(PYTHON, ["-c",
    `import sys, json; sys.path.insert(0, sys.argv[1]); ${codigo}`, RAIZ, ...args],
  { encoding: "utf8" }).trim();
}

/** Arranca una app Express en HTTP (sin TLS) en un puerto libre. El TLS se prueba aparte. */
async function servir(app) {
  const servidor = http.createServer(app);
  await new Promise((resolver) => servidor.listen(0, "127.0.0.1", resolver));
  const base = `http://127.0.0.1:${servidor.address().port}`;
  return { base, cerrar: () => new Promise((resolver) => servidor.close(resolver)) };
}

const silencio = { info() {}, error() {} };

module.exports = { RAIZ, crearBdTemporal, python, servir, silencio };
