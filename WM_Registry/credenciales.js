"use strict";
// Credenciales de las estaciones (docs/release2/ARCHITECTURE.md, D3).
//
// El formato del hash es el mismo que el de common/seguridad.py:
//   pbkdf2_sha256$<iteraciones>$<sal en base64url>$<hash en base64url>
// Así WM_Registry (Node.js) guarda el hash y WM_Central (Python) puede
// verificar la credencial cuando la estación se autentica por sockets.

const crypto = require("node:crypto");
const { promisify } = require("node:util");

const pbkdf2 = promisify(crypto.pbkdf2);

const ALGORITMO = "pbkdf2_sha256";
const ITERACIONES = 200000;
const BYTES_SAL = 16;
const BYTES_HASH = 32; // SHA-256, igual que hashlib.pbkdf2_hmac por defecto
const MAX_ITERACIONES = 10000000;

/** Credencial aleatoria de 256 bits en base64url (43 caracteres, sin '#'). */
function generarCredencial() {
  return crypto.randomBytes(32).toString("base64url");
}

/** Hash con sal nueva. Es asíncrono para no bloquear el servidor mientras calcula. */
async function hashCredencial(credencial, iteraciones = ITERACIONES) {
  if (typeof credencial !== "string" || credencial === "") {
    throw new TypeError("Credencial vacía");
  }
  const sal = crypto.randomBytes(BYTES_SAL);
  const resumen = await pbkdf2(credencial, sal, iteraciones, BYTES_HASH, "sha256");
  return `${ALGORITMO}$${iteraciones}$${sal.toString("base64url")}$${resumen.toString("base64url")}`;
}

/** true si la credencial corresponde al hash guardado; false ante cualquier dato mal formado. */
async function verificarCredencial(credencial, almacenado) {
  if (typeof credencial !== "string" || credencial === "" || typeof almacenado !== "string") {
    return false;
  }
  const partes = almacenado.split("$");
  if (partes.length !== 4 || partes[0] !== ALGORITMO) {
    return false;
  }
  const iteraciones = Number(partes[1]);
  if (!Number.isInteger(iteraciones) || iteraciones < 1 || iteraciones > MAX_ITERACIONES) {
    return false;
  }
  const sal = Buffer.from(partes[2], "base64url");
  const esperado = Buffer.from(partes[3], "base64url");
  if (sal.length === 0 || esperado.length === 0) {
    return false;
  }
  const resumen = await pbkdf2(credencial, sal, iteraciones, esperado.length, "sha256");
  // Comparación en tiempo constante: no revela cuántos bytes coinciden.
  return crypto.timingSafeEqual(resumen, esperado);
}

module.exports = { ALGORITMO, ITERACIONES, generarCredencial, hashCredencial, verificarCredencial };
