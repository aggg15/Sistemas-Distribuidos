"use strict";
// Credenciales: mismo formato de hash que common/seguridad.py (Issue #2).

const assert = require("node:assert/strict");
const { test } = require("node:test");

const { generarCredencial, hashCredencial, verificarCredencial } = require("../credenciales");
const { python } = require("./ayuda");

test("credenciales aleatorias de 256 bits, sin '#'", () => {
  const credenciales = new Set(Array.from({ length: 50 }, generarCredencial));
  assert.equal(credenciales.size, 50);
  for (const credencial of credenciales) assert.match(credencial, /^[A-Za-z0-9_-]{43}$/);
});

test("hash con sal nueva cada vez y verificación", async () => {
  const credencial = generarCredencial();
  const [a, b] = [await hashCredencial(credencial), await hashCredencial(credencial)];
  assert.notEqual(a, b);
  assert.equal(await verificarCredencial(credencial, a), true);
  assert.equal(await verificarCredencial(`${credencial}x`, a), false);
});

test("datos mal formados: false, nunca una excepción", async () => {
  const hash = await hashCredencial("secreta", 1000);
  for (const almacenado of [null, "", "a$b$c", "md5$1000$abc$def", "pbkdf2_sha256$x$abc$def",
    "pbkdf2_sha256$0$abc$def", "pbkdf2_sha256$1000$$def", hash.replace(/\$[^$]+$/, "$")]) {
    assert.equal(await verificarCredencial("secreta", almacenado), false, String(almacenado));
  }
  assert.equal(await verificarCredencial("", hash), false);
  assert.equal(await verificarCredencial(undefined, hash), false);
});

test("compatibilidad en los dos sentidos con common/seguridad.py", async () => {
  const credencial = generarCredencial();
  const desdeNode = await hashCredencial(credencial);
  assert.equal(python("from common.seguridad import verificar_credencial;"
    + "print(verificar_credencial(sys.argv[2], sys.argv[3]))", credencial, desdeNode), "True");
  const desdePython = python("from common.seguridad import hash_credencial;"
    + "print(hash_credencial(sys.argv[2]))", credencial);
  assert.equal(await verificarCredencial(credencial, desdePython), true);
  assert.equal(await verificarCredencial("otra", desdePython), false);
});
