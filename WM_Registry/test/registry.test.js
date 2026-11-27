"use strict";
// Pruebas de la API REST de WM_Registry (Issue #3). Equivalen a las llamadas de
// Postman del tutorial, pero automáticas: node --test (npm test).

const assert = require("node:assert/strict");
const { after, afterEach, before, beforeEach, describe, test } = require("node:test");
const { DatabaseSync } = require("node:sqlite");

const { crearApp, campoValido } = require("../app");
const { BaseDatosRegistry, EsquemaNoPreparado } = require("../bd");
const { verificarCredencial } = require("../credenciales");
const { crearBdTemporal, python, servir, silencio } = require("./ayuda");

const API = "/registry/v1/estaciones";

describe("API REST de WM_Registry", () => {
  let bdTemporal;
  let bd;
  let servidor;

  async function peticion(metodo, ruta, { cuerpo, credencial, cabeceras = {} } = {}) {
    const opciones = { method: metodo, headers: { ...cabeceras } };
    if (cuerpo !== undefined) {
      opciones.body = typeof cuerpo === "string" ? cuerpo : JSON.stringify(cuerpo);
      opciones.headers["Content-Type"] = "application/json";
    }
    if (credencial) opciones.headers.Authorization = `Bearer ${credencial}`;
    const respuesta = await fetch(servidor.base + ruta, opciones);
    const texto = await respuesta.text();
    return { estado: respuesta.status, cabeceras: respuesta.headers,
      datos: texto ? JSON.parse(texto) : null };
  }

  const alta = (wsId, ubicacion = "Alicante") => peticion("PUT", `${API}/${wsId}`,
    { cuerpo: { ubicacion } });

  beforeEach(async () => {
    bdTemporal = crearBdTemporal();
    bd = new BaseDatosRegistry(bdTemporal.ruta);
    servidor = await servir(crearApp({ bd, log: silencio }));
  });

  afterEach(async () => {
    await servidor.cerrar();
    bd.cerrar();
    bdTemporal.borrar();
  });

  test("página de inicio, como el GET / del tutorial", async () => {
    const { estado, datos } = await peticion("GET", "/");
    assert.equal(estado, 200);
    assert.equal(datos.servicio, "WM_Registry");
  });

  test("alta: 201 con la credencial, solo esta vez", async () => {
    const { estado, datos, cabeceras } = await alta("ws-04", "River Park");
    assert.equal(estado, 201);
    assert.equal(datos.ws_id, "WS-04", "el ID se guarda en mayúsculas, como en Release 1");
    assert.equal(datos.ubicacion, "River Park");
    assert.equal(datos.estado_registro, "ALTA");
    assert.match(datos.credencial, /^[A-Za-z0-9_-]{43}$/);
    assert.equal(cabeceras.get("location"), `${API}/WS-04`);
    assert.equal(cabeceras.get("cache-control"), "no-store");
    // Ni la consulta ni el listado vuelven a dar la credencial.
    const consulta = await peticion("GET", `${API}/WS-04`, { credencial: datos.credencial });
    assert.equal(consulta.estado, 200);
    assert.equal(consulta.datos.credencial, undefined);
    const listado = await peticion("GET", API);
    assert.deepEqual(Object.keys(listado.datos[0]).sort(),
      ["estado_registro", "fecha_alta", "fecha_baja", "ubicacion", "ws_id"]);
  });

  test("en la BD solo queda el hash, y CENTRAL (Python) lo verifica", async () => {
    const { datos } = await alta("WS-04");
    const fila = new DatabaseSync(bdTemporal.ruta).prepare(
      "SELECT credencial_hash FROM estaciones WHERE id = 'WS-04'").get();
    assert.doesNotMatch(fila.credencial_hash, new RegExp(datos.credencial));
    assert.match(fila.credencial_hash, /^pbkdf2_sha256\$200000\$/);
    assert.equal(await verificarCredencial(datos.credencial, fila.credencial_hash), true);
    const verificada = python(
      "from common.seguridad import verificar_credencial;"
      + "print(verificar_credencial(sys.argv[2], sys.argv[3]))",
      datos.credencial, fila.credencial_hash);
    assert.equal(verificada, "True");
  });

  test("listado vacío: lista vacía con 200 (el tutorial respondía «No hay resultados»)", async () => {
    const { estado, datos } = await peticion("GET", API);
    assert.equal(estado, 200);
    assert.deepEqual(datos, []);
  });

  test("consulta, baja y autenticación exigen la credencial", async () => {
    const { datos } = await alta("WS-04");
    for (const [metodo, ruta] of [["GET", `${API}/WS-04`], ["DELETE", `${API}/WS-04`],
      ["POST", `${API}/WS-04/autenticacion`]]) {
      const sin = await peticion(metodo, ruta);
      assert.equal(sin.estado, 401, `${metodo} sin credencial`);
      assert.equal(sin.cabeceras.get("www-authenticate"), 'Bearer realm="WM_Registry"');
      const mala = await peticion(metodo, ruta, { credencial: "x".repeat(43) });
      assert.equal(mala.estado, 401, `${metodo} con otra credencial`);
    }
    const ok = await peticion("POST", `${API}/WS-04/autenticacion`, { credencial: datos.credencial });
    assert.equal(ok.estado, 200);
    assert.deepEqual(ok.datos, { ws_id: "WS-04", autenticada: true });
  });

  test("una credencial no sirve para otra estación", async () => {
    const ws04 = await alta("WS-04");
    await alta("WS-05");
    const { estado } = await peticion("GET", `${API}/WS-05`, { credencial: ws04.datos.credencial });
    assert.equal(estado, 401);
  });

  test("segunda alta del mismo ID: 409; con su credencial cambia la ubicación", async () => {
    const { datos } = await alta("WS-04", "River Park");
    assert.equal((await alta("WS-04", "Otro sitio")).estado, 409);
    const cambio = await peticion("PUT", `${API}/WS-04`,
      { cuerpo: { ubicacion: "Central Park" }, credencial: datos.credencial });
    assert.equal(cambio.estado, 200);
    assert.equal(cambio.datos.ubicacion, "Central Park");
    assert.equal(cambio.datos.credencial, undefined, "cambiar la ubicación no da otra credencial");
    const mala = await peticion("PUT", `${API}/WS-04`,
      { cuerpo: { ubicacion: "Otro sitio" }, credencial: "x".repeat(43) });
    assert.equal(mala.estado, 401);
  });

  test("baja lógica: 204, después 404 y re-alta con credencial nueva", async () => {
    const primera = await alta("WS-04");
    const baja = await peticion("DELETE", `${API}/WS-04`, { credencial: primera.datos.credencial });
    assert.equal(baja.estado, 204);
    const consulta = await peticion("GET", `${API}/WS-04`, { credencial: primera.datos.credencial });
    assert.equal(consulta.estado, 404);
    const [fila] = (await peticion("GET", API)).datos;
    assert.equal(fila.estado_registro, "BAJA");
    assert.ok(fila.fecha_baja);
    const segunda = await alta("WS-04");
    assert.equal(segunda.estado, 201);
    assert.notEqual(segunda.datos.credencial, primera.datos.credencial);
    const vieja = await peticion("POST", `${API}/WS-04/autenticacion`,
      { credencial: primera.datos.credencial });
    assert.equal(vieja.estado, 401, "la credencial anterior a la baja ya no vale");
  });

  test("estación de Release 1 sin credencial: el Registry le da la primera", async () => {
    python("from common.base_datos import BaseDatos; BaseDatos(sys.argv[2])"
      + ".registrar_estacion('WS-07', 'Riverside Gardens')", bdTemporal.ruta);
    const { estado, datos } = await alta("WS-07", "Riverside Gardens");
    assert.equal(estado, 201);
    assert.ok(datos.credencial);
  });

  test("tras la baja, CENTRAL ya no acepta la estación (conectar_estacion)", async () => {
    const { datos } = await alta("WS-04");
    const conectar = "from common.base_datos import BaseDatos;"
      + "print(BaseDatos(sys.argv[2]).conectar_estacion('WS-04', 'Alicante') is not None)";
    assert.equal(python(conectar, bdTemporal.ruta), "True");
    await peticion("DELETE", `${API}/WS-04`, { credencial: datos.credencial });
    assert.equal(python(conectar, bdTemporal.ruta), "False");
  });

  test("validación: ID con '#', ubicación ausente, JSON roto y cuerpo enorme", async () => {
    assert.equal((await alta("WS%2304")).estado, 400, "ID con #");
    assert.equal((await alta(" WS-04")).estado, 400, "ID con espacio inicial");
    assert.equal((await alta("W".repeat(33))).estado, 400, "ID demasiado largo");
    assert.equal((await alta("WS-04", "")).estado, 400, "ubicación vacía");
    assert.equal((await alta("WS-04", "Calle #3")).estado, 400, "ubicación con #");
    assert.equal((await peticion("PUT", `${API}/WS-04`)).estado, 400, "sin cuerpo");
    const roto = await peticion("PUT", `${API}/WS-04`, { cuerpo: "{ubicacion:" });
    assert.equal(roto.estado, 400);
    assert.equal(roto.datos.error, "JSON no válido");
    const enorme = await peticion("PUT", `${API}/WS-04`, { cuerpo: { ubicacion: "x".repeat(5000) } });
    assert.equal(enorme.estado, 413);
    assert.deepEqual((await peticion("GET", API)).datos, [], "nada se ha guardado");
  });

  test("ruta desconocida: 404 en JSON", async () => {
    const { estado, datos } = await peticion("GET", "/usuarios");
    assert.equal(estado, 404);
    assert.deepEqual(datos, { error: "No encontrado" });
  });

  test("altas simultáneas del mismo ID: solo una recibe credencial", async () => {
    const respuestas = await Promise.all(Array.from({ length: 10 }, () => alta("WS-04")));
    const estados = respuestas.map((r) => r.estado).sort();
    assert.deepEqual(estados, [201, ...Array(9).fill(409)]);
  });

  test("auditoría: alta, baja y fallos con IP y sin la credencial", async () => {
    const { datos } = await alta("WS-04");
    await peticion("GET", `${API}/WS-04`, { credencial: "x".repeat(43) });
    await alta("WS-04");
    await peticion("DELETE", `${API}/WS-04`, { credencial: datos.credencial });
    const filas = new DatabaseSync(bdTemporal.ruta).prepare(
      "SELECT accion, resultado, origen_tipo, origen_id, ip, detalle FROM auditoria ORDER BY id").all();
    assert.deepEqual(filas.map((f) => `${f.accion}:${f.resultado}`),
      ["REGISTRY_ALTA:OK", "AUTH_KO:KO", "REGISTRY_KO:KO", "REGISTRY_BAJA:OK"]);
    for (const fila of filas) {
      assert.equal(fila.origen_tipo, "WS");
      assert.equal(fila.origen_id, "WS-04");
      assert.equal(fila.ip, "127.0.0.1");
      assert.ok(!fila.detalle.includes(datos.credencial));
    }
    // La auditoría también se lee desde Python (scripts/gestionar_bd.py auditoria).
    const desdePython = python("from common.base_datos import BaseDatos;"
      + "print(len(BaseDatos(sys.argv[2]).listar_auditoria()))", bdTemporal.ruta);
    assert.equal(desdePython, "4");
  });
});

describe("Piezas sueltas", () => {
  let bdTemporal;
  before(() => { bdTemporal = crearBdTemporal(); });
  after(() => bdTemporal.borrar());

  test("campoValido sigue el criterio de campo_valido (Python)", () => {
    for (const valido of ["WS-04", "River Park", "Plaza de España"]) {
      assert.equal(campoValido(valido, 100), true, valido);
    }
    for (const invalido of ["", " WS", "WS ", "A#B", "a\tb", "a\nb", "a​b", 4, null]) {
      assert.equal(campoValido(invalido, 100), false, JSON.stringify(invalido));
    }
  });

  test("sin esquema de Release 2, el Registry no arranca y espera a CENTRAL", () => {
    assert.throws(() => new BaseDatosRegistry(`${bdTemporal.ruta}.no-existe`), EsquemaNoPreparado);
    const vacia = `${bdTemporal.ruta}.vacia`;
    new DatabaseSync(vacia).close();
    assert.throws(() => new BaseDatosRegistry(vacia), EsquemaNoPreparado);
  });
});
