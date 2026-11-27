"use strict";
// Pruebas del Front (Issue #22): proxy de solo consulta hacia API_Central.

const assert = require("node:assert/strict");
const http = require("node:http");
const { afterEach, beforeEach, describe, test } = require("node:test");

const { crearApp } = require("../app");
const { leerConfiguracion } = require("../WM_Front");

const silencio = { info() {}, error() {} };

function escuchar(manejador) {
  const servidor = http.createServer(manejador);
  return new Promise((resolver) => servidor.listen(0, "127.0.0.1", () => resolver({
    url: `http://127.0.0.1:${servidor.address().port}`,
    cerrar: () => new Promise((fin) => { servidor.closeAllConnections(); servidor.close(fin); }),
  })));
}

const ESTADO = {
  estaciones: [{ ws_id: "WS-04", ubicacion: "<img src=x onerror=alert(1)>", estado: "REGANDO",
    conectada: true, riego: { riego_id: 3, origen: "FO", fo_id: "FO1", caudal_lpm: 6, volumen_l: 1.5,
      segundos: 15 }, volumen_total_l: 12.5 }],
  eventos: [{ nivel: "INFO", texto: "12:00:00 INFO Riego autorizado" }],
  kafka: true,
  broker: "kafka:29092",
};

describe("WM_Front", () => {
  let api;
  let front;
  let peticionesApi;

  beforeEach(async () => {
    peticionesApi = [];
    // API_Central simulada: responde como GET /api/estado de Release 1.
    api = await escuchar((peticion, respuesta) => {
      peticionesApi.push(`${peticion.method} ${peticion.url}`);
      if (peticion.url === "/api/estado") {
        respuesta.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
        respuesta.end(JSON.stringify(ESTADO));
      } else {
        respuesta.writeHead(404, { "Content-Type": "application/json; charset=utf-8" });
        respuesta.end(JSON.stringify({ error: "No encontrado" }));
      }
    });
    const app = crearApp({ urlApi: api.url, log: silencio });
    front = await escuchar(app);
  });

  afterEach(async () => {
    await front.cerrar();
    await api.cerrar();
  });

  test("sirve el dashboard con cabeceras de seguridad", async () => {
    const respuesta = await fetch(`${front.url}/`);
    assert.equal(respuesta.status, 200);
    assert.match(respuesta.headers.get("content-type"), /text\/html/);
    assert.match(await respuesta.text(), /Cuadro de monitorización de CENTRAL/);
    assert.match(respuesta.headers.get("content-security-policy"), /default-src 'self'/);
    assert.equal(respuesta.headers.get("x-content-type-options"), "nosniff");
    assert.equal(respuesta.headers.get("x-powered-by"), null);
    for (const recurso of ["/front.js", "/estilos.css"]) {
      assert.equal((await fetch(front.url + recurso)).status, 200, recurso);
    }
  });

  test("reenvía GET /api/... a API_Central y devuelve su respuesta tal cual", async () => {
    const respuesta = await fetch(`${front.url}/api/estado`);
    assert.equal(respuesta.status, 200);
    assert.equal(respuesta.headers.get("cache-control"), "no-store");
    assert.deepEqual(await respuesta.json(), ESTADO);
    const noExiste = await fetch(`${front.url}/api/v1/clima?ciudad=Alicante`);
    assert.equal(noExiste.status, 404, "los errores de API_Central se pasan con su código");
    assert.deepEqual(peticionesApi, ["GET /api/estado", "GET /api/v1/clima?ciudad=Alicante"]);
  });

  test("solo consulta: POST, PUT y DELETE no llegan a API_Central", async () => {
    for (const method of ["POST", "PUT", "DELETE"]) {
      const respuesta = await fetch(`${front.url}/api/orden`, { method, body: "{}",
        headers: { "Content-Type": "application/json" } });
      assert.equal(respuesta.status, 405, method);
      assert.equal(respuesta.headers.get("allow"), "GET");
    }
    assert.deepEqual(peticionesApi, []);
  });

  test("si API_Central no responde: 502 y el Front sigue vivo", async () => {
    await api.cerrar();
    const respuesta = await fetch(`${front.url}/api/estado`);
    assert.equal(respuesta.status, 502);
    assert.deepEqual(await respuesta.json(), { error: "Sin conexión con API_Central" });
    assert.equal((await fetch(`${front.url}/salud`)).status, 200);
  });

  test("las rutas fuera de /api no salen hacia API_Central", async () => {
    assert.equal((await fetch(`${front.url}/apiestado`)).status, 404);
    assert.equal((await fetch(`${front.url}/../api/estado`)).status, 200);
    assert.deepEqual(peticionesApi, ["GET /api/estado"]);
  });

  test("el dashboard escribe los datos como texto, nunca como HTML", async () => {
    const codigo = await (await fetch(`${front.url}/front.js`)).text();
    assert.doesNotMatch(codigo, /innerHTML|outerHTML|insertAdjacentHTML|document\.write/);
  });
});

describe("Configuración del Front", () => {
  test("valores por defecto y variables de entorno", () => {
    assert.deepEqual(leerConfiguracion([], {}),
      { ayuda: false, puerto: 8000, host: "0.0.0.0", api: "http://localhost:8080" });
    const config = leerConfiguracion(["--puerto", "9000"],
      { WM_API_CENTRAL_URL: "http://3.80.1.2:8080", WM_FRONT_PORT: "8001" });
    assert.equal(config.puerto, 9000, "el argumento manda sobre la variable");
    assert.equal(config.api, "http://3.80.1.2:8080");
  });

  test("rechaza puertos y URLs no válidos", () => {
    assert.throws(() => leerConfiguracion(["--puerto", "70000"], {}), /puerto/);
    assert.throws(() => leerConfiguracion(["--api", "ftp://x"], {}), /URL/);
  });
});
