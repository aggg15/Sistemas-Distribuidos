"use strict";
// Front de Water Management con Express (tutorial «Servicios web – API REST con
// Node.js»): sirve el dashboard y reenvía sus consultas a API_Central.
//
//   GET  /            dashboard (public/index.html)
//   GET  /api/...     se reenvía tal cual a API_Central (WM_API_CENTRAL_URL)
//   GET  /salud       estado del propio Front
//
// El navegador solo habla con el Front, así que no hay CORS ni certificados
// que aceptar. Es de SOLO CONSULTA: el Front es público (p. 5 del enunciado) y
// las órdenes (bloquear, activar, restaurar claves) se dan desde WM_Central.
//
// El tutorial usa el paquete `request` para llamar a otras APIs; está retirado
// desde 2020 y Node.js trae fetch, que es lo que se usa aquí.

const path = require("node:path");
const express = require("express");

const ESPERA_API_MS = 5000;

function crearApp({ urlApi, fetchImpl = fetch, log = console }) {
  const base = new URL(urlApi);
  const app = express();
  app.disable("x-powered-by");
  app.use((peticion, respuesta, siguiente) => {
    respuesta.set({
      "X-Content-Type-Options": "nosniff",
      "Referrer-Policy": "no-referrer",
      // Solo recursos propios: ningún script de terceros puede ejecutarse en el panel.
      "Content-Security-Policy": "default-src 'self'; style-src 'self'; script-src 'self'; "
        + "img-src 'self' data:; frame-ancestors 'none'",
    });
    siguiente();
  });

  app.get("/salud", (peticion, respuesta) => {
    respuesta.json({ servicio: "WM_Front", api_central: base.origin });
  });

  // Solo GET: un Front público no puede dar órdenes a la red.
  app.all(/^\/api(\/.*)?$/, async (peticion, respuesta) => {
    if (peticion.method !== "GET") {
      respuesta.set("Allow", "GET");
      return respuesta.status(405).json({
        error: "El Front es de solo consulta: las órdenes se dan desde WM_Central",
      });
    }
    // originalUrl empieza por /api, así que el destino siempre es API_Central.
    const destino = new URL(peticion.originalUrl, base);
    try {
      const respuestaApi = await fetchImpl(destino, {
        headers: { Accept: "application/json" },
        signal: AbortSignal.timeout(ESPERA_API_MS),
      });
      const cuerpo = Buffer.from(await respuestaApi.arrayBuffer());
      respuesta.status(respuestaApi.status)
        .type(respuestaApi.headers.get("content-type") || "application/json")
        .set("Cache-Control", "no-store")
        .send(cuerpo);
    } catch (fallo) {
      log.error(`[Front] Sin respuesta de API_Central (${destino.pathname}): ${fallo.message}`);
      respuesta.status(502).set("Cache-Control", "no-store")
        .json({ error: "Sin conexión con API_Central" });
    }
  });

  app.use(express.static(path.join(__dirname, "public"), { index: "index.html" }));

  app.use((peticion, respuesta) => respuesta.status(404).json({ error: "No encontrado" }));

  return app;
}

module.exports = { crearApp };
