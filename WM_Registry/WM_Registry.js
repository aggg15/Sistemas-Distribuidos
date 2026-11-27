"use strict";
// WM_Registry: alta y baja de las estaciones de riego por API REST sobre HTTPS.
//
//   npm start                                  (dentro de WM_Registry/)
//   node WM_Registry.js --puerto 8443 --db ../data/wm.db
//
// Variables de entorno (los argumentos tienen prioridad):
//   WM_REGISTRY_PORT  puerto HTTPS (8443)
//   WM_DB_PATH        BD SQLite compartida con WM_Central (data/wm.db)
//   WM_TLS_CERT       certificado del servidor (certs/servidor.pem)
//   WM_TLS_KEY        clave privada del servidor (certs/servidor.key)
//
// Los certificados se generan con: python scripts/generar_certificados.py IP_PUBLICA
// Solo se atiende HTTPS: el enunciado exige un canal seguro con el Monitor (p. 3).

const fs = require("node:fs");
const https = require("node:https");
const path = require("node:path");
const { parseArgs } = require("node:util");

const { crearApp } = require("./app");
const { BaseDatosRegistry, EsquemaNoPreparado } = require("./bd");

const RAIZ = path.resolve(__dirname, "..");
const ESPERA_ESQUEMA_MS = 2000;

const AYUDA = `Uso: node WM_Registry.js [opciones]

  --puerto N    puerto HTTPS (WM_REGISTRY_PORT, 8443; 0 = uno libre)
  --host IP     interfaz de escucha (WM_REGISTRY_HOST, 0.0.0.0)
  --db RUTA     BD SQLite compartida con WM_Central (WM_DB_PATH, data/wm.db)
  --cert RUTA   certificado TLS (WM_TLS_CERT, certs/servidor.pem)
  --key RUTA    clave privada TLS (WM_TLS_KEY, certs/servidor.key)
  -h, --help    esta ayuda
`;

function leerConfiguracion(argv = process.argv.slice(2), entorno = process.env) {
  const { values } = parseArgs({
    args: argv,
    options: {
      puerto: { type: "string" },
      host: { type: "string" },
      db: { type: "string" },
      cert: { type: "string" },
      key: { type: "string" },
      help: { type: "boolean", short: "h" },
    },
  });
  const puerto = Number(values.puerto ?? entorno.WM_REGISTRY_PORT ?? 8443);
  if (!Number.isInteger(puerto) || puerto < 0 || puerto > 65535) {
    throw new Error("El puerto debe ser un entero entre 0 y 65535");
  }
  return {
    ayuda: Boolean(values.help),
    puerto,
    host: values.host ?? entorno.WM_REGISTRY_HOST ?? "0.0.0.0",
    db: path.resolve(values.db ?? entorno.WM_DB_PATH ?? path.join(RAIZ, "data", "wm.db")),
    cert: path.resolve(values.cert ?? entorno.WM_TLS_CERT ?? path.join(RAIZ, "certs", "servidor.pem")),
    key: path.resolve(values.key ?? entorno.WM_TLS_KEY ?? path.join(RAIZ, "certs", "servidor.key")),
  };
}

function leerCertificados(config) {
  for (const ruta of [config.cert, config.key]) {
    if (!fs.existsSync(ruta)) {
      throw new Error(`No existe ${ruta}. Genera los certificados con:\n`
        + "  python scripts/generar_certificados.py IP_PUBLICA");
    }
  }
  return { cert: fs.readFileSync(config.cert), key: fs.readFileSync(config.key) };
}

/** Espera a que WM_Central cree o migre la BD (es quien define el esquema). */
async function abrirBaseDatos(ruta, log = console) {
  let avisado = false;
  for (;;) {
    try {
      return new BaseDatosRegistry(ruta);
    } catch (fallo) {
      if (!(fallo instanceof EsquemaNoPreparado)) throw fallo;
      if (!avisado) {
        log.info(`[Registry] ${fallo.message}: esperando a que WM_Central la prepare...`);
        avisado = true;
      }
      await new Promise((resolver) => setTimeout(resolver, ESPERA_ESQUEMA_MS));
    }
  }
}

/** Crea y arranca el servidor HTTPS. Devuelve { servidor, bd } cuando ya escucha. */
async function arrancar(config, log = console) {
  const tls = leerCertificados(config);
  const bd = await abrirBaseDatos(config.db, log);
  const servidor = https.createServer({ ...tls, minVersion: "TLSv1.2" }, crearApp({ bd, log }));
  servidor.on("close", () => bd.cerrar());
  await new Promise((resolver, rechazar) => {
    servidor.once("error", rechazar);
    servidor.listen(config.puerto, config.host, resolver);
  });
  log.info(`[Registry] WM_Registry escuchando por HTTPS en el puerto ${servidor.address().port}`);
  return { servidor, bd };
}

async function main() {
  let config;
  try {
    config = leerConfiguracion();
  } catch (fallo) {
    console.error(`${fallo.message}\n\n${AYUDA}`);
    process.exit(2);
  }
  if (config.ayuda) {
    console.log(AYUDA);
    return;
  }
  try {
    const { servidor } = await arrancar(config);
    const parar = () => {
      console.info("[Registry] Parando WM_Registry...");
      servidor.close(() => process.exit(0));
      servidor.closeAllConnections();
    };
    process.on("SIGINT", parar);
    process.on("SIGTERM", parar);
  } catch (fallo) {
    console.error(`[Registry] No se pudo arrancar: ${fallo.message}`);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

module.exports = { arrancar, leerConfiguracion };
