"use strict";
// Dashboard del Front: cada 2 s pide el estado a API_Central (a través del
// servidor del Front) y lo pinta. Todo el texto se inserta con textContent,
// nunca como HTML: una ubicación o un mensaje no pueden inyectar código.
//
// Hoy usa GET /api/estado (el de Release 1). Cuando API_Central v1 (#12, #17)
// añada registro, clave y alerta climática, sus columnas aparecen solas; el
// resto del dashboard (operarios, clima, alertas de WM_W) es el Issue #23.

const INTERVALO_MS = 2000;

const COLUMNAS = [
  { titulo: "Estación", valor: (e) => e.ws_id },
  { titulo: "Ubicación", valor: (e) => e.ubicacion },
  { titulo: "Estado", celda: (e) => etiquetaEstado(e.estado) },
  // Columnas de Release 2: solo se muestran si API_Central ya envía el dato.
  { titulo: "Registro", campo: "estado_registro", valor: (e) => e.estado_registro },
  { titulo: "Clave", campo: "tiene_clave", celda: (e) => siNo(e.tiene_clave, "Vigente", "Sin clave") },
  { titulo: "Alerta clima", campo: "alerta_clima", celda: (e) => siNo(!e.alerta_clima, "No", "Activa") },
  { titulo: "Última conexión", valor: (e) => fecha(e.ultima_conexion) },
  { titulo: "Volumen total (l)", num: true, valor: (e) => numero(e.volumen_total_l, 1) },
];

function crear(etiqueta, clase, texto) {
  const elemento = document.createElement(etiqueta);
  if (clase) elemento.className = clase;
  if (texto !== undefined && texto !== null) elemento.textContent = String(texto);
  return elemento;
}

function etiquetaEstado(estado) {
  return crear("span", `estado ${estado || ""}`, (estado || "—").replace("_", " "));
}

function siNo(valor, si, no) {
  return crear("span", `etiqueta ${valor ? "si" : "no"}`, valor ? si : no);
}

function numero(valor, decimales) {
  return typeof valor === "number" ? valor.toFixed(decimales) : "—";
}

function fecha(texto) {
  if (!texto) return "—";
  const instante = new Date(texto);
  return Number.isNaN(instante.getTime()) ? texto : instante.toLocaleString("es-ES");
}

function vaciar(elemento, mensaje, columnas) {
  elemento.replaceChildren();
  const fila = crear("tr");
  const celda = crear("td", "vacio", mensaje);
  celda.colSpan = columnas;
  fila.append(celda);
  elemento.append(fila);
}

function indicador(id, ok, titulo) {
  const elemento = document.getElementById(id);
  elemento.classList.toggle("ok", ok === true);
  elemento.classList.toggle("ko", ok === false);
  elemento.title = titulo;
}

function pintarEstaciones(estaciones) {
  const visibles = COLUMNAS.filter((c) => !c.campo || estaciones.some((e) => c.campo in e));
  const cabecera = document.getElementById("cabecera-estaciones");
  cabecera.replaceChildren(...visibles.map((c) => crear("th", c.num ? "num" : "", c.titulo)));
  const cuerpo = document.getElementById("estaciones");
  if (estaciones.length === 0) {
    vaciar(cuerpo, "No hay estaciones dadas de alta", visibles.length);
    return;
  }
  cuerpo.replaceChildren(...estaciones.map((estacion) => {
    const fila = crear("tr");
    for (const columna of visibles) {
      const celda = crear("td", columna.num ? "num" : "");
      if (columna.celda) celda.append(columna.celda(estacion));
      else celda.textContent = columna.valor(estacion) ?? "—";
      fila.append(celda);
    }
    return fila;
  }));
  const cuenta = {};
  for (const e of estaciones) cuenta[e.estado] = (cuenta[e.estado] || 0) + 1;
  document.getElementById("resumen").textContent = Object.entries(cuenta)
    .map(([estado, n]) => `${n} ${estado.replace("_", " ").toLowerCase()}`).join(" · ");
}

function pintarRiegos(estaciones) {
  const cuerpo = document.getElementById("riegos");
  const enCurso = estaciones.filter((e) => e.riego);
  if (enCurso.length === 0) {
    vaciar(cuerpo, "Ningún riego en curso", 7);
    return;
  }
  cuerpo.replaceChildren(...enCurso.map(({ ws_id: wsId, riego }) => {
    const fila = crear("tr");
    fila.append(crear("td", "", wsId), crear("td", "", `#${riego.riego_id}`),
      crear("td", "", riego.origen), crear("td", "", riego.fo_id || "—"),
      crear("td", "num", numero(riego.caudal_lpm, 1)), crear("td", "num", numero(riego.volumen_l, 1)),
      crear("td", "num", riego.segundos ?? "—"));
    return fila;
  }));
}

function pintarEventos(eventos) {
  const lista = document.getElementById("eventos");
  if (eventos.length === 0) {
    lista.replaceChildren(crear("li", "vacio", "Sin mensajes"));
    return;
  }
  // Los más recientes arriba.
  lista.replaceChildren(...eventos.slice().reverse()
    .map((evento) => crear("li", evento.nivel, evento.texto)));
}

function avisar(texto) {
  const aviso = document.getElementById("aviso");
  aviso.hidden = !texto;
  aviso.textContent = texto || "";
}

async function actualizar() {
  try {
    const respuesta = await fetch("/api/estado", { cache: "no-store" });
    const datos = await respuesta.json().catch(() => ({}));
    if (!respuesta.ok) throw new Error(datos.error || `HTTP ${respuesta.status}`);
    const estaciones = Array.isArray(datos.estaciones) ? datos.estaciones : [];
    pintarEstaciones(estaciones);
    pintarRiegos(estaciones);
    pintarEventos(Array.isArray(datos.eventos) ? datos.eventos : []);
    indicador("ind-api", true, "API_Central responde");
    indicador("ind-kafka", datos.kafka === true,
      datos.kafka ? `Conectada a ${datos.broker || "Kafka"}` : "CENTRAL sin conexión con Kafka");
    avisar(datos.kafka === false ? "CENTRAL no tiene conexión con Kafka: no se pueden coordinar riegos." : "");
    document.getElementById("actualizado").textContent =
      `Actualizado ${new Date().toLocaleTimeString("es-ES")}`;
  } catch (fallo) {
    // Se conservan los últimos datos y se avisa de que pueden estar desfasados.
    indicador("ind-api", false, fallo.message);
    indicador("ind-kafka", null, "Desconocido");
    avisar(`Sin conexión con API_Central (${fallo.message}). Los datos mostrados pueden estar desactualizados.`);
  }
}

actualizar();
setInterval(actualizar, INTERVALO_MS);
