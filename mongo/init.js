// GameInsight — Inicialización de MongoDB
// Se ejecuta automáticamente cuando el contenedor inicia por primera vez.
// Crea la base de datos, colecciones con validación, índices y documentos de ejemplo.

db = db.getSiblingDB("gamestore_db");

// ── COLECCIÓN: juegos ─────────────────────────────────────────────────────────
db.createCollection("juegos", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["juego_id", "titulo", "categoria", "precio_base"],
      properties: {
        juego_id:        { bsonType: "string",  description: "ID único del juego" },
        titulo:          { bsonType: "string",  description: "Nombre del juego" },
        categoria:       { bsonType: "string",  description: "Género del juego" },
        plataformas:     { bsonType: "array",   description: "Plataformas disponibles" },
        precio_base:     { bsonType: "double",  description: "Precio en soles" },
        pegi:            { bsonType: "int",     description: "Clasificación por edad" }
      }
    }
  }
});

db.juegos.createIndex({ juego_id: 1 }, { unique: true });
db.juegos.createIndex({ categoria: 1 });
db.juegos.createIndex({ titulo: "text" });

// ── COLECCIÓN: ventas_procesadas ──────────────────────────────────────────────
db.createCollection("ventas_procesadas");
db.ventas_procesadas.createIndex({ venta_id: 1 }, { unique: true });
db.ventas_procesadas.createIndex({ fecha: 1 });
db.ventas_procesadas.createIndex({ cliente_id: 1 });
db.ventas_procesadas.createIndex({ juego_id: 1 });
db.ventas_procesadas.createIndex({ plataforma: 1 });
db.ventas_procesadas.createIndex({ mes: 1, anio: 1 });

// ── COLECCIÓN: analisis_juegos ────────────────────────────────────────────────
db.createCollection("analisis_juegos");
db.analisis_juegos.createIndex({ total_unidades: -1 });

// ── COLECCIÓN: analisis_categorias ───────────────────────────────────────────
db.createCollection("analisis_categorias");
db.analisis_categorias.createIndex({ ingresos_total: -1 });

// ── COLECCIÓN: analisis_plataformas ──────────────────────────────────────────
db.createCollection("analisis_plataformas");
db.analisis_plataformas.createIndex({ ingresos_total: -1 });

// ── COLECCIÓN: reportes_mensuales ─────────────────────────────────────────────
db.createCollection("reportes_mensuales");
db.reportes_mensuales.createIndex({ anio: 1, mes: 1 });

// ── COLECCIÓN: perfil_clientes ────────────────────────────────────────────────
db.createCollection("perfil_clientes");
db.perfil_clientes.createIndex({ gasto_total: -1 });

// ── COLECCIÓN: alertas_inventario ─────────────────────────────────────────────
db.createCollection("alertas_inventario");
db.alertas_inventario.createIndex({ stock: 1 });

// ── DOCUMENTOS DE EJEMPLO ─────────────────────────────────────────────────────
// Estos son ejemplos del modelo documental; los datos reales los inserta el ETL de Spark.

db.juegos.insertMany([
  {
    juego_id: "JV001",
    titulo: "God of War Ragnarök",
    categoria: "Acción",
    plataformas: ["PS5", "PS4"],
    precio_base: 299.90,
    desarrollador: "Santa Monica Studio",
    publicador: "Sony Interactive Entertainment",
    anio_lanzamiento: 2022,
    pegi: 18,
    descripcion: "Continúa la épica historia de Kratos y Atreus a través de los Nueve Reinos.",
    tags: ["mitología nórdica", "combate", "historia épica"],
    metricas: {
      total_ventas_unidades: 0,
      ingresos_acumulados: 0.0,
      puntuacion_promedio: 4.7,
      total_resenas: 3
    }
  },
  {
    juego_id: "JV005",
    titulo: "Marvel's Spider-Man 2",
    categoria: "Acción",
    plataformas: ["PS5"],
    precio_base: 299.90,
    desarrollador: "Insomniac Games",
    publicador: "Sony Interactive Entertainment",
    anio_lanzamiento: 2023,
    pegi: 16,
    descripcion: "Peter Parker y Miles Morales enfrentan a Kraven el Cazador y Venom.",
    tags: ["superhéroe", "mundo abierto", "combate", "narrativa"],
    metricas: {
      total_ventas_unidades: 0,
      ingresos_acumulados: 0.0,
      puntuacion_promedio: 4.8,
      total_resenas: 3
    }
  },
  {
    juego_id: "JV006",
    titulo: "Hogwarts Legacy",
    categoria: "RPG",
    plataformas: ["PS5", "Xbox", "Switch", "PC"],
    precio_base: 219.90,
    desarrollador: "Avalanche Software",
    publicador: "Warner Bros. Games",
    anio_lanzamiento: 2023,
    pegi: 12,
    descripcion: "Explora el mundo mágico de Hogwarts en el siglo XIX.",
    tags: ["Harry Potter", "mundo abierto", "magia", "RPG"],
    metricas: {
      total_ventas_unidades: 0,
      ingresos_acumulados: 0.0,
      puntuacion_promedio: 4.0,
      total_resenas: 3
    }
  }
]);

// Ejemplo de reporte mensual
db.reportes_mensuales.insertOne({
  anio: 2024,
  mes: 12,
  nombre_mes: "Diciembre",
  num_transacciones: 20,
  total_unidades: 22,
  ingresos_mes: 4998.80,
  ticket_promedio: 249.94,
  clientes_unicos: 20,
  juego_top: "Marvel's Spider-Man 2",
  categoria_top: "Acción",
  plataforma_top: "PS5",
  observacion: "Pico de ventas navideñas — 39% por encima de la media mensual"
});

print("✓ Base de datos gamestore_db inicializada correctamente");
print("✓ Colecciones creadas: juegos, ventas_procesadas, analisis_juegos,");
print("  analisis_categorias, analisis_plataformas, reportes_mensuales,");
print("  perfil_clientes, alertas_inventario");
print("✓ Índices creados para optimizar consultas frecuentes");
print("✓ Documentos de ejemplo insertados");
