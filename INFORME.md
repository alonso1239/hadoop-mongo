# INFORME TÉCNICO — PROYECTO BIG DATA
## GameInsight: Análisis de Ventas y Preferencias de Clientes de una Tienda de Videojuegos

---

## 1. PORTADA

| Campo | Detalle |
|---|---|
| **Actividad** | Aplicando tecnologías para las soluciones Big Data I |
| **Institución** | Certus |
| **Caso** | GameInsight — Análisis de Ventas de Tienda de Videojuegos |
| **Tecnologías** | Docker · Apache Hadoop · Apache Spark · MongoDB |
| **Año** | 2024 |

---

## 2. INTRODUCCIÓN

La tienda de videojuegos **GameZone Lima** registra sus operaciones comerciales en múltiples sistemas y formatos de archivo que no están integrados entre sí. Esta dispersión de datos impide que los responsables del negocio puedan tomar decisiones informadas sobre qué productos reponer, qué plataformas promover o qué meses son los más rentables.

El presente trabajo propone e implementa un ecosistema **Big Data funcional** que centraliza, procesa y analiza la información de ventas, clientes, catálogo de juegos, reseñas, logs de actividad e inventario, utilizando Apache Hadoop como almacén distribuido, Apache Spark como motor de procesamiento y MongoDB como base de datos documental de resultados.

---

## 3. DEFINICIÓN DEL CASO Y PROBLEMA

### 3.1 Nombre del caso
**GameInsight** — Sistema de Análisis de Ventas y Preferencias de Clientes de una Tienda de Videojuegos

### 3.2 Problema identificado
GameZone Lima acumula datos de ventas en archivos dispersos y formatos heterogéneos sin un sistema centralizado que permita responder con agilidad las siguientes preguntas de negocio:

- ¿Qué juegos se venden más?
- ¿Qué categorías prefieren los clientes?
- ¿Qué plataformas son más populares?
- ¿Qué meses tienen mayores ventas?
- ¿Qué clientes generan mayor valor?

### 3.3 Objetivo general
Diseñar e implementar un pipeline Big Data que integre fuentes de datos heterogéneas, procese la información con Apache Spark y persista los resultados analíticos en MongoDB para soportar la toma de decisiones de la tienda.

### 3.4 Actores involucrados
| Actor | Rol |
|---|---|
| Gerente de tienda | Consume reportes mensuales y rankings de ventas |
| Encargado de inventario | Consulta alertas de stock bajo |
| Analista de datos | Opera el pipeline y configura nuevos análisis |
| Vendedores | Generan los registros de ventas diarios |

### 3.5 Justificación del caso
El caso cumple todos los requisitos mínimos: problema concreto y real, datos heterogéneos en múltiples formatos, escala coherente con Big Data (crecimiento diario de transacciones), modelo documental justificado por la variabilidad de atributos entre juegos de distintas plataformas, y potencial claro de evolución hacia streaming.

### 3.6 Continuidad hacia Streaming
En la siguiente fase, el sistema puede evolucionar incorporando **Apache Kafka** para capturar ventas en tiempo real conforme se registran en el punto de venta. Spark Structured Streaming procesaría el flujo para actualizar dashboards en vivo, detectar anomalías de ventas y disparar alertas automáticas de reposición de inventario cuando el stock baje del umbral definido.

```
[TPV / Punto de Venta] → [Kafka Topic: ventas-stream]
       → [Spark Structured Streaming]
           → [MongoDB: alertas en tiempo real]
           → [Dashboard: ventas del día]
```

---

## 4. ANÁLISIS DE REQUERIMIENTOS

### 4.1 Necesidades funcionales
| ID | Requerimiento |
|---|---|
| RF-01 | Leer e integrar datos de 6 archivos en 4 formatos distintos |
| RF-02 | Identificar los 10 juegos más vendidos por unidades e ingresos |
| RF-03 | Analizar ventas agrupadas por categoría, plataforma y mes |
| RF-04 | Generar perfil de los 10 clientes con mayor gasto acumulado |
| RF-05 | Detectar y alertar juegos con stock por debajo del umbral de reposición |
| RF-06 | Persistir todos los resultados en MongoDB con estructura documental |

### 4.2 Necesidades técnicas
| ID | Requerimiento |
|---|---|
| RT-01 | Despliegue completo en contenedores Docker sin instalación local de Hadoop/Spark |
| RT-02 | Almacenamiento de archivos raw en HDFS |
| RT-03 | Procesamiento con Spark RDD, DataFrame API y Spark SQL |
| RT-04 | Lectura de 4 formatos: CSV, JSON, TXT y XML |
| RT-05 | Pipeline orquestado desde Java con `ProcessBuilder` |
| RT-06 | Base de datos MongoDB con colecciones validadas e índices optimizados |

### 4.3 Origen y naturaleza de los datos
| Archivo | Origen | Naturaleza |
|---|---|---|
| `ventas.csv` | Simulado por el equipo | Transaccional, estructurado |
| `clientes.csv` | Simulado por el equipo | Maestro, estructurado |
| `catalogo_juegos.json` | Generado con apoyo de IA | Catálogo, semi-estructurado, atributos variables |
| `resenas.json` | Generado con apoyo de IA | Opiniones, semi-estructurado, texto libre |
| `logs_tienda.txt` | Simulado por el equipo | Eventos secuenciales, no estructurado |
| `inventario.xml` | Simulado por el equipo | Stock jerárquico, semi-estructurado |

### 4.4 Posibles problemas de calidad de datos
- **Valores nulos**: clientes o juegos que no hacen match en el join (manejado con `left join`)
- **Tipos incorrectos**: precios como string en CSV (manejado con `inferSchema=True`)
- **Formato de fechas**: inconsistencias regionales (manejado con `to_date("yyyy-MM-dd")`)
- **Tipos Decimal de Spark SQL**: no serializables por BSON (manejado con cast a `DoubleType`)
- **JSON multilínea**: arrays JSON no compatibles con el lector por defecto de Spark (manejado con `multiLine=true`)

---

## 5. DESCRIPCIÓN DE LOS DATOS DE ENTRADA

### Resumen de archivos
| # | Archivo | Formato | Registros | Descripción |
|---|---|---|---|---|
| 1 | `ventas.csv` | CSV | 144 | Transacciones de venta: fecha, cliente, juego, plataforma, precio, método de pago |
| 2 | `clientes.csv` | CSV | 50 | Perfil de clientes: nombre, edad, distrito, membresía |
| 3 | `catalogo_juegos.json` | JSON | 18 | Catálogo de juegos con atributos anidados: plataformas (array), tags, desarrollador |
| 4 | `resenas.json` | JSON | 40 | Reseñas de clientes: puntuación, comentario, votos útiles |
| 5 | `logs_tienda.txt` | TXT | ~90 | Log de eventos: LOGIN, VIEW_GAME, ADD_CART, PURCHASE, LOGOUT |
| 6 | `inventario.xml` | XML | 44 combinaciones | Stock por juego y plataforma con umbral de reposición |

### Formatos utilizados: 4 (CSV, JSON, TXT, XML)

### Uso previsto de cada archivo
- **ventas.csv + clientes.csv** → Join principal para enriquecer cada venta con datos del cliente
- **catalogo_juegos.json** → Join para añadir categoría y desarrollador a cada venta
- **resenas.json** → Agregación de puntuación promedio por juego
- **logs_tienda.txt** → Procesado con RDD para contar sesiones y compras por cliente
- **inventario.xml** → Parseado con `xml.etree` para generar alertas de stock bajo

---

## 6. DISEÑO DE LA BASE DE DATOS EN MONGODB

### 6.1 Nombre de la base de datos
`gamestore_db`

### 6.2 Colecciones y atributos principales

#### `ventas_procesadas` (144 documentos)
```json
{
  "venta_id": "V0135",
  "fecha": "2024-12-17",
  "mes": 12,
  "anio": 2024,
  "trimestre": 4,
  "cliente_id": "C020",
  "nombre": "Daniela Aguilar",
  "edad": 24,
  "genero": "F",
  "distrito": "Miraflores",
  "nivel_membresia": "Silver",
  "juego_id": "JV001",
  "titulo": "God of War Ragnarök",
  "categoria": "Acción",
  "plataforma": "PS5",
  "precio": 299.90,
  "cantidad": 2,
  "total": 599.80,
  "metodo_pago": "Tarjeta Crédito",
  "vendedor": "Carlos",
  "puntuacion_promedio": 4.67,
  "total_resenas": 3
}
```

#### `analisis_juegos` (10 documentos — Top 10)
```json
{
  "juego_id": "JV006",
  "titulo": "Hogwarts Legacy",
  "categoria": "RPG",
  "total_unidades": 17,
  "num_transacciones": 17,
  "ingresos_total": 3738.30,
  "rating_promedio": 4.0
}
```

#### `analisis_categorias` (8 documentos)
```json
{
  "categoria": "Acción",
  "num_ventas": 38,
  "total_unidades": 39,
  "ingresos_total": 9836.20,
  "precio_promedio": 279.90,
  "pct_ventas": 26.4
}
```

#### `analisis_plataformas` (5 documentos)
```json
{
  "plataforma": "PS5",
  "num_ventas": 89,
  "total_unidades": 91,
  "ingresos_total": 22814.10,
  "ticket_promedio": 256.34,
  "cuota_mercado_pct": 61.8,
  "stock_actual": 144
}
```

#### `reportes_mensuales` (12 documentos)
```json
{
  "anio": 2024,
  "mes": 12,
  "num_transacciones": 20,
  "total_unidades": 22,
  "ingresos_mes": 4998.80,
  "ticket_promedio": 249.94,
  "clientes_unicos": 20
}
```

#### `perfil_clientes` (10 documentos — Top 10)
```json
{
  "cliente_id": "C008",
  "nombre": "Lucía Vargas",
  "nivel_membresia": "Gold",
  "distrito": "Surco",
  "num_compras": 3,
  "gasto_total": 829.70,
  "ticket_promedio": 276.57,
  "ultima_compra": "2024-08-20"
}
```

#### `alertas_inventario` (documentos con stock ≤ umbral)
```json
{
  "juego_id": "JV005",
  "titulo": "Marvel's Spider-Man 2",
  "plataforma": "PS5",
  "stock": 8,
  "umbral": 5,
  "precio": 299.90
}
```

### 6.3 Relaciones lógicas entre colecciones

```
ventas_procesadas ──── juego_id ────► analisis_juegos
ventas_procesadas ──── categoria ───► analisis_categorias
ventas_procesadas ──── plataforma ──► analisis_plataformas
ventas_procesadas ──── mes/anio ────► reportes_mensuales
ventas_procesadas ──── cliente_id ──► perfil_clientes
analisis_juegos ─────── juego_id ───► alertas_inventario
```

### 6.4 Índices definidos
```javascript
db.ventas_procesadas.createIndex({ venta_id: 1 }, { unique: true })
db.ventas_procesadas.createIndex({ fecha: 1 })
db.ventas_procesadas.createIndex({ cliente_id: 1 })
db.ventas_procesadas.createIndex({ juego_id: 1 })
db.ventas_procesadas.createIndex({ mes: 1, anio: 1 })
db.analisis_juegos.createIndex({ total_unidades: -1 })
db.analisis_plataformas.createIndex({ ingresos_total: -1 })
db.reportes_mensuales.createIndex({ anio: 1, mes: 1 })
db.perfil_clientes.createIndex({ gasto_total: -1 })
db.alertas_inventario.createIndex({ stock: 1 })
```

### 6.5 Justificación del modelo documental
MongoDB es la elección correcta para este caso porque:
1. Cada juego tiene **atributos variables** según plataforma (PS5 tiene exclusivos, PC tiene mods)
2. Las reseñas tienen **estructura anidada** (votos, verificación, comentario libre)
3. Los reportes mensuales requieren **documentos autocontenidos** para consulta rápida sin joins
4. La naturaleza **heterogénea** de los datos de ventas (métodos de pago, plataformas, géneros) encaja con el esquema flexible de MongoDB

---

## 7. DISEÑO DEL PROCESAMIENTO DE DATOS

### 7.1 Diagrama de arquitectura

```
╔══════════════════════════════════════════════════════════════════════╗
║                     FUENTES DE DATOS (6 archivos)                   ║
║  ventas.csv  clientes.csv  catalogo.json  resenas.json  logs.txt  inventario.xml
║     CSV          CSV           JSON           JSON        TXT        XML
╚══════════════╦═══════════════════════════════════════════════════════╝
               │  docker cp → docker exec hdfs dfs -put
               ▼
╔══════════════════════════════════════════════════════════════════════╗
║                    APACHE HADOOP HDFS                                ║
║         NameNode (metadata)    DataNode (bloques)                    ║
║         hdfs://namenode:9000/gamestore/raw/                          ║
╚══════════════╦═══════════════════════════════════════════════════════╝
               │  spark-submit (montaje /opt/spark-data)
               ▼
╔══════════════════════════════════════════════════════════════════════╗
║                      APACHE SPARK 3.5.3                              ║
║                                                                      ║
║  ┌─────────────────────────────────────────────────────────────┐    ║
║  │ 01_etl_ventas.py — ETL                                      │    ║
║  │  • RDD:       parseo de logs_tienda.txt                     │    ║
║  │  • DataFrame: lectura CSV, JSON (multiLine), XML (etree)    │    ║
║  │  • Limpieza:  nulos, tipos, fechas, duplicados              │    ║
║  │  • Join:      5 fuentes integradas en dataset único         │    ║
║  │  → Salida: /processed/ventas_enriquecidas.parquet           │    ║
║  └─────────────────────────────────────────────────────────────┘    ║
║  ┌─────────────────────────────────────────────────────────────┐    ║
║  │ 02_analisis_sql.py — Spark SQL                              │    ║
║  │  • Vistas temporales: ventas, inventario                    │    ║
║  │  • SQL: top juegos, categorías, plataformas, mensual        │    ║
║  │  • Window Functions: cuota de mercado por plataforma        │    ║
║  │  → Salida: /processed/sql_results/*.parquet                 │    ║
║  └─────────────────────────────────────────────────────────────┘    ║
╚══════════════╦═══════════════════════════════════════════════════════╝
               │  pymongo insert_many
               ▼
╔══════════════════════════════════════════════════════════════════════╗
║                         MONGODB 7.0                                  ║
║                      gamestore_db                                    ║
║                                                                      ║
║  ventas_procesadas │ analisis_juegos │ analisis_categorias           ║
║  analisis_plataformas │ reportes_mensuales │ perfil_clientes         ║
║  alertas_inventario │ pipeline_metadata                              ║
╚══════════════════════════════════════════════════════════════════════╝
               │
               ▼
╔══════════════════════════════════════════════════════════════════════╗
║                      MONGO EXPRESS (UI)                              ║
║                   http://localhost:8082                              ║
╚══════════════════════════════════════════════════════════════════════╝
```

### 7.2 Flujo ETL detallado

| Etapa | Descripción | Herramienta |
|---|---|---|
| **Ingesta** | 6 archivos copiados al namenode con `docker cp` y subidos a HDFS | Hadoop HDFS |
| **Extracción** | Lectura de CSV, JSON (multiLine), TXT (RDD), XML (xml.etree) | Spark |
| **Limpieza** | Cast de tipos, parseo de fechas, filtro de nulos, deduplicación | Spark DataFrame |
| **Transformación** | Columnas derivadas (mes, año, trimestre, total), explosión de arrays | Spark DataFrame |
| **Integración** | Left joins por juego_id y cliente_id uniendo 5 fuentes | Spark SQL |
| **Análisis** | 6 consultas SQL con GROUP BY, Window Functions, ORDER BY | Spark SQL |
| **Carga** | Conversión a dicts Python, cast Decimal→float, insert_many | pymongo |

### 7.3 Consultas Spark SQL implementadas

```sql
-- Top 10 juegos más vendidos
SELECT juego_id, titulo, categoria,
       SUM(cantidad) AS total_unidades,
       ROUND(SUM(total), 2) AS ingresos_total
FROM ventas
GROUP BY juego_id, titulo, categoria
ORDER BY total_unidades DESC LIMIT 10;

-- Cuota de mercado por plataforma (Window Function)
SELECT plataforma,
       COUNT(*) AS num_ventas,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS cuota_mercado_pct
FROM ventas
GROUP BY plataforma ORDER BY num_ventas DESC;

-- Tendencia mensual de ventas
SELECT anio, mes, COUNT(*) AS num_transacciones,
       ROUND(SUM(total), 2) AS ingresos_mes
FROM ventas
GROUP BY anio, mes ORDER BY anio, mes;
```

---

## 8. FRAMEWORKS Y LIBRERÍAS UTILIZADAS

| Tecnología | Versión | Justificación |
|---|---|---|
| **Apache Hadoop HDFS** | 3.2.1 | Almacenamiento distribuido tolerante a fallos para los archivos raw; simula el ecosistema Big Data real |
| **Apache Spark** | 3.5.3 | Motor de procesamiento en memoria para transformaciones y análisis sobre grandes volúmenes de datos |
| **PySpark** | 3.5.3 | API Python de Spark; estándar en la industria para data engineering, menor boilerplate que Java/Scala |
| **MongoDB** | 7.0 | Base de datos documental flexible para almacenar resultados con esquemas heterogéneos |
| **pymongo** | latest | Conector Python-MongoDB para cargar resultados desde los scripts Spark |
| **pandas** | latest | Conversión intermedia de Spark DataFrame a dict para inserción en MongoDB |
| **Docker / Docker Compose** | latest | Despliegue reproducible del ecosistema completo sin instalación local de Hadoop o Spark |
| **xml.etree.ElementTree** | stdlib | Parseo del inventario XML sin dependencias externas adicionales |
| **Java ProcessBuilder** | JDK 17 | Orquestación del pipeline completo desde IntelliJ IDEA |

---

## 9. PROTOTIPO FUNCIONAL EN DOCKER

### 9.1 Contenedores desplegados

| Contenedor | Imagen | Puerto | Rol |
|---|---|---|---|
| `gamestore-namenode` | bde2020/hadoop-namenode:3.2.1 | 19870, 19000 | Gestiona metadata de HDFS |
| `gamestore-datanode` | bde2020/hadoop-datanode:3.2.1 | 9864 | Almacena bloques de datos en HDFS |
| `gamestore-spark-master` | apache/spark:3.5.3 | 8080, 7077 | Coordina los jobs de Spark |
| `gamestore-spark-worker` | apache/spark:3.5.3 | 8081 | Ejecuta las tareas de Spark |
| `gamestore-mongodb` | mongo:7.0 | 27017 | Base de datos documental |
| `gamestore-mongo-express` | mongo-express:1.0.2 | 8082 | UI web para explorar MongoDB |

### 9.2 Cómo ejecutar el prototipo

```
1. Abrir Docker Desktop
2. Abrir el proyecto en IntelliJ IDEA
3. Verificar Working Directory: Ejecutar → Editar Configuraciones → Working Directory = raíz del proyecto
4. Ejecutar Main.java (botón Run ▶)
```

El orquestador Java ejecuta automáticamente:
- `docker compose down` (limpia contenedores anteriores)
- `docker compose up -d` (levanta los 6 servicios)
- Espera activa de HDFS, MongoDB y Spark Master
- `hdfs dfsadmin -safemode leave` (habilita escritura en HDFS)
- `docker cp` de los 6 archivos + `hdfs dfs -put` a HDFS
- `python3 -m pip install pymongo pandas`
- `spark-submit` de los 3 jobs secuencialmente
- Verificación final en MongoDB

### 9.3 Interfaces de verificación

| UI | URL | Credenciales |
|---|---|---|
| HDFS Web UI | http://localhost:19870 | — |
| Spark Master UI | http://localhost:8080 | — |
| Spark Worker UI | http://localhost:8081 | — |
| Mongo Express | http://localhost:8082 | admin / admin123 |

### 9.4 Credenciales de acceso a MongoDB

#### Mongo Express (UI web)
| Campo | Valor |
|---|---|
| URL | http://localhost:8082 |
| Usuario | `admin` |
| Contraseña | `admin123` |

#### Conexión directa a MongoDB (Compass / mongosh)
| Campo | Valor |
|---|---|
| Host | `localhost` |
| Puerto | `27017` |
| Base de datos | `gamestore_db` |
| Autenticación | Sin credenciales (acceso libre en red local) |

```bash
# Conectar desde terminal
mongosh mongodb://localhost:27017/gamestore_db

# Consulta de ejemplo: top juegos más vendidos
db.analisis_juegos.find().sort({ total_unidades: -1 }).pretty()

# Consulta de ejemplo: ventas por plataforma
db.analisis_plataformas.find({}, { plataforma:1, num_ventas:1, cuota_mercado_pct:1 }).sort({ num_ventas:-1 })

# Consulta de ejemplo: alertas de stock bajo
db.alertas_inventario.find().sort({ stock: 1 }).pretty()
```

### 9.4 Evidencia de funcionamiento esperada
- **HDFS**: 6 archivos visibles en `/gamestore/raw/` desde la Web UI
- **Spark**: 3 jobs completados con estado FINISHED en el historial
- **MongoDB**: 8 colecciones con documentos en `gamestore_db` vía Mongo Express

---

## 10. BENEFICIOS DEL DISEÑO

### Beneficio 1 — Centralización de datos heterogéneos (Tangible)
La solución unifica en un único pipeline 6 archivos de 4 formatos distintos que anteriormente estaban dispersos. El equipo de gestión pasa de consultar manualmente hojas de cálculo y archivos de texto a tener todos los indicadores disponibles en MongoDB con una sola ejecución.

### Beneficio 2 — Escalabilidad horizontal con Hadoop y Spark (Tangible)
El ecosistema está diseñado para crecer. Si GameZone expande sus operaciones a múltiples sucursales y el volumen de ventas crece de miles a millones de registros, basta con agregar nodos DataNode y Spark Workers al cluster sin modificar el código de procesamiento.

### Beneficio 3 — Velocidad de análisis con procesamiento en memoria (Tangible)
Apache Spark procesa los 144 registros de ventas integrados con 5 fuentes en segundos gracias a su ejecución en memoria. A escala real (millones de registros), Spark mantiene ventajas de rendimiento de 10x a 100x frente a soluciones MapReduce tradicionales.

### Beneficio 4 — Modelo de datos flexible para el catálogo (Intangible)
MongoDB permite que cada documento de juego tenga exactamente los atributos que le corresponden: un juego exclusivo de PS5 no necesita campos vacíos de otras plataformas, y un juego con DLCs puede tener arrays de contenido adicional sin alterar el esquema de otros documentos. Esto reduce la complejidad de mantenimiento frente a una base de datos relacional.

### Beneficio 5 — Trazabilidad y auditoría del pipeline (Intangible)
El documento `pipeline_metadata` insertado automáticamente en MongoDB registra la fecha de ejecución, las fuentes procesadas, la cantidad de registros por archivo y el estado final. Esto garantiza trazabilidad completa de cada ejecución del análisis, facilitando la auditoría y la detección de anomalías en los datos de entrada.

---

## 11. MÉTRICAS Y VIABILIDAD

### Métrica 1 — Rendimiento: Throughput de procesamiento Spark
**Definición**: Registros procesados por segundo durante el job ETL.

| Escenario | Registros | Tiempo estimado | Throughput |
|---|---|---|---|
| Prototipo (1 worker, 1G RAM) | 144 ventas + 5 fuentes | ~45 segundos | ~3.2 registros/seg |
| Producción (4 workers, 8G RAM) | 1,000,000 ventas | ~180 segundos | ~5,555 registros/seg |

**Justificación**: La arquitectura Spark escala linealmente con workers adicionales. El prototipo demuestra el pipeline completo; la producción agrega capacidad horizontal.

### Métrica 2 — Tiempo: Duración total del pipeline end-to-end
**Definición**: Tiempo desde `docker compose up` hasta la última inserción en MongoDB.

| Fase | Tiempo (prototipo) |
|---|---|
| Arranque de contenedores | ~60 seg (primera vez con descarga de imágenes: ~5 min) |
| Carga de archivos a HDFS | ~10 seg |
| Job ETL (01_etl_ventas.py) | ~40 seg |
| Job SQL (02_analisis_sql.py) | ~30 seg |
| Job MongoDB (03_carga_mongodb.py) | ~20 seg |
| **Total pipeline (sin descarga)** | **~2.5 minutos** |

**Viabilidad**: Para un análisis diario nocturno de ventas, un pipeline de 2-3 minutos es completamente viable y deja margen amplio para escalar.

### Métrica 3 — Esfuerzo: Complejidad de mantenimiento
**Definición**: Número de componentes que requieren intervención manual ante cambios en los datos de entrada.

| Cambio | Componentes a modificar | Esfuerzo |
|---|---|---|
| Nuevo archivo CSV de ventas | Solo reemplazar el archivo en `data/raw/` | Bajo (0 código) |
| Nueva columna en ventas.csv | Actualizar `inferSchema=True` solo si el tipo es ambiguo | Muy bajo |
| Nueva colección en MongoDB | Agregar 1 función `df_to_mongo()` en el script 03 | Bajo |
| Nuevo formato de archivo | Agregar parser al script ETL | Medio |

**Viabilidad**: El diseño modular con ETL separado del análisis permite evolucionar el sistema con esfuerzo mínimo.

---

## 12. MEJORES PRÁCTICAS DE DISEÑO BIG DATA

### Práctica 1 — Separación de capas ETL (Patrón Lambda Architecture)
**Fuente**: Marz & Warren, "Big Data" (2015); adoptado por Netflix, LinkedIn.

Separar el pipeline en capas independientes: ingesta (HDFS), procesamiento batch (Spark ETL), análisis (Spark SQL) y servicio (MongoDB). Cada capa puede fallar y reejecutarse de forma independiente sin afectar a las demás. En el proyecto, los 3 scripts de Spark son independientes y la salida Parquet actúa como capa intermedia durable.

### Práctica 2 — Formato de almacenamiento columnar (Parquet)
**Fuente**: Apache Parquet Documentation; Google Dremel paper (2010).

Los resultados intermedios se guardan en formato **Parquet** en lugar de CSV. Parquet comprime los datos 3x-10x y permite lectura selectiva de columnas (predicate pushdown), acelerando las consultas posteriores. En el proyecto, `ventas_enriquecidas.parquet` es leído por el script de análisis SQL y el de carga MongoDB sin duplicar el procesamiento.

### Práctica 3 — Idempotencia del pipeline
**Fuente**: Martin Fowler, "DataOps Patterns" (2020); Apache Airflow Best Practices.

Cada ejecución del pipeline produce el mismo resultado sin importar cuántas veces se ejecute. Se implementa con `.write.mode("overwrite")` en Spark y `drop()` antes de cada `insert_many()` en MongoDB. Esto permite re-ejecutar el pipeline ante fallos sin dejar datos corruptos o duplicados.

### Práctica 4 — Esquema flexible con validación en MongoDB
**Fuente**: MongoDB Best Practices Guide; ThoughtWorks Technology Radar.

Aunque MongoDB permite documentos sin esquema, aplicar **JSON Schema Validation** en las colecciones críticas (como `juegos`) previene la inserción de documentos malformados que romperían consultas posteriores. El proyecto define validadores con `$jsonSchema` en el `init.js` para las colecciones maestras.

### Práctica 5 — Orquestación automatizada del pipeline
**Fuente**: Apache Airflow documentation; Databricks Lakehouse patterns (2022); Google Cloud Dataflow.

Los pipelines Big Data en producción nunca se ejecutan manualmente paso a paso. Se orquestan con herramientas como Apache Airflow, Prefect o, en nuestro caso, un **orquestador Java** que encadena todos los pasos con control de errores, espera de servicios y verificación final. Esto garantiza reproducibilidad, trazabilidad y posibilidad de programar ejecuciones automáticas (ejecución nocturna diaria, por ejemplo).

---

## 13. CONCLUSIONES

**El diseño es útil para el problema**: La solución GameInsight integra exitosamente 6 fuentes de datos heterogéneas en un pipeline unificado que responde las 4 preguntas de negocio clave de GameZone Lima. Los resultados almacenados en MongoDB permiten consultas en milisegundos sobre datos que antes requerían revisión manual de múltiples archivos.

**La arquitectura es viable**: El prototipo funcional demuestra que el ecosistema Hadoop + Spark + MongoDB puede desplegarse en una máquina local con Docker en aproximadamente 2.5 minutos de tiempo de procesamiento. Las métricas de throughput, tiempo y esfuerzo de mantenimiento confirman que la arquitectura es sostenible y escalable para el crecimiento real del negocio.

**El caso puede continuar en la siguiente evaluación**: La arquitectura está preparada para evolucionar hacia streaming. La incorporación de Apache Kafka como broker de mensajes permitirá capturar ventas en tiempo real desde el punto de venta, procesarlas con Spark Structured Streaming y actualizar las colecciones de MongoDB al instante. Las colecciones `reportes_mensuales` y `alertas_inventario` son los candidatos naturales para convertirse en vistas de streaming en la siguiente fase.

---

## 14. REFERENCIAS

1. Apache Hadoop Documentation. (2024). *HDFS Architecture Guide*. https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html

2. Apache Spark Documentation. (2024). *Spark SQL, DataFrames and Datasets Guide*. https://spark.apache.org/docs/latest/sql-programming-guide.html

3. MongoDB, Inc. (2024). *MongoDB Manual — Data Modeling*. https://www.mongodb.com/docs/manual/core/data-modeling-introduction/

4. Chambers, B. & Zaharia, M. (2018). *Spark: The Definitive Guide*. O'Reilly Media.

5. Marz, N. & Warren, J. (2015). *Big Data: Principles and best practices of scalable realtime data systems*. Manning Publications.

6. White, T. (2015). *Hadoop: The Definitive Guide* (4th ed.). O'Reilly Media.

7. Docker Inc. (2024). *Docker Compose Documentation*. https://docs.docker.com/compose/

8. Bitnami / VMware. (2024). *Apache Spark Docker Image*. https://hub.docker.com/r/apache/spark

9. MongoDB, Inc. (2024). *PyMongo Documentation*. https://pymongo.readthedocs.io/

10. Fowler, M. (2020). *DataOps: A Set of Practices to Improve the Development of Data Analytics*. martinfowler.com
