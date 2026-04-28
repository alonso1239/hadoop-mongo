# GUÍA DE EJECUCIÓN MANUAL
## GameInsight — Pipeline Big Data paso a paso

> Esta guía describe cómo ejecutar el ecosistema Big Data de forma manual
> desde la terminal, sin usar el orquestador Java (Main.java).
> Útil para demostraciones, sustentaciones o depuración de errores.

---

## PREREQUISITOS

- Docker Desktop instalado y **ejecutándose** (ícono de ballena activo en la barra de tareas)
- Terminal abierta en la **raíz del proyecto**: `D:\Documentos\Desktop\JAVA\BigData`

---

## PASO 1 — Posicionarse en la raíz del proyecto

```bash
cd D:\Documentos\Desktop\JAVA\BigData
```

---

## PASO 2 — Limpiar contenedores anteriores

```bash
docker compose down --remove-orphans
```

> Elimina contenedores de ejecuciones previas que puedan estar ocupando puertos.
> Si es la primera vez, este comando simplemente no hace nada.

---

## PASO 3 — Levantar el ecosistema completo

```bash
docker compose up -d
```

> La primera vez descarga las imágenes Docker (~5 minutos según tu internet).
> Las siguientes ejecuciones arrancan en ~30 segundos.

Verifica que los 6 contenedores estén corriendo:

```bash
docker ps
```

Debes ver estos 6 contenedores con estado `Up`:

```
gamestore-namenode
gamestore-datanode
gamestore-spark-master
gamestore-spark-worker
gamestore-mongodb
gamestore-mongo-express
```

---

## PASO 4 — Esperar que HDFS esté listo

Ejecuta el siguiente comando repetidamente hasta que responda sin error:

```bash
docker exec gamestore-namenode hdfs dfs -ls /
```

> Puede tardar entre 30 y 60 segundos en estar disponible.
> Cuando responda `Found 0 items` o liste directorios, HDFS está listo.

---

## PASO 5 — Salir del modo seguro de HDFS (SafeMode)

HDFS arranca en SafeMode (solo lectura). Este comando lo fuerza a salir:

```bash
docker exec gamestore-namenode hdfs dfsadmin -safemode leave
```

Respuesta esperada:
```
Safe mode is OFF
```

---

## PASO 6 — Crear estructura de directorios en HDFS

```bash
docker exec gamestore-namenode hdfs dfs -mkdir -p /gamestore/raw
docker exec gamestore-namenode hdfs dfs -mkdir -p /gamestore/processed
```

---

## PASO 7 — Subir los 6 archivos a HDFS

Para cada archivo se realizan 2 operaciones:
1. `docker cp` — copia el archivo del host al contenedor (a `/tmp/`)
2. `hdfs dfs -put` — sube el archivo de `/tmp/` a HDFS

```bash
# 1. ventas.csv (CSV)
docker cp data/raw/ventas.csv gamestore-namenode:/tmp/ventas.csv
docker exec gamestore-namenode hdfs dfs -put -f /tmp/ventas.csv /gamestore/raw/

# 2. clientes.csv (CSV)
docker cp data/raw/clientes.csv gamestore-namenode:/tmp/clientes.csv
docker exec gamestore-namenode hdfs dfs -put -f /tmp/clientes.csv /gamestore/raw/

# 3. catalogo_juegos.json (JSON)
docker cp data/raw/catalogo_juegos.json gamestore-namenode:/tmp/catalogo_juegos.json
docker exec gamestore-namenode hdfs dfs -put -f /tmp/catalogo_juegos.json /gamestore/raw/

# 4. resenas.json (JSON)
docker cp data/raw/resenas.json gamestore-namenode:/tmp/resenas.json
docker exec gamestore-namenode hdfs dfs -put -f /tmp/resenas.json /gamestore/raw/

# 5. logs_tienda.txt (TXT)
docker cp data/raw/logs_tienda.txt gamestore-namenode:/tmp/logs_tienda.txt
docker exec gamestore-namenode hdfs dfs -put -f /tmp/logs_tienda.txt /gamestore/raw/

# 6. inventario.xml (XML)
docker cp data/raw/inventario.xml gamestore-namenode:/tmp/inventario.xml
docker exec gamestore-namenode hdfs dfs -put -f /tmp/inventario.xml /gamestore/raw/
```

Verifica que los 6 archivos estén en HDFS:

```bash
docker exec gamestore-namenode hdfs dfs -ls /gamestore/raw/
```

Resultado esperado:
```
Found 6 items
-rw-r--r-- ...  /gamestore/raw/catalogo_juegos.json
-rw-r--r-- ...  /gamestore/raw/clientes.csv
-rw-r--r-- ...  /gamestore/raw/inventario.xml
-rw-r--r-- ...  /gamestore/raw/logs_tienda.txt
-rw-r--r-- ...  /gamestore/raw/resenas.json
-rw-r--r-- ...  /gamestore/raw/ventas.csv
```

---

## PASO 8 — Instalar dependencias Python en Spark

```bash
docker exec gamestore-spark-master python3 -m pip install pymongo pandas --quiet
```

> `pymongo` permite conectar Spark con MongoDB.
> `pandas` permite convertir DataFrames de Spark a diccionarios Python para inserción.

---

## PASO 9 — Ejecutar el ETL (01_etl_ventas.py)

**Qué hace:** Lee los 6 archivos (CSV con DataFrame, JSON con multiLine, TXT con RDD, XML con xml.etree), limpia los datos, realiza joins entre las 5 fuentes y guarda el resultado integrado en formato Parquet.

```bash
docker exec gamestore-spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 512m \
  --driver-memory 512m \
  /opt/spark-apps/01_etl_ventas.py
```

Resultado esperado al final:
```
[ETL completado exitosamente]
```

---

## PASO 10 — Ejecutar el análisis Spark SQL (02_analisis_sql.py)

**Qué hace:** Lee el Parquet generado en el paso anterior, registra vistas temporales y ejecuta 6 consultas SQL que responden las preguntas de negocio. Guarda los resultados en Parquet separados por tema.

```bash
docker exec gamestore-spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 512m \
  --driver-memory 512m \
  /opt/spark-apps/02_analisis_sql.py
```

Resultado esperado al final:
```
[Análisis SQL completado exitosamente]
```

---

## PASO 11 — Cargar resultados en MongoDB (03_carga_mongodb.py)

**Qué hace:** Lee los Parquet de resultados y los inserta en 8 colecciones de MongoDB usando pymongo.

```bash
docker exec gamestore-spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 512m \
  --driver-memory 512m \
  /opt/spark-apps/03_carga_mongodb.py
```

Resultado esperado:
```
✓ ventas_procesadas: 144 documentos insertados
✓ analisis_juegos: 10 documentos insertados
✓ analisis_categorias: 8 documentos insertados
✓ analisis_plataformas: 5 documentos insertados
✓ reportes_mensuales: 12 documentos insertados
✓ perfil_clientes: 10 documentos insertados
✓ alertas_inventario: X documentos insertados
✓ pipeline_metadata insertado
[Carga a MongoDB completada exitosamente]
```

---

## PASO 12 — Verificar colecciones en MongoDB

```bash
docker exec gamestore-mongodb mongosh gamestore_db --eval \
  "db.getCollectionNames().forEach(c => print(c + ': ' + db[c].countDocuments() + ' docs'))"
```

---

## PASO 13 — Abrir las interfaces web

| Servicio | URL | Credenciales |
|---|---|---|
| HDFS Web UI | http://localhost:19870 | — |
| Spark Master UI | http://localhost:8080 | — |
| Spark Worker UI | http://localhost:8081 | — |
| Mongo Express | http://localhost:8082 | `admin` / `admin123` |

---

## ACCESO DIRECTO A MONGODB

### Desde Mongo Express (navegador)
```
URL:        http://localhost:8082
Usuario:    admin
Contraseña: admin123
```

### Desde MongoDB Compass
```
URI: mongodb://localhost:27017
Base de datos: gamestore_db
Autenticación: ninguna
```

### Desde terminal con mongosh
```bash
docker exec -it gamestore-mongodb mongosh gamestore_db
```

Consultas de ejemplo dentro de mongosh:

```javascript
// Top juegos más vendidos
db.analisis_juegos.find().sort({ total_unidades: -1 }).pretty()

// Ventas por plataforma con cuota de mercado
db.analisis_plataformas.find({}, { plataforma:1, num_ventas:1, cuota_mercado_pct:1 }).sort({ num_ventas:-1 })

// Tendencia mensual de ingresos
db.reportes_mensuales.find({}, { mes:1, ingresos_mes:1 }).sort({ mes:1 })

// Alertas de stock bajo
db.alertas_inventario.find().sort({ stock: 1 }).pretty()

// Top 10 clientes por gasto
db.perfil_clientes.find().sort({ gasto_total: -1 }).pretty()
```

---

## RESUMEN DEL FLUJO COMPLETO

```
Docker Desktop activo
        │
        ▼
docker compose down --remove-orphans    ← limpiar
        │
        ▼
docker compose up -d                    ← levantar 6 contenedores
        │
        ▼
hdfs dfsadmin -safemode leave          ← habilitar escritura en HDFS
        │
        ▼
hdfs dfs -mkdir /gamestore/raw         ← crear directorios
        │
        ▼
docker cp + hdfs dfs -put (x6)         ← subir archivos a HDFS
        │
        ▼
pip install pymongo pandas             ← dependencias Python
        │
        ▼
spark-submit 01_etl_ventas.py          ← ETL: leer, limpiar, integrar → Parquet
        │
        ▼
spark-submit 02_analisis_sql.py        ← Spark SQL: 6 consultas → Parquet
        │
        ▼
spark-submit 03_carga_mongodb.py       ← cargar → 8 colecciones MongoDB
        │
        ▼
Mongo Express: http://localhost:8082   ← verificar resultados
```
