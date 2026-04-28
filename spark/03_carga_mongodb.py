"""
GameInsight - Carga a MongoDB
Lee los resultados Parquet del análisis y los persiste en MongoDB
como documentos JSON dentro de la base de datos gamestore_db.

Colecciones creadas:
  - ventas_procesadas    → Detalle de cada venta enriquecida
  - analisis_juegos      → Ranking y métricas por juego
  - analisis_categorias  → Métricas agregadas por categoría
  - analisis_plataformas → Cuota de mercado por plataforma
  - reportes_mensuales   → Ingresos y tendencias por mes
  - perfil_clientes      → Top clientes por valor
  - alertas_inventario   → Juegos con stock crítico
"""

import os
os.environ["PYSPARK_PYTHON"] = "python3"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DoubleType
from pymongo import MongoClient
from datetime import datetime
from decimal import Decimal
import json

# ── INICIALIZAR SPARK ─────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("GameInsight-MongoDB") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

OUT_PATH = "/opt/spark-data/processed"
SQL_PATH = "/opt/spark-data/processed/sql_results"
MONGO_URI = "mongodb://mongodb:27017/"
DB_NAME   = "gamestore_db"

print("=" * 60)
print("  GAMEINISGHT — CARGA A MONGODB")
print("=" * 60)

# ── CONEXIÓN A MONGODB ────────────────────────────────────────────────────────
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

def fix_types(obj):
    """Convierte Decimal y otros tipos no serializables por BSON a tipos nativos."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: fix_types(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [fix_types(i) for i in obj]
    return obj

def df_to_mongo(spark_df, collection_name, drop_first=True):
    """Convierte un Spark DataFrame a lista de dicts e inserta en MongoDB."""
    # Castear columnas Decimal a Double en Spark antes de toPandas
    for field in spark_df.schema.fields:
        if isinstance(field.dataType, DecimalType):
            spark_df = spark_df.withColumn(field.name, F.col(field.name).cast(DoubleType()))
    if drop_first:
        db[collection_name].drop()
    rows = [fix_types(r) for r in spark_df.toPandas().to_dict("records")]
    if rows:
        db[collection_name].insert_many(rows)
    print(f"  ✓ {collection_name}: {len(rows)} documentos insertados")
    return len(rows)

# ── 1. VENTAS PROCESADAS ──────────────────────────────────────────────────────
print("\n[1/7] Cargando ventas procesadas ...")
ventas_df = spark.read.parquet(f"{OUT_PATH}/ventas_enriquecidas")

ventas_mongo = ventas_df.select(
    "venta_id", "fecha", "mes", "anio", "trimestre",
    "cliente_id", "nombre", "edad", "genero", "distrito", "nivel_membresia",
    "juego_id", "titulo", "categoria", "plataforma",
    "precio", "cantidad", "total",
    "metodo_pago", "vendedor",
    "puntuacion_promedio", "total_resenas"
).withColumn("fecha", F.col("fecha").cast("string"))

df_to_mongo(ventas_mongo, "ventas_procesadas")

# ── 2. ANÁLISIS POR JUEGO ─────────────────────────────────────────────────────
print("\n[2/7] Cargando análisis por juego ...")
top_juegos = spark.read.parquet(f"{SQL_PATH}/top_juegos")
df_to_mongo(top_juegos, "analisis_juegos")

# ── 3. ANÁLISIS POR CATEGORÍA ─────────────────────────────────────────────────
print("\n[3/7] Cargando análisis por categoría ...")
categorias = spark.read.parquet(f"{SQL_PATH}/categorias")
df_to_mongo(categorias, "analisis_categorias")

# ── 4. ANÁLISIS POR PLATAFORMA ────────────────────────────────────────────────
print("\n[4/7] Cargando análisis por plataforma ...")
plataformas = spark.read.parquet(f"{SQL_PATH}/plataformas")
df_to_mongo(plataformas, "analisis_plataformas")

# ── 5. REPORTES MENSUALES ─────────────────────────────────────────────────────
print("\n[5/7] Cargando reportes mensuales ...")
meses = spark.read.parquet(f"{SQL_PATH}/ventas_mensuales")
df_to_mongo(meses, "reportes_mensuales")

# ── 6. PERFIL DE CLIENTES ─────────────────────────────────────────────────────
print("\n[6/7] Cargando perfil de clientes ...")
clientes = spark.read.parquet(f"{SQL_PATH}/top_clientes")
clientes_mongo = clientes.withColumn("ultima_compra", F.col("ultima_compra").cast("string"))
df_to_mongo(clientes_mongo, "perfil_clientes")

# ── 7. ALERTAS DE INVENTARIO ──────────────────────────────────────────────────
print("\n[7/7] Cargando alertas de inventario ...")
stock_bajo = spark.read.parquet(f"{SQL_PATH}/stock_bajo")
df_to_mongo(stock_bajo, "alertas_inventario")

# ── DOCUMENTO DE METADATA DEL PIPELINE ───────────────────────────────────────
print("\nCreando documento de metadata del pipeline ...")
metadata = {
    "pipeline":        "GameInsight ETL",
    "version":         "1.0",
    "fecha_ejecucion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "tecnologias": {
        "ingesta":       "Apache Hadoop HDFS",
        "procesamiento": "Apache Spark 3.5",
        "almacenamiento":"MongoDB 7.0"
    },
    "fuentes": [
        {"archivo": "ventas.csv",         "formato": "CSV",  "registros": ventas_df.count()},
        {"archivo": "clientes.csv",       "formato": "CSV",  "registros": 50},
        {"archivo": "catalogo_juegos.json","formato": "JSON", "registros": 18},
        {"archivo": "resenas.json",       "formato": "JSON", "registros": 40},
        {"archivo": "logs_tienda.txt",    "formato": "TXT",  "registros": 80},
        {"archivo": "inventario.xml",     "formato": "XML",  "registros": 50}
    ],
    "colecciones_creadas": [
        "ventas_procesadas", "analisis_juegos", "analisis_categorias",
        "analisis_plataformas", "reportes_mensuales", "perfil_clientes",
        "alertas_inventario"
    ],
    "estado": "COMPLETADO"
}

db["pipeline_metadata"].drop()
db["pipeline_metadata"].insert_one(metadata)
print("  ✓ pipeline_metadata insertado")

# ── VERIFICACIÓN FINAL ────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("  VERIFICACIÓN — COLECCIONES EN MONGODB")
print("=" * 60)
for col_name in db.list_collection_names():
    count = db[col_name].count_documents({})
    print(f"  {col_name:<30} → {count:>4} documentos")

client.close()
print("\n[Carga a MongoDB completada exitosamente]\n")
spark.stop()
