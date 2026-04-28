"""
GameInsight - ETL Pipeline
Lee los 5 archivos fuente (CSV, JSON, TXT, XML), limpia y transforma los datos,
y guarda el resultado procesado en Parquet para su análisis posterior.

Demuestra: Spark RDD, DataFrame API y lectura de formatos heterogéneos.
"""

import xml.etree.ElementTree as ET
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType
)

# ── 1. INICIALIZAR SPARK ───────────────────────────────────────────────────────
import os
os.environ["PYSPARK_PYTHON"] = "python3"

spark = SparkSession.builder \
    .appName("GameInsight-ETL") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext

BASE_PATH = "/opt/spark-data/raw"
OUT_PATH  = "/opt/spark-data/processed"

print("=" * 60)
print("  GAMEINISGHT — ETL PIPELINE")
print("=" * 60)

# ── 2. LECTURA CSV: ventas y clientes ─────────────────────────────────────────
print("\n[1/5] Leyendo ventas.csv ...")
ventas_df = spark.read.csv(
    f"{BASE_PATH}/ventas.csv",
    header=True,
    inferSchema=True
)

print(f"  → {ventas_df.count()} registros de ventas cargados")
ventas_df.printSchema()

print("\n[2/5] Leyendo clientes.csv ...")
clientes_df = spark.read.csv(
    f"{BASE_PATH}/clientes.csv",
    header=True,
    inferSchema=True
)
print(f"  → {clientes_df.count()} clientes cargados")

# ── 3. LECTURA JSON: catálogo y reseñas ───────────────────────────────────────
print("\n[3/5] Leyendo catalogo_juegos.json ...")
catalogo_df = spark.read.option("multiLine", "true").json(f"{BASE_PATH}/catalogo_juegos.json")
print(f"  → {catalogo_df.count()} juegos en catálogo")
catalogo_df.printSchema()

print("\n[4/5] Leyendo resenas.json ...")
resenas_df = spark.read.option("multiLine", "true").json(f"{BASE_PATH}/resenas.json")
print(f"  → {resenas_df.count()} reseñas cargadas")

# ── 4. LECTURA TXT con RDD: logs de tienda ────────────────────────────────────
print("\n[5/5] Leyendo logs_tienda.txt con RDD ...")
logs_rdd = sc.textFile(f"{BASE_PATH}/logs_tienda.txt")

# Filtrar solo líneas de PURCHASE para contar compras por cliente via RDD
compras_rdd = (
    logs_rdd
    .filter(lambda line: "PURCHASE" in line and "SISTEMA" not in line)
    .map(lambda line: line.split())
    .map(lambda parts: (parts[2], 1))           # (cliente_id, 1)
    .reduceByKey(lambda a, b: a + b)            # suma por cliente
)

# Contar sesiones (LOGIN) por cliente
sesiones_rdd = (
    logs_rdd
    .filter(lambda line: "LOGIN" in line and "SISTEMA" not in line)
    .map(lambda line: line.split())
    .map(lambda parts: (parts[2], 1))
    .reduceByKey(lambda a, b: a + b)
)

print(f"  → Clientes con compras detectadas en logs: {compras_rdd.count()}")

# Convertir RDD a DataFrame para poder usar en joins
compras_log_df = spark.createDataFrame(
    compras_rdd, schema=["cliente_id", "compras_en_log"]
)
sesiones_log_df = spark.createDataFrame(
    sesiones_rdd, schema=["cliente_id", "sesiones_log"]
)

# ── 5. LECTURA XML: inventario ────────────────────────────────────────────────
print("\nParsing inventario.xml ...")
xml_content = sc.wholeTextFiles(f"{BASE_PATH}/inventario.xml").collect()[0][1]
root = ET.fromstring(xml_content)

inventario_rows = []
for juego_elem in root.findall("juego"):
    juego_id = juego_elem.get("id")
    titulo   = juego_elem.find("titulo").text
    for plat in juego_elem.find("plataformas").findall("plataforma"):
        inventario_rows.append({
            "juego_id":  juego_id,
            "titulo_inv": titulo,
            "plataforma": plat.get("nombre"),
            "stock":      int(plat.find("stock").text),
            "precio_inv": float(plat.find("precio").text),
            "umbral":     int(plat.find("umbral_reposicion").text)
        })

inventario_df = spark.createDataFrame(inventario_rows)
print(f"  → {inventario_df.count()} combinaciones juego/plataforma en inventario")

# ── 6. LIMPIEZA Y TRANSFORMACIÓN ──────────────────────────────────────────────
print("\nLimpiando y transformando datos ...")

# Ventas: agregar columnas derivadas y calcular total
ventas_clean = (
    ventas_df
    .withColumn("fecha",    F.to_date("fecha", "yyyy-MM-dd"))
    .withColumn("mes",      F.month("fecha"))
    .withColumn("anio",     F.year("fecha"))
    .withColumn("trimestre",F.quarter("fecha"))
    .withColumn("total",    F.round(F.col("precio") * F.col("cantidad"), 2))
    .filter(F.col("precio") > 0)           # eliminar filas con precio inválido
    .dropDuplicates(["venta_id"])
)

# Clientes: normalizar texto
clientes_clean = (
    clientes_df
    .withColumn("nivel_membresia", F.trim(F.col("nivel_membresia")))
    .withColumn("distrito",        F.trim(F.col("distrito")))
    .dropDuplicates(["cliente_id"])
)

# Catálogo: explotar plataformas (array → filas) para join con ventas
catalogo_exploded = (
    catalogo_df
    .withColumn("plataforma", F.explode("plataformas"))
    .select("juego_id", "titulo", "categoria", "plataforma",
            "precio_base", "desarrollador", "anio_lanzamiento", "pegi", "tags")
)

# Reseñas: calcular puntuación promedio por juego
puntuacion_promedio = (
    resenas_df
    .groupBy("juego_id")
    .agg(
        F.round(F.avg("puntuacion"), 2).alias("puntuacion_promedio"),
        F.count("resena_id").alias("total_resenas"),
        F.sum("util_votos").alias("total_votos_util")
    )
)

# ── 7. INTEGRACIÓN (JOIN) DE FUENTES ──────────────────────────────────────────
print("Integrando fuentes de datos ...")

ventas_enriquecidas = (
    ventas_clean
    .join(
        clientes_clean.select(
            "cliente_id", "nombre", "edad", "genero", "distrito", "nivel_membresia"
        ),
        on="cliente_id",
        how="left"
    )
    .join(
        catalogo_exploded.select(
            "juego_id", "plataforma", "titulo", "categoria",
            "desarrollador", "anio_lanzamiento", "tags"
        ),
        on=["juego_id", "plataforma"],
        how="left"
    )
    .join(puntuacion_promedio, on="juego_id", how="left")
)

print(f"  → Dataset integrado: {ventas_enriquecidas.count()} registros")

# Mostrar muestra del dataset integrado
print("\nMuestra del dataset integrado:")
ventas_enriquecidas.select(
    "venta_id", "fecha", "nombre", "titulo", "categoria",
    "plataforma", "total", "nivel_membresia"
).show(10, truncate=False)

# ── 8. GUARDAR RESULTADOS PROCESADOS ─────────────────────────────────────────
print("\nGuardando resultados procesados en Parquet ...")

ventas_enriquecidas.write.mode("overwrite").parquet(f"{OUT_PATH}/ventas_enriquecidas")
inventario_df.write.mode("overwrite").parquet(f"{OUT_PATH}/inventario")
puntuacion_promedio.write.mode("overwrite").parquet(f"{OUT_PATH}/puntuaciones")

# Guardar actividad de logs como CSV para referencia
compras_log_df.write.mode("overwrite").csv(f"{OUT_PATH}/actividad_logs", header=True)

print("  ✓ ventas_enriquecidas.parquet guardado")
print("  ✓ inventario.parquet guardado")
print("  ✓ puntuaciones.parquet guardado")
print("  ✓ actividad_logs.csv guardado")

print("\n[ETL completado exitosamente]\n")
spark.stop()
