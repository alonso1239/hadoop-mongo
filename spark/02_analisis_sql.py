"""
GameInsight - Análisis con Spark SQL
Lee el dataset procesado y ejecuta consultas SQL para responder las
4 preguntas de negocio clave de la tienda de videojuegos.

Demuestra: Spark SQL, vistas temporales, Window Functions, GroupBy avanzado.
"""

import os
os.environ["PYSPARK_PYTHON"] = "python3"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("GameInsight-SparkSQL") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

OUT_PATH = "/opt/spark-data/processed"
SQL_PATH = "/opt/spark-data/processed/sql_results"

print("=" * 60)
print("  GAMEINISGHT — ANÁLISIS CON SPARK SQL")
print("=" * 60)

# ── CARGAR DATOS PROCESADOS ───────────────────────────────────────────────────
ventas_df = spark.read.parquet(f"{OUT_PATH}/ventas_enriquecidas")
inventario_df = spark.read.parquet(f"{OUT_PATH}/inventario")

# Registrar vistas temporales para Spark SQL
ventas_df.createOrReplaceTempView("ventas")
inventario_df.createOrReplaceTempView("inventario")

print(f"\nDataset cargado: {ventas_df.count()} registros de ventas\n")

# ═══════════════════════════════════════════════════════════════════════════════
# PREGUNTA 1: ¿Qué juegos se venden más?
# ═══════════════════════════════════════════════════════════════════════════════
print("━" * 60)
print("PREGUNTA 1: Top 10 juegos más vendidos")
print("━" * 60)

top_juegos = spark.sql("""
    SELECT
        juego_id,
        titulo,
        categoria,
        SUM(cantidad)                    AS total_unidades,
        COUNT(venta_id)                  AS num_transacciones,
        ROUND(SUM(total), 2)             AS ingresos_total,
        ROUND(AVG(puntuacion_promedio), 2) AS rating_promedio
    FROM ventas
    WHERE titulo IS NOT NULL
    GROUP BY juego_id, titulo, categoria
    ORDER BY total_unidades DESC
    LIMIT 10
""")

top_juegos.show(truncate=False)
top_juegos.write.mode("overwrite").parquet(f"{SQL_PATH}/top_juegos")

# ═══════════════════════════════════════════════════════════════════════════════
# PREGUNTA 2: ¿Qué categorías prefieren los clientes?
# ═══════════════════════════════════════════════════════════════════════════════
print("━" * 60)
print("PREGUNTA 2: Preferencia de categorías")
print("━" * 60)

categorias = spark.sql("""
    SELECT
        categoria,
        COUNT(venta_id)              AS num_ventas,
        SUM(cantidad)                AS total_unidades,
        ROUND(SUM(total), 2)         AS ingresos_total,
        ROUND(AVG(precio), 2)        AS precio_promedio,
        ROUND(
            COUNT(venta_id) * 100.0 / SUM(COUNT(venta_id)) OVER (), 1
        )                            AS pct_ventas
    FROM ventas
    WHERE categoria IS NOT NULL
    GROUP BY categoria
    ORDER BY ingresos_total DESC
""")

categorias.show(truncate=False)
categorias.write.mode("overwrite").parquet(f"{SQL_PATH}/categorias")

# ═══════════════════════════════════════════════════════════════════════════════
# PREGUNTA 3: ¿Qué plataformas son más populares?
# ═══════════════════════════════════════════════════════════════════════════════
print("━" * 60)
print("PREGUNTA 3: Popularidad por plataforma")
print("━" * 60)

plataformas = spark.sql("""
    SELECT
        v.plataforma,
        COUNT(v.venta_id)              AS num_ventas,
        SUM(v.cantidad)                AS total_unidades,
        ROUND(SUM(v.total), 2)         AS ingresos_total,
        ROUND(AVG(v.precio), 2)        AS ticket_promedio,
        ROUND(
            COUNT(v.venta_id) * 100.0 / SUM(COUNT(v.venta_id)) OVER (), 1
        )                              AS cuota_mercado_pct,
        COALESCE(SUM(i.stock), 0)      AS stock_actual
    FROM ventas v
    LEFT JOIN (
        SELECT plataforma, SUM(stock) AS stock
        FROM inventario
        GROUP BY plataforma
    ) i ON v.plataforma = i.plataforma
    GROUP BY v.plataforma
    ORDER BY ingresos_total DESC
""")

plataformas.show(truncate=False)
plataformas.write.mode("overwrite").parquet(f"{SQL_PATH}/plataformas")

# ═══════════════════════════════════════════════════════════════════════════════
# PREGUNTA 4: ¿Qué meses tienen mayores ventas?
# ═══════════════════════════════════════════════════════════════════════════════
print("━" * 60)
print("PREGUNTA 4: Tendencia mensual de ventas 2024")
print("━" * 60)

meses_map = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",
             7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"}

ventas_mensuales = spark.sql("""
    SELECT
        anio,
        mes,
        COUNT(venta_id)          AS num_transacciones,
        SUM(cantidad)            AS total_unidades,
        ROUND(SUM(total), 2)     AS ingresos_mes,
        ROUND(AVG(total), 2)     AS ticket_promedio,
        COUNT(DISTINCT cliente_id) AS clientes_unicos
    FROM ventas
    GROUP BY anio, mes
    ORDER BY anio, mes
""")

ventas_mensuales.show(12, truncate=False)
ventas_mensuales.write.mode("overwrite").parquet(f"{SQL_PATH}/ventas_mensuales")

# ═══════════════════════════════════════════════════════════════════════════════
# ANÁLISIS ADICIONAL: Top 10 clientes por gasto total
# ═══════════════════════════════════════════════════════════════════════════════
print("━" * 60)
print("BONUS: Top 10 clientes por gasto total")
print("━" * 60)

top_clientes = spark.sql("""
    SELECT
        cliente_id,
        nombre,
        nivel_membresia,
        distrito,
        COUNT(venta_id)          AS num_compras,
        ROUND(SUM(total), 2)     AS gasto_total,
        ROUND(AVG(total), 2)     AS ticket_promedio,
        MAX(fecha)               AS ultima_compra
    FROM ventas
    WHERE nombre IS NOT NULL
    GROUP BY cliente_id, nombre, nivel_membresia, distrito
    ORDER BY gasto_total DESC
    LIMIT 10
""")

top_clientes.show(truncate=False)
top_clientes.write.mode("overwrite").parquet(f"{SQL_PATH}/top_clientes")

# ═══════════════════════════════════════════════════════════════════════════════
# ANÁLISIS ADICIONAL: Método de pago más usado por nivel de membresía
# ═══════════════════════════════════════════════════════════════════════════════
print("━" * 60)
print("BONUS: Método de pago preferido por nivel de membresía")
print("━" * 60)

pago_membresia = spark.sql("""
    SELECT
        nivel_membresia,
        metodo_pago,
        COUNT(*) AS frecuencia
    FROM ventas
    WHERE nivel_membresia IS NOT NULL
    GROUP BY nivel_membresia, metodo_pago
    ORDER BY nivel_membresia, frecuencia DESC
""")

pago_membresia.show(truncate=False)
pago_membresia.write.mode("overwrite").parquet(f"{SQL_PATH}/pago_membresia")

# ═══════════════════════════════════════════════════════════════════════════════
# ANÁLISIS ADICIONAL: Alertas de stock bajo
# ═══════════════════════════════════════════════════════════════════════════════
print("━" * 60)
print("BONUS: Juegos con stock bajo (requieren reposición)")
print("━" * 60)

stock_bajo = spark.sql("""
    SELECT
        juego_id,
        titulo_inv   AS titulo,
        plataforma,
        stock,
        umbral,
        precio_inv   AS precio
    FROM inventario
    WHERE stock <= umbral
    ORDER BY stock ASC
""")

stock_bajo.show(truncate=False)
stock_bajo.write.mode("overwrite").parquet(f"{SQL_PATH}/stock_bajo")

# ── RESUMEN EJECUTIVO ─────────────────────────────────────────────────────────
print("=" * 60)
print("  RESUMEN EJECUTIVO — GAMEZONE LIMA 2024")
print("=" * 60)

resumen = spark.sql("""
    SELECT
        COUNT(venta_id)             AS total_ventas,
        SUM(cantidad)               AS total_unidades_vendidas,
        ROUND(SUM(total), 2)        AS ingresos_totales_soles,
        ROUND(AVG(total), 2)        AS ticket_promedio,
        COUNT(DISTINCT cliente_id)  AS clientes_activos,
        COUNT(DISTINCT juego_id)    AS juegos_vendidos
    FROM ventas
""")

resumen.show(truncate=False)
resumen.write.mode("overwrite").parquet(f"{SQL_PATH}/resumen_ejecutivo")

print("\n[Análisis SQL completado exitosamente]\n")
spark.stop()
