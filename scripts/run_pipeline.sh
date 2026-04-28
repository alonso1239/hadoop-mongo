#!/bin/bash
# run_pipeline.sh — Orquestador del pipeline Big Data completo de GameInsight.
# Levanta los contenedores, espera que los servicios estén listos,
# configura HDFS, instala dependencias Python y ejecuta los 3 jobs de Spark.

set -e

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║       GAMEINISGHT — PIPELINE BIG DATA COMPLETO          ║"
echo "║   Hadoop + Spark + MongoDB · GameZone Lima 2024          ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# ── PASO 1: Levantar contenedores ─────────────────────────────────────────────
echo "[ PASO 1 ] Levantando contenedores Docker..."
docker-compose up -d
echo "  ✓ Contenedores iniciados"

# ── PASO 2: Esperar que los servicios estén listos ────────────────────────────
echo ""
echo "[ PASO 2 ] Esperando que los servicios estén disponibles..."

echo -n "  Esperando HDFS NameNode"
until docker exec gamestore-namenode hdfs dfs -ls / > /dev/null 2>&1; do
  echo -n "."
  sleep 5
done
echo " ✓"

echo -n "  Esperando MongoDB"
until docker exec gamestore-mongodb mongosh --eval "db.runCommand({ping:1})" > /dev/null 2>&1; do
  echo -n "."
  sleep 3
done
echo " ✓"

echo -n "  Esperando Spark Master"
until curl -s http://localhost:8080 > /dev/null 2>&1; do
  echo -n "."
  sleep 3
done
echo " ✓"

# ── PASO 3: Configurar HDFS ───────────────────────────────────────────────────
echo ""
echo "[ PASO 3 ] Configurando HDFS y subiendo archivos raw..."
bash ./scripts/setup_hdfs.sh

# ── PASO 4: Instalar dependencias Python en Spark ────────────────────────────
echo ""
echo "[ PASO 4 ] Instalando dependencias Python en Spark Master..."
docker exec gamestore-spark-master pip install pymongo pandas --quiet
echo "  ✓ pymongo y pandas instalados"

# ── PASO 5: ETL — Lectura, limpieza e integración ────────────────────────────
echo ""
echo "[ PASO 5 ] Ejecutando ETL (01_etl_ventas.py)..."
echo "  Lee CSV/JSON/TXT/XML → limpia → integra → guarda Parquet"
docker exec gamestore-spark-master spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 512m \
  --driver-memory 512m \
  /opt/spark-apps/01_etl_ventas.py
echo "  ✓ ETL completado"

# ── PASO 6: Análisis con Spark SQL ────────────────────────────────────────────
echo ""
echo "[ PASO 6 ] Ejecutando análisis SQL (02_analisis_sql.py)..."
echo "  Responde las 4 preguntas de negocio + análisis bonus"
docker exec gamestore-spark-master spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 512m \
  --driver-memory 512m \
  /opt/spark-apps/02_analisis_sql.py
echo "  ✓ Análisis SQL completado"

# ── PASO 7: Carga a MongoDB ───────────────────────────────────────────────────
echo ""
echo "[ PASO 7 ] Cargando resultados en MongoDB (03_carga_mongodb.py)..."
docker exec gamestore-spark-master spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 512m \
  --driver-memory 512m \
  /opt/spark-apps/03_carga_mongodb.py
echo "  ✓ Carga a MongoDB completada"

# ── PASO 8: Verificación final ────────────────────────────────────────────────
echo ""
echo "[ PASO 8 ] Verificación final en MongoDB..."
docker exec gamestore-mongodb mongosh gamestore_db --eval "
  print('');
  print('=== COLECCIONES EN gamestore_db ===');
  db.getCollectionNames().forEach(function(col) {
    print('  ' + col + ': ' + db[col].countDocuments() + ' documentos');
  });
  print('');
  print('=== RESUMEN EJECUTIVO ===');
  var res = db.analisis_plataformas.find({}, {plataforma:1, num_ventas:1, cuota_mercado_pct:1, _id:0}).sort({num_ventas:-1});
  print('Ventas por plataforma:');
  res.forEach(function(r){ print('  ' + r.plataforma + ': ' + r.num_ventas + ' ventas (' + r.cuota_mercado_pct + '%)'); });
"

# ── RESUMEN DE ACCESO ─────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║               PIPELINE COMPLETADO                       ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  HDFS Web UI      → http://localhost:9870              ║"
echo "║  Spark Master UI  → http://localhost:8080              ║"
echo "║  Spark Worker UI  → http://localhost:8081              ║"
echo "║  Mongo Express    → http://localhost:8082              ║"
echo "║                     usuario: admin / admin123          ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
