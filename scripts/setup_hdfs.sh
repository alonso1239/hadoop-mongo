#!/bin/bash
# setup_hdfs.sh — Crea la estructura de directorios en HDFS y sube los archivos raw.
# Ejecutar DESPUÉS de que el namenode esté listo (ver run_pipeline.sh).

set -e

NAMENODE="gamestore-namenode"
HDFS_BASE="/gamestore"

echo "=========================================="
echo "  CONFIGURACIÓN DE HDFS"
echo "=========================================="

echo ""
echo "[1/3] Creando estructura de directorios en HDFS..."
docker exec $NAMENODE hdfs dfs -mkdir -p $HDFS_BASE/raw
docker exec $NAMENODE hdfs dfs -mkdir -p $HDFS_BASE/processed
docker exec $NAMENODE hdfs dfs -mkdir -p $HDFS_BASE/logs
echo "  ✓ Directorios creados"

echo ""
echo "[2/3] Subiendo archivos raw a HDFS..."

# Los archivos están en /data/raw dentro del contenedor (montado desde ./data)
docker exec $NAMENODE hdfs dfs -put -f /data/raw/ventas.csv          $HDFS_BASE/raw/
docker exec $NAMENODE hdfs dfs -put -f /data/raw/clientes.csv        $HDFS_BASE/raw/
docker exec $NAMENODE hdfs dfs -put -f /data/raw/catalogo_juegos.json $HDFS_BASE/raw/
docker exec $NAMENODE hdfs dfs -put -f /data/raw/resenas.json        $HDFS_BASE/raw/
docker exec $NAMENODE hdfs dfs -put -f /data/raw/logs_tienda.txt     $HDFS_BASE/raw/
docker exec $NAMENODE hdfs dfs -put -f /data/raw/inventario.xml      $HDFS_BASE/raw/
echo "  ✓ 6 archivos subidos a HDFS"

echo ""
echo "[3/3] Verificando archivos en HDFS..."
docker exec $NAMENODE hdfs dfs -ls -h $HDFS_BASE/raw/

echo ""
echo "  HDFS configurado exitosamente."
echo "  Web UI disponible en: http://localhost:9870"
echo "=========================================="
