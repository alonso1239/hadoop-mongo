package certus.edu;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.ArrayList;

/**
 * GameInsight — Orquestador del Pipeline Big Data
 *
 * Al ejecutar este Main desde IntelliJ, se lanza automáticamente
 * el ecosistema completo: Hadoop + Spark + MongoDB vía Docker,
 * seguido de los tres jobs de Spark (ETL, SQL, carga MongoDB).
 */
public class Main {

    private static final String ROOT = Paths.get("").toAbsolutePath().toString();

    public static void main(String[] args) throws Exception {
        printBanner();
        log("Directorio del proyecto: " + ROOT);

        step(1, "Verificando que Docker esté disponible");
        checkDocker();

        step(2, "Limpiando contenedores anteriores");
        run(List.of("docker", "compose", "down", "--remove-orphans"), ROOT, true);

        step(3, "Levantando contenedores Docker");
        run(List.of("docker", "compose", "up", "-d"), ROOT, true);

        step(4, "Esperando que los servicios estén listos");
        waitForHDFS();
        waitForMongo();
        waitForSpark();

        step(5, "Configurando HDFS — salir de SafeMode y crear directorios");
        // Forzar salida de SafeMode (HDFS lo activa al arrancar)
        run(List.of("docker", "exec", "gamestore-namenode",
                "hdfs", "dfsadmin", "-safemode", "leave"), ROOT, true);
        Thread.sleep(2000);
        run(List.of("docker", "exec", "gamestore-namenode",
                "hdfs", "dfs", "-mkdir", "-p", "/gamestore/raw"), ROOT, true);
        run(List.of("docker", "exec", "gamestore-namenode",
                "hdfs", "dfs", "-mkdir", "-p", "/gamestore/processed"), ROOT, true);

        step(6, "Subiendo archivos raw a HDFS");
        String[] archivos = {
            "ventas.csv", "clientes.csv", "catalogo_juegos.json",
            "resenas.json", "logs_tienda.txt", "inventario.xml"
        };
        for (String archivo : archivos) {
            // docker cp copia del host al contenedor sin depender de volume mounts
            run(List.of("docker", "cp",
                    "data" + File.separator + "raw" + File.separator + archivo,
                    "gamestore-namenode:/tmp/" + archivo), ROOT, true);
            // luego HDFS lo lee desde /tmp (path nativo del contenedor)
            run(List.of("docker", "exec", "gamestore-namenode",
                    "hdfs", "dfs", "-put", "-f",
                    "/tmp/" + archivo,
                    "/gamestore/raw/"), ROOT, true);
            log("  ✓ " + archivo + " → HDFS /gamestore/raw/");
        }

        step(7, "Instalando dependencias Python en Spark");
        run(List.of("docker", "exec", "gamestore-spark-master",
                "python3", "-m", "pip", "install",
                "pymongo", "pandas", "--quiet"), ROOT, true);

        step(8, "Ejecutando ETL — Lectura, limpieza e integración (01_etl_ventas.py)");
        sparkSubmit("01_etl_ventas.py");

        step(9, "Ejecutando análisis Spark SQL (02_analisis_sql.py)");
        sparkSubmit("02_analisis_sql.py");

        step(10, "Cargando resultados en MongoDB (03_carga_mongodb.py)");
        sparkSubmit("03_carga_mongodb.py");

        step(11, "Verificando colecciones en MongoDB");
        run(List.of("docker", "exec", "gamestore-mongodb",
                "mongosh", "gamestore_db", "--eval",
                "db.getCollectionNames().forEach(c => print('  ' + c + ': ' + db[c].countDocuments() + ' docs'))"),
                ROOT, true);

        printFinalInfo();
    }

    // ── PASOS DEL PIPELINE ────────────────────────────────────────────────────

    private static void checkDocker() throws Exception {
        try {
            int exit = new ProcessBuilder("docker", "info")
                    .redirectErrorStream(true)
                    .start()
                    .waitFor();
            if (exit != 0) throw new RuntimeException();
            log("  ✓ Docker disponible");
        } catch (Exception e) {
            error("Docker no está disponible o no está corriendo.");
            error("Abre Docker Desktop y vuelve a ejecutar.");
            System.exit(1);
        }
    }

    private static void waitForHDFS() throws Exception {
        log("  Esperando HDFS NameNode...");
        for (int i = 0; i < 30; i++) {
            int exit = new ProcessBuilder(
                    "docker", "exec", "gamestore-namenode",
                    "hdfs", "dfs", "-ls", "/")
                    .redirectErrorStream(true)
                    .start()
                    .waitFor();
            if (exit == 0) { log("  ✓ HDFS listo"); return; }
            Thread.sleep(5000);
        }
        error("Timeout esperando HDFS. Revisa el contenedor gamestore-namenode.");
        System.exit(1);
    }

    private static void waitForMongo() throws Exception {
        log("  Esperando MongoDB...");
        for (int i = 0; i < 20; i++) {
            int exit = new ProcessBuilder(
                    "docker", "exec", "gamestore-mongodb",
                    "mongosh", "--eval", "db.runCommand({ping:1})")
                    .redirectErrorStream(true)
                    .start()
                    .waitFor();
            if (exit == 0) { log("  ✓ MongoDB listo"); return; }
            Thread.sleep(3000);
        }
        error("Timeout esperando MongoDB.");
        System.exit(1);
    }

    private static void waitForSpark() throws Exception {
        log("  Esperando Spark Master...");
        for (int i = 0; i < 20; i++) {
            try {
                HttpURLConnection conn = (HttpURLConnection)
                        new URL("http://localhost:8080").openConnection();
                conn.setConnectTimeout(2000);
                if (conn.getResponseCode() == 200) {
                    log("  ✓ Spark Master listo");
                    return;
                }
            } catch (Exception ignored) {}
            Thread.sleep(3000);
        }
        error("Timeout esperando Spark. Revisa el contenedor gamestore-spark-master.");
        System.exit(1);
    }

    private static void sparkSubmit(String script) throws Exception {
        run(List.of(
                "docker", "exec", "gamestore-spark-master",
                "/opt/spark/bin/spark-submit",
                "--master", "spark://spark-master:7077",
                "--executor-memory", "512m",
                "--driver-memory", "512m",
                "/opt/spark-apps/" + script
        ), ROOT, true);
    }

    // ── UTILIDADES ────────────────────────────────────────────────────────────

    private static void run(List<String> cmd, String workDir, boolean showOutput)
            throws Exception {
        ProcessBuilder pb = new ProcessBuilder(cmd)
                .directory(new File(workDir))
                .redirectErrorStream(true);

        Process proc = pb.start();

        if (showOutput) {
            try (BufferedReader reader =
                         new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("  | " + line);
                }
            }
        } else {
            proc.getInputStream().transferTo(OutputStream.nullOutputStream());
        }

        int exit = proc.waitFor();
        if (exit != 0) {
            throw new RuntimeException(
                "Comando falló (exit=" + exit + "): " + String.join(" ", cmd) +
                "\n  → Revisa la salida de arriba para ver el error exacto."
            );
        }
    }

    private static void step(int n, String desc) {
        System.out.println();
        System.out.println("┌─────────────────────────────────────────────────────");
        System.out.printf( "│  PASO %2d │ %s%n", n, desc);
        System.out.println("└─────────────────────────────────────────────────────");
    }

    private static void log(String msg)   { System.out.println(msg); }
    private static void error(String msg) { System.err.println("[ERROR] " + msg); }

    private static void printBanner() {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════╗");
        System.out.println("║        GAMEINISGHT — PIPELINE BIG DATA                  ║");
        System.out.println("║   Análisis de Ventas · Tienda de Videojuegos Lima       ║");
        System.out.println("║   Hadoop  ·  Apache Spark 3.5  ·  MongoDB 7.0           ║");
        System.out.println("╚══════════════════════════════════════════════════════════╝");
    }

    private static void printFinalInfo() {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════╗");
        System.out.println("║            PIPELINE COMPLETADO EXITOSAMENTE             ║");
        System.out.println("╠══════════════════════════════════════════════════════════╣");
        System.out.println("║  HDFS Web UI     →  http://localhost:19870              ║");
        System.out.println("║  Spark Master    →  http://localhost:8080               ║");
        System.out.println("║  Spark Worker    →  http://localhost:8081               ║");
        System.out.println("║  Mongo Express   →  http://localhost:8082               ║");
        System.out.println("║                     usuario: admin / admin123           ║");
        System.out.println("╚══════════════════════════════════════════════════════════╝");
        System.out.println();
    }
}
