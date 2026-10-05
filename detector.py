#!/usr/bin/env python3
"""
detector.py - CASO A: Monitoreo de flota de vehiculos (topic 'flota')
Kafka + Spark Structured Streaming

Reglas implementadas:
  1. EXCESO DE VELOCIDAD SOSTENIDO  -> agregacion con ventana de tiempo (nativa de Spark)
  2. CAIDA ANOMALA DE COMBUSTIBLE   -> foreachBatch + estado (lectura anterior por vehiculo)
  3. VEHICULO EN SILENCIO           -> foreachBatch + estado (ultima emision por vehiculo)

Cada alerta se imprime en consola y se guarda en alertas_log.csv (evidencia para el informe).

Ejecucion (desde la carpeta del proyecto, en PowerShell):
  docker compose exec spark /opt/spark/bin/spark-submit --master "local[4]" ^
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /app/detector.py
"""
import csv
import os
import shutil
import threading
from collections import Counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType)

# ----------------------------------------------------------------------------
# PARAMETROS (ajustables; justificarlos en el informe)
# ----------------------------------------------------------------------------
KAFKA_SERVER = "kafka:29092"      # listener INTERNO (Spark corre en un contenedor)
TOPIC = "flota"

UMBRAL_VELOCIDAD = 100            # km/h: por encima de esto es "exceso"
MIN_LECTURAS = 3                  # lecturas rapidas dentro de la ventana para considerarlo sostenido
VENTANA = "10 seconds"            # ventana de tiempo de la regla 1 (el productor emite 1 lectura/seg por vehiculo)
WATERMARK = "20 seconds"          # tolerancia a datos tardios (permite a Spark liberar estado)

UMBRAL_CAIDA = 1.0                # puntos de combustible perdidos entre 2 lecturas (normal: 0.05-0.2; robo: 2.5-4.0)
SILENCIO_SEG = 10                 # segundos sin emitir para declarar a un vehiculo "en silencio"

LOG_CSV = "/app/alertas_log.csv"
CHECKPOINTS = "/tmp/chk_flota"

# ----------------------------------------------------------------------------
# REGISTRO DE ALERTAS (consola + CSV + contador por vehiculo)
# ----------------------------------------------------------------------------
candado = threading.Lock()        # las 2 queries corren en hilos distintos
contador = Counter()              # (regla, vehiculo) -> numero de alertas
ICONOS = {"VELOCIDAD": "[VEL]", "COMBUSTIBLE": "[FUEL]", "SILENCIO": "[SILENCIO]"}


def registrar(regla, vehiculo, detalle, momento):
    """Imprime la alerta y la agrega al CSV."""
    with candado:
        contador[(regla, vehiculo)] += 1
        print(f"{ICONOS[regla]} ALERTA {regla:<11} | {vehiculo} | {momento:%H:%M:%S} | {detalle}",
              flush=True)
        nuevo = not os.path.exists(LOG_CSV)
        with open(LOG_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if nuevo:
                w.writerow(["hora", "regla", "vehiculo", "detalle"])
            w.writerow([f"{momento:%Y-%m-%d %H:%M:%S}", regla, vehiculo, detalle])


# ----------------------------------------------------------------------------
# REGLA 1: EXCESO DE VELOCIDAD SOSTENIDO (ventana de tiempo nativa)
# ----------------------------------------------------------------------------
ventanas_alertadas = set()        # (vehiculo, inicio_ventana) ya alertadas -> evita repetir en modo 'update'


def regla_velocidad(df, batch_id):
    """Recibe, por cada micro-lote, las ventanas cuyo conteo cambio. Alerta la primera vez que llegan a >= 3."""
    for r in df.filter(F.col("lecturas_rapidas") >= MIN_LECTURAS).collect():
        clave = (r["id_vehiculo"], r["window"]["start"])
        if clave in ventanas_alertadas:
            continue
        ventanas_alertadas.add(clave)
        ini, fin = r["window"]["start"], r["window"]["end"]
        registrar("VELOCIDAD", r["id_vehiculo"],
                  f"{r['lecturas_rapidas']} lecturas > {UMBRAL_VELOCIDAD} km/h en ventana "
                  f"{ini:%H:%M:%S}-{fin:%H:%M:%S} (max {r['vel_max']} km/h)",
                  ini)


# ----------------------------------------------------------------------------
# REGLAS 2 y 3: foreachBatch con estado en el driver
#   Spark no permite 'lag' (comparar con la lectura anterior) sobre un stream,
#   asi que cada micro-lote llega como DataFrame normal y mantenemos el estado
#   (ultima lectura / ultima emision por vehiculo) en diccionarios del driver.
# ----------------------------------------------------------------------------
ultimo_combustible = {}           # id_vehiculo -> combustible de la lectura anterior
ultima_emision = {}               # id_vehiculo -> timestamp de su ultimo evento
silenciosos = set()               # vehiculos ya declarados en silencio
reloj_stream = None               # "hora" del stream = timestamp mas reciente visto en cualquier vehiculo


def regla_combustible_y_silencio(df, batch_id):
    global reloj_stream
    # Orden exacto de llegada (el topic tiene 1 particion -> 'offset' es un orden total)
    filas = df.orderBy("offset").collect()
    if not filas:
        return

    for f in filas:
        v, comb, ts = f["id_vehiculo"], f["combustible"], f["ts"]

        # --- Regla 2: comparar con la lectura anterior del MISMO vehiculo
        previo = ultimo_combustible.get(v)
        if previo is not None:
            caida = previo - comb
            if caida >= UMBRAL_CAIDA:     # si es negativa (rellenado a 100) no alerta
                registrar("COMBUSTIBLE", v,
                          f"cayo {caida:.1f} puntos en una lectura ({previo:.1f} -> {comb:.1f}); "
                          f"normal <= 0.3 | posible robo/fuga", ts)
        ultimo_combustible[v] = comb

        # --- Regla 3 (parte A): registrar ultima emision
        ultima_emision[v] = max(ts, ultima_emision.get(v, ts))
        if v in silenciosos:              # volvio a emitir
            silenciosos.discard(v)
            print(f"[OK] {v} volvio a emitir a las {ts:%H:%M:%S}", flush=True)

    # --- Regla 3 (parte B): comparar la ultima emision de cada vehiculo contra el reloj del stream
    max_lote = max(f["ts"] for f in filas)
    reloj_stream = max_lote if reloj_stream is None else max(reloj_stream, max_lote)
    for v, ultimo_ts in ultima_emision.items():
        sin_emitir = (reloj_stream - ultimo_ts).total_seconds()
        if v not in silenciosos and sin_emitir > SILENCIO_SEG:
            silenciosos.add(v)
            registrar("SILENCIO", v,
                      f"sin emitir hace {sin_emitir:.0f} s (ultimo dato {ultimo_ts:%H:%M:%S}) | "
                      f"posible falla/accidente", reloj_stream)


# ----------------------------------------------------------------------------
# PIPELINE: Kafka -> parseo JSON -> reglas
# ----------------------------------------------------------------------------
def main():
    shutil.rmtree(CHECKPOINTS, ignore_errors=True)    # arrancar siempre limpio

    spark = (SparkSession.builder
             .appName("DetectorFlota")
             .config("spark.sql.shuffle.partitions", "4")          # por defecto 200: excesivo para este volumen
             .config("spark.sql.session.timeZone", "America/Bogota")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    esquema = StructType([
        StructField("id_vehiculo", StringType()),
        StructField("velocidad", IntegerType()),
        StructField("combustible", DoubleType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
    ])

    crudo = (spark.readStream.format("kafka")
             .option("kafka.bootstrap.servers", KAFKA_SERVER)
             .option("subscribe", TOPIC)
             .option("startingOffsets", "latest")      # solo datos en vivo
             .load())

    # 'timestamp' lo pone Kafka al recibir el mensaje -> lo usamos como tiempo del evento
    eventos = (crudo
               .select(F.from_json(F.col("value").cast("string"), esquema).alias("e"),
                       F.col("timestamp").alias("ts"),
                       F.col("offset"))
               .select("e.*", "ts", "offset"))

    # Query 1: regla de velocidad (ventana + watermark, modo update)
    q1 = (eventos
          .filter(F.col("velocidad") > UMBRAL_VELOCIDAD)
          .withWatermark("ts", WATERMARK)
          .groupBy(F.window("ts", VENTANA), "id_vehiculo")
          .agg(F.count("*").alias("lecturas_rapidas"),
               F.max("velocidad").alias("vel_max"))
          .writeStream
          .outputMode("update")
          .foreachBatch(regla_velocidad)
          .option("checkpointLocation", f"{CHECKPOINTS}/velocidad")
          .trigger(processingTime="2 seconds")
          .start())

    # Query 2: combustible + silencio (foreachBatch con estado)
    q2 = (eventos
          .writeStream
          .foreachBatch(regla_combustible_y_silencio)
          .option("checkpointLocation", f"{CHECKPOINTS}/combustible_silencio")
          .trigger(processingTime="2 seconds")
          .start())

    print("=" * 70)
    print(" Detector de flota ACTIVO. Esperando eventos del topic 'flota'...")
    print(" (arranca ahora el productor en otra terminal) | Ctrl+C para parar")
    print("=" * 70, flush=True)

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        pass
    finally:
        for q in (q1, q2):
            try:
                q.stop()
            except Exception:
                pass
        print("\n" + "=" * 70)
        print(" RESUMEN DE ALERTAS")
        for (regla, veh), n in sorted(contador.items(), key=lambda x: (-x[1], x[0])):
            print(f"   {regla:<12} {veh}: {n}")
        print("=" * 70)
        spark.stop()


if __name__ == "__main__":
    main()
