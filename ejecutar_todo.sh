#!/usr/bin/env bash
# ============================================================
# Trabajo 3 - Caso A (flota de vehiculos)
# Ejecuta TODO el pipeline de punta a punta y valida el resultado:
#   Kafka (Docker) -> topic 'flota' -> detector Spark -> alertas -> resumen
#
# Uso (desde esta carpeta):
#   bash ejecutar_todo.sh            # corrida de 120 s (por defecto)
#   bash ejecutar_todo.sh 180        # corrida de 180 s
#   APAGAR=1 bash ejecutar_todo.sh   # apaga Docker al terminar
#
# Requisitos: Docker (corriendo) y Python 3. kafka-python se instala solo.
# La primera vez descarga imagenes de Docker y el conector de Spark
# (puede tardar varios minutos); las siguientes corridas son rapidas.
# ============================================================

export MSYS_NO_PATHCONV=1   # Git Bash (Windows): no convertir rutas como /opt/... en C:/...
export PYTHONUTF8=1
cd "$(dirname "$0")" || exit 1

DURACION="${1:-${DURACION:-120}}"
TOPIC="flota"
LOGS="logs"
DC="docker compose"

paso()  { echo; echo "==> $*"; }
ok()    { echo "    OK  $*"; }
aviso() { echo "    !!  $*"; }
fallo() { echo; echo "    ERROR: $*" >&2; exit 1; }

PROD_PID=""
DET_PID=""

# ------------------------------------------------------------
# Limpieza: nunca dejar procesos sueltos (tambien si se pulsa Ctrl+C)
# ------------------------------------------------------------
detener_productor() {
  if [ -n "$PROD_PID" ] && kill -0 "$PROD_PID" 2>/dev/null; then
    kill "$PROD_PID" 2>/dev/null
    sleep 1
    if kill -0 "$PROD_PID" 2>/dev/null; then kill -9 "$PROD_PID" 2>/dev/null; fi
  fi
  PROD_PID=""
}

detener_detector() {
  [ -z "$DET_PID" ] && return 0
  if kill -0 "$DET_PID" 2>/dev/null; then
    # Equivale al Ctrl+C del detector dentro del contenedor (asi imprime su resumen final)
    $DC exec -T spark sh -c 'for p in /proc/[0-9]*; do if tr "\0" " " < $p/cmdline 2>/dev/null | grep -q "^python.*[d]etector.py"; then kill -INT ${p#/proc/}; fi; done' </dev/null >/dev/null 2>&1
    for _ in $(seq 1 20); do
      kill -0 "$DET_PID" 2>/dev/null || break
      sleep 1
    done
    if kill -0 "$DET_PID" 2>/dev/null; then
      aviso "El detector no cerro solo; se reinicia el contenedor spark para detenerlo"
      $DC restart -t 2 spark </dev/null >/dev/null 2>&1
      sleep 2
      kill "$DET_PID" 2>/dev/null
    fi
  fi
  wait "$DET_PID" 2>/dev/null
  DET_PID=""
}

limpiar() { detener_productor; detener_detector; }
trap limpiar EXIT
trap 'echo; echo "Interrumpido por el usuario."; exit 130' INT TERM

mostrar_alertas_nuevas() {
  local total
  total=$(wc -l < "$LOGS/detector.log" | tr -d ' ')
  if [ "$total" -gt "$VISTAS" ]; then
    awk -v desde="$VISTAS" 'NR>desde && (/ALERTA/ || /\[OK\]/)' "$LOGS/detector.log"
    VISTAS=$total
  fi
}

echo "============================================================"
echo " Trabajo 3 - Streaming con Kafka + Spark (Caso A: flota)"
echo " Duracion de la corrida: ${DURACION} s"
echo "============================================================"

# ------------------------------------------------------------
# 1. Verificaciones
# ------------------------------------------------------------
paso "1/8 Verificando requisitos..."
case "$DURACION" in ''|*[!0-9]*) fallo "La duracion debe ser un numero de segundos (ej: bash ejecutar_todo.sh 120)";; esac
if [ "$DURACION" -lt 110 ]; then
  aviso "Con menos de ~110 s puede que V07 aun no alcance a callar y la validacion falle."
fi

for f in docker-compose.yml detector.py productor_flota.py resumen.py; do
  [ -f "$f" ] || fallo "Falta el archivo '$f' en esta carpeta."
done

docker --version >/dev/null 2>&1 || fallo "Docker no esta instalado."
docker info >/dev/null 2>&1 || fallo "Docker no esta corriendo (abra Docker Desktop y reintente)."
$DC version >/dev/null 2>&1 || fallo "Falta 'docker compose'."
ok "Docker y docker compose disponibles"

PY=""
for c in python3 python py; do
  if command -v "$c" >/dev/null 2>&1 && "$c" -c "import sys; sys.exit(0 if sys.version_info[0] == 3 else 1)" >/dev/null 2>&1; then
    PY="$c"; break
  fi
done
[ -n "$PY" ] || fallo "No se encontro Python 3."
if ! "$PY" -c "import kafka" >/dev/null 2>&1; then
  aviso "Instalando kafka-python..."
  "$PY" -m pip install --quiet kafka-python \
    || "$PY" -m pip install --quiet --break-system-packages kafka-python \
    || fallo "No se pudo instalar kafka-python (pruebe: $PY -m pip install kafka-python)"
fi
ok "Python 3 ($PY) con kafka-python"

OTRO=$(docker ps --format '{{.Names}}|{{.Ports}}' 2>/dev/null | grep ':9092->' | cut -d'|' -f1 | grep -v '^kafka-flota$' | head -1)
if [ -n "$OTRO" ]; then
  fallo "El contenedor '$OTRO' esta usando el puerto 9092 (el productor usa localhost:9092 fijo). Detengalo sin borrarlo con: docker stop $OTRO"
fi
ok "Puerto 9092 libre"

mkdir -p "$LOGS"

# ------------------------------------------------------------
# 2. Kafka y Spark en Docker
# ------------------------------------------------------------
paso "2/8 Levantando Kafka y Spark (docker compose up -d)..."
$DC up -d </dev/null || fallo "No se pudo levantar docker compose."

paso "3/8 Esperando a que Kafka este listo..."
KT="/opt/kafka/bin/kafka-topics.sh"
LISTO=0
for _ in $(seq 1 60); do
  if $DC exec -T kafka $KT --bootstrap-server localhost:9092 --list </dev/null >/dev/null 2>&1; then LISTO=1; break; fi
  sleep 2
done
[ "$LISTO" = "1" ] || fallo "Kafka no respondio a tiempo. Revise: docker compose logs kafka"
ok "Kafka responde en localhost:9092"

# ------------------------------------------------------------
# 3. Topic 'flota' con 1 particion (orden total de eventos)
# ------------------------------------------------------------
paso "4/8 Creando el topic '$TOPIC' (1 particion)..."
DESC=$($DC exec -T kafka $KT --bootstrap-server localhost:9092 --describe --topic "$TOPIC" </dev/null 2>/dev/null)
if echo "$DESC" | grep -q "PartitionCount"; then
  if echo "$DESC" | grep -Eq "PartitionCount:[[:space:]]*1([^0-9]|$)"; then
    ok "El topic '$TOPIC' ya existe con 1 particion"
  else
    aviso "El topic '$TOPIC' existe con varias particiones; se recrea con 1 (el orden de eventos lo exige)"
    $DC exec -T kafka $KT --bootstrap-server localhost:9092 --delete --topic "$TOPIC" </dev/null >/dev/null 2>&1
    sleep 3
    DESC=""
  fi
fi
if ! echo "$DESC" | grep -q "PartitionCount"; then
  CREADO=0
  for _ in $(seq 1 10); do
    if $DC exec -T kafka $KT --bootstrap-server localhost:9092 --create --if-not-exists --topic "$TOPIC" --partitions 1 --replication-factor 1 </dev/null >/dev/null 2>&1; then CREADO=1; break; fi
    sleep 2
  done
  [ "$CREADO" = "1" ] || fallo "No se pudo crear el topic '$TOPIC'."
  ok "Topic '$TOPIC' creado"
fi

# ------------------------------------------------------------
# 4. Detector (Spark Structured Streaming) en segundo plano
# ------------------------------------------------------------
paso "5/8 Arrancando el detector de Spark (la primera vez descarga el conector)..."
if [ -f alertas_log.csv ]; then
  RESPALDO="alertas_log_anterior_$(date +%Y%m%d_%H%M%S).csv"
  mv alertas_log.csv "$RESPALDO"
  aviso "El alertas_log.csv anterior se conservo como $RESPALDO"
fi
: > "$LOGS/detector.log"

$DC exec -T spark /opt/spark/bin/spark-submit --master "local[4]" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  /app/detector.py </dev/null > "$LOGS/detector.log" 2>&1 &
DET_PID=$!

ARRANCO=0
for _ in $(seq 1 150); do
  if grep -q "Detector de flota ACTIVO" "$LOGS/detector.log" 2>/dev/null; then ARRANCO=1; break; fi
  if ! kill -0 "$DET_PID" 2>/dev/null; then
    tail -n 25 "$LOGS/detector.log"
    fallo "El detector termino antes de arrancar (log completo en $LOGS/detector.log)."
  fi
  sleep 2
done
if [ "$ARRANCO" != "1" ]; then
  tail -n 25 "$LOGS/detector.log"
  fallo "El detector no arranco a tiempo (log completo en $LOGS/detector.log)."
fi
ok "Detector ACTIVO (esperando eventos)"
sleep 10   # margen para que Spark termine de suscribirse al topic

# ------------------------------------------------------------
# 5. Productor en segundo plano + alertas en vivo
# ------------------------------------------------------------
paso "6/8 Arrancando el productor y mostrando alertas en vivo (${DURACION} s)..."
"$PY" productor_flota.py </dev/null > "$LOGS/productor.log" 2>&1 &
PROD_PID=$!
sleep 3
if ! kill -0 "$PROD_PID" 2>/dev/null; then
  tail -n 15 "$LOGS/productor.log"
  fallo "El productor no arranco (log en $LOGS/productor.log)."
fi
ok "Productor enviando eventos al topic '$TOPIC'"
echo

VISTAS=0
for ((t = 1; t <= DURACION; t++)); do
  sleep 1
  if (( t % 2 == 0 )); then mostrar_alertas_nuevas; fi
  if (( t % 30 == 0 )); then echo "    ... ${t}/${DURACION} s"; fi
  if ! kill -0 "$PROD_PID" 2>/dev/null; then
    aviso "El productor se detuvo antes de tiempo (ver $LOGS/productor.log)"
    break
  fi
done

# ------------------------------------------------------------
# 6. Cierre ordenado: primero el productor, luego el detector
# ------------------------------------------------------------
paso "7/8 Deteniendo productor y detector..."
detener_productor
sleep 6   # el detector termina de procesar lo pendiente
mostrar_alertas_nuevas
detener_detector
mostrar_alertas_nuevas
ok "Pipeline detenido"

if grep -q "RESUMEN DE ALERTAS" "$LOGS/detector.log" 2>/dev/null; then
  echo
  awk '/RESUMEN DE ALERTAS/ {f=1} f' "$LOGS/detector.log" | head -n 15
fi

# ------------------------------------------------------------
# 7. Resumen y validacion automatica contra los patrones sembrados
# ------------------------------------------------------------
paso "8/8 Resumen de alertas (resumen.py)"
"$PY" resumen.py

echo
echo "------------------------------------------------------------"
echo " VALIDACION contra los patrones sembrados por el productor"
echo "------------------------------------------------------------"
"$PY" - <<'PYEOF'
import csv, sys

esperado = {
    ("VELOCIDAD", "V03"): "Regla 1: exceso de velocidad sostenido  -> V03",
    ("COMBUSTIBLE", "V05"): "Regla 2: caida anomala de combustible  -> V05",
    ("SILENCIO", "V07"): "Regla 3: vehiculo en silencio           -> V07",
}
try:
    filas = list(csv.DictReader(open("alertas_log.csv", encoding="utf-8")))
except FileNotFoundError:
    filas = []

encontrado = {(f["regla"], f["vehiculo"]) for f in filas}
todo_ok = True
for clave, texto in esperado.items():
    ok = clave in encontrado
    todo_ok = todo_ok and ok
    print(f"  [{'OK' if ok else 'FALLO'}] {texto}")

extra = sorted(encontrado - set(esperado))
if extra:
    todo_ok = False
    print(f"  [FALLO] Alertas inesperadas (falsos positivos): {extra}")
else:
    print("  [OK]    Sin falsos positivos: ningun otro vehiculo/regla genero alertas")

print()
if todo_ok:
    print(f"  RESULTADO: PIPELINE VALIDADO ({len(filas)} alertas, 3/3 patrones detectados)")
else:
    print("  RESULTADO: FALLO. Si V07 no aparece, repita con mas tiempo: bash ejecutar_todo.sh 180")
sys.exit(0 if todo_ok else 1)
PYEOF
RC=$?

echo
echo "Logs en ./$LOGS/ | Alertas en ./alertas_log.csv"
if [ "${APAGAR:-0}" = "1" ]; then
  paso "Apagando Docker (APAGAR=1)..."
  $DC down </dev/null
else
  echo "Para apagar Docker:  docker compose down"
fi
exit $RC
