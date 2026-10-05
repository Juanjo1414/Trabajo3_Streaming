#!/usr/bin/env python3
"""
resumen.py - Resume alertas_log.csv para el informe (sin dependencias, corre en Windows).
Uso:  python resumen.py
"""
import csv
from collections import Counter, defaultdict
from datetime import datetime

alertas = list(csv.DictReader(open("alertas_log.csv", encoding="utf-8")))
if not alertas:
    raise SystemExit("alertas_log.csv esta vacio: corre primero el detector.")

por_regla = Counter(a["regla"] for a in alertas)
por_vehiculo = Counter(a["vehiculo"] for a in alertas)
matriz = defaultdict(Counter)
for a in alertas:
    matriz[a["vehiculo"]][a["regla"]] += 1

reglas = ["VELOCIDAD", "COMBUSTIBLE", "SILENCIO"]
print("\nTOTAL DE ALERTAS:", len(alertas))
print("\nPor regla:")
for r in reglas:
    print(f"  {r:<12} {por_regla.get(r, 0)}")

print("\nPor vehiculo (ordenado):")
print(f"  {'Vehiculo':<10}" + "".join(f"{r:<13}" for r in reglas) + "TOTAL")
for v, total in por_vehiculo.most_common():
    print(f"  {v:<10}" + "".join(f"{matriz[v][r]:<13}" for r in reglas) + str(total))

print(f"\nVehiculo con mas alertas: {por_vehiculo.most_common(1)[0][0]}")

# Primera alerta de cada regla (util para hablar de latencia)
print("\nPrimera alerta de cada regla:")
for r in reglas:
    for a in alertas:
        if a["regla"] == r:
            print(f"  {r:<12} {a['hora']}  {a['vehiculo']}  {a['detalle']}")
            break
