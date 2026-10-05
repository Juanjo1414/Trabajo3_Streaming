#!/usr/bin/env python3
"""
productor_precios.py - CASO C: Monitoreo de precios de acciones/cripto.
Emite precios al topic 'precios'. Cada símbolo hace un 'random walk' con
episodios de volatilidad sembrados.
Patrones a detectar:
  - media móvil del precio por ventana
  - volatilidad: cambio brusco de precio en una ventana
  - cruce de medias (media corta cruza la larga)

Requiere:  pip install kafka-python
Uso:       python3 productor_precios.py
Detener:   Ctrl+C
"""
import json, time, random, math
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

SIMBOLOS = {"BTC": 60000.0, "ETH": 3000.0, "AAPL": 190.0, "TSLA": 250.0}
TOPIC = "precios"
print(f"Enviando precios al topic '{TOPIC}'. Ctrl+C para parar.")
print("Cada símbolo hace random walk; se siembran episodios de volatilidad.")

i = 0
try:
    while True:
        i += 1
        for sim, precio in SIMBOLOS.items():
            cambio = random.gauss(0, 0.004)   
            if random.random() < 0.05:
                cambio += random.choice([-1, 1]) * random.uniform(0.02, 0.05)  
            precio = max(1.0, precio * (1 + cambio))
            SIMBOLOS[sim] = precio

            evento = {
                "simbolo": sim,
                "precio": round(precio, 2),
            }
            producer.send(TOPIC, evento)
        if i % 5 == 0:
            print(f"  tick {i}: " + ", ".join(f"{s}={p:,.0f}" for s, p in SIMBOLOS.items()))
        time.sleep(0.5)  
except KeyboardInterrupt:
    print("\nDeteniendo..."); producer.flush(); producer.close(); print("Productor cerrado.")
