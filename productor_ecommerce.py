#!/usr/bin/env python3
"""
productor_ecommerce.py - CASO B: Analítica de e-commerce en vivo.
Emite eventos de comportamiento al topic 'ecommerce'. Tipos de evento:
  - "vista"    : un usuario vio un producto
  - "carrito"  : lo agregó al carrito
  - "compra"   : lo compró
Patrones a detectar:
  - productos más vistos por ventana de tiempo
  - tasa de conversión vista -> compra
  - carritos ABANDONADOS (carrito sin compra posterior)

Requiere:  pip install kafka-python
Uso:       python3 productor_ecommerce.py
Detener:   Ctrl+C
"""
import json, time, random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

PRODUCTOS = [f"P{n:02d}" for n in range(1, 13)]   
PESOS = [12,10,9,8,7,6,5,4,3,3,2,1]
TOPIC = "ecommerce"
print(f"Enviando eventos de e-commerce al topic '{TOPIC}'. Ctrl+C para parar.")
print("Eventos: vista / carrito / compra. Muchos carritos quedan abandonados a propósito.")

i = 0
try:
    while True:
        i += 1
        for _ in range(random.randint(3, 6)):
            usuario = f"U{random.randint(1, 120):03d}"
            producto = random.choices(PRODUCTOS, weights=PESOS)[0]

            r = random.random()
            if r < 0.65:
                tipo = "vista"
            elif r < 0.88:
                tipo = "carrito"      
            else:
                tipo = "compra"       

            evento = {
                "usuario": usuario,
                "producto": producto,
                "tipo": tipo,
                "precio": round(random.uniform(20, 500), 2),
            }
            producer.send(TOPIC, evento)
        print(f"  tick {i}: eventos enviados")
        time.sleep(1.0)
except KeyboardInterrupt:
    print("\nDeteniendo..."); producer.flush(); producer.close(); print("Productor cerrado.")
