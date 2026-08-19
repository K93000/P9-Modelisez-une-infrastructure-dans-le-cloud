import os
import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

# Adresse du serveur Kafka/Redpanda (local par défaut, ou via Docker Compose)
BOOTSTRAP_SERVERS = os.getenv("KAFKA_SERVER", "localhost:9092")

# Initialisation du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TYPES_DEMANDE = [
    "Incident technique",
    "Facturation",
    "Demande d'information",
    "Accès et permissions",
    "Performance"
]

PRIORITES = ["Basse", "Moyenne", "Haute", "Critique"]

DEMANDES_EXEMPLES = {
    "Incident technique": "Impossible de se connecter à la plateforme.",
    "Facturation": "Erreur sur la dernière facture envoyée.",
    "Demande d'information": "Comment modifier mon adresse e-mail ?",
    "Accès et permissions": "Demande de droits d'accès au module RH.",
    "Performance": "Lenteur lors de la génération du rapport."
}

def generer_ticket(id_counter):
    type_dem = random.choice(TYPES_DEMANDE)
    return {
        "ticket_id": f"TCK-{id_counter:04d}",
        "client_id": f"CLI-{random.randint(100, 999)}",
        "created_at": datetime.now().isoformat(),
        "demande": DEMANDES_EXEMPLES[type_dem],
        "type_demande": type_dem,
        "priorite": random.choice(PRIORITES)
    }

print(f"=== Producteur démarré sur {BOOTSTRAP_SERVERS} ===")
counter = 1000

try:
    while True:
        ticket = generer_ticket(counter)
        producer.send('client_tickets', value=ticket)
        print(
    f"[+] Ticket envoyé : "
    f"ID={ticket['ticket_id']} | "
    f"Client={ticket['client_id']} | "
    f"Date={ticket['created_at']} | "
    f"Demande={ticket['demande']} | "
    f"Type={ticket['type_demande']} | "
    f"Priorité={ticket['priorite']}"
)
        counter += 1
        time.sleep(2)  # Envoi d'un ticket toutes les 2 secondes
except KeyboardInterrupt:
    print("\nArrêt du producteur.")
    producer.close()