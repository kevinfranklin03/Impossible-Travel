"""
FIXED: Real-time Credit Card Transaction Generator

- Normal behavior: Each card stays in the SAME city.
- Fraud behavior: Card "teleports" to a distant city within minutes
  (distance > 5000 km, time gap 10–30 minutes → impossible travel).

Sends transactions to Azure Event Hubs.
"""

import json
import random
import time
from datetime import datetime, timedelta

from azure.eventhub import EventHubProducerClient, EventData
from geopy.distance import geodesic
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

CONNECTION_STRING = os.getenv("EVENT_HUB_CONNECTION_STRING")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")

# City coordinates (lat, lon)
CITIES = {
    "London": (51.5074, -0.1278),
    "New York": (40.7128, -74.0060),
    "Tokyo": (35.6762, 139.6503),
    "Sydney": (-33.8688, 151.2093),
    "Dubai": (25.2048, 55.2708),
    "São Paulo": (-23.5505, -46.6333),
    "Singapore": (1.3521, 103.8198),
    "Mumbai": (19.0760, 72.8777),
    "Paris": (48.8566, 2.3522),
    "Los Angeles": (34.0522, -118.2437),
}

MERCHANTS = {
    "retail": ["Amazon", "Walmart", "Target", "Tesco", "IKEA"],
    "gas": ["Shell", "BP", "Exxon", "Chevron"],
    "food": ["Starbucks", "McDonald's", "Subway", "Chipotle"],
    "tech": ["Apple Store", "Best Buy", "Microsoft Store"],
    "travel": ["British Airways", "Hilton", "Marriott", "Uber"],
}


def calculate_distance_km(city1: str, city2: str) -> float:
    """Calculate distance between cities in kilometers."""
    return geodesic(CITIES[city1], CITIES[city2]).kilometers


def generate_transaction(card_id: int, last_transaction: dict | None, force_fraud: bool) -> dict:
    """
    Generate a transaction for a card.

    Normal:
      - Card stays in the SAME city as last_transaction["location"].
      - Time always moves forward (UTC now).

    Fraud:
      - Card jumps to a city > 5000km away from last city.
      - Time gap 10–30 minutes from last_transaction["timestamp"].
    """
    if last_transaction:
        last_city = last_transaction["location"]

        if force_fraud:
            # Fraud: teleport to a far city, short time gap
            distant_cities = [
                c for c in CITIES.keys()
                if c != last_city and calculate_distance_km(last_city, c) > 5000
            ]
            new_city = random.choice(distant_cities) if distant_cities else "Tokyo"

            last_time = datetime.fromisoformat(last_transaction["timestamp"])
            timestamp = last_time + timedelta(minutes=random.randint(10, 30))

            distance = calculate_distance_km(last_city, new_city)
            delta_min = int((timestamp - last_time).total_seconds() // 60)

            print(
                f"🚨 FRAUD INJECTED: Card {card_id} | "
                f"{last_city} → {new_city} | {distance:.0f} km in {delta_min} mins"
            )
        else:
            # Normal: stay in same city, use current time
            new_city = last_city
            timestamp = datetime.utcnow()
    else:
        # First ever transaction for this card
        new_city = random.choice(list(CITIES.keys()))
        timestamp = datetime.utcnow()

    category = random.choice(list(MERCHANTS.keys()))

    return {
        "card_id": card_id,
        "transaction_id": f"TXN-{timestamp.strftime('%Y%m%d%H%M%S')}-{random.randint(1000, 9999)}",
        "location": new_city,
        "latitude": CITIES[new_city][0],
        "longitude": CITIES[new_city][1],
        "timestamp": timestamp.isoformat(),
        "amount": round(random.uniform(5.0, 500.0), 2),
        "merchant": random.choice(MERCHANTS[category]),
        "merchant_category": category,
        "currency": "USD",
    }


def main() -> None:
    if not CONNECTION_STRING:
        print("❌ ERROR: EVENT_HUB_CONNECTION_STRING not set in .env file")
        return

    print("=" * 70)
    print("🚀 FIXED IMPOSSIBLE-TRAVEL FRAUD GENERATOR")
    print("=" * 70)
    print(f"📡 Event Hub: {EVENT_HUB_NAME}")
    print("💳 Active cards: IDs 1000–1050")
    print("🎯 Approx fraud rate: 1 per 50 transactions")
    print("=" * 70)
    print()

    try:
        producer = EventHubProducerClient.from_connection_string(
            conn_str=CONNECTION_STRING,
            eventhub_name=EVENT_HUB_NAME,
        )
        print("✅ Connected to Azure Event Hubs\n")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return

    card_states: dict[int, dict] = {}
    transaction_count = 0
    fraud_count = 0

    try:
        while True:
            batch = producer.create_batch()
            batch_count = 0

            # Generate 10 transactions per batch
            for _ in range(10):
                transaction_count += 1
                card_id = random.randint(1000, 1050)

                # Inject fraud every 50th global transaction if the card has history
                force_fraud = (transaction_count % 50 == 0) and (card_id in card_states)
                if force_fraud:
                    fraud_count += 1

                transaction = generate_transaction(
                    card_id,
                    card_states.get(card_id),
                    force_fraud,
                )

                # Update state for card
                card_states[card_id] = transaction

                batch.add(EventData(json.dumps(transaction)))
                batch_count += 1

            producer.send_batch(batch)

            print(
                f"📤 Sent batch of {batch_count} txns | "
                f"Total Txns: {transaction_count} | Total Fraud: {fraud_count}"
            )

            # Light heartbeat log of a sample normal transaction
            if transaction_count % 100 == 0:
                sample_card = random.choice(list(card_states.keys()))
                sample_txn = card_states[sample_card]
                print(
                    f"✅ Sample card {sample_card} @ {sample_txn['location']} | "
                    f"Last amount: ${sample_txn['amount']:.2f}"
                )

            time.sleep(2)

    except KeyboardInterrupt:
        print("\n🛑 Generator stopped via keyboard interrupt.")
        print("=" * 70)
        print(f"📊 Total transactions: {transaction_count}")
        print(f"🚨 Total fraud events: {fraud_count}")
        print("=" * 70)
    except Exception as e:
        print(f"❌ Generator error: {e}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
