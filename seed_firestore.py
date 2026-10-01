"""Seed script for AutoLake Fleet & Telemetry Firestore database."""

from google.cloud import firestore

FIRESTORE_PROJECT = "qwiklabs-gcp-04-3cb39bb4f24f"

def seed():
    print(f"Connecting to Firestore for project '{FIRESTORE_PROJECT}'...")
    db = firestore.Client(project=FIRESTORE_PROJECT)
    collection_ref = db.collection("vehicles")

    sample_vehicles = [
        {
            "vin": "1G1YC2D47H5100001",
            "model": "VoltTrack-EV",
            "fleet": "WestCoast-Delivery",
            "status": "active",
            "mileage_km": 45210.5,
            "battery_health_pct": 94.2,
            "dtc_fault_codes": [],
            "last_seen_city": "San Francisco",
            "last_telemetry_timestamp": "2026-10-01T20:45:00Z",
            "notes": "Routine maintenance completed last week."
        },
        {
            "vin": "1G1YC2D47H5100002",
            "model": "CyberHauler-Heavy",
            "fleet": "Midwest-Freight",
            "status": "warning",
            "mileage_km": 112400.0,
            "battery_health_pct": 86.0,
            "dtc_fault_codes": ["P0A80", "P0562"],
            "last_seen_city": "Chicago",
            "last_telemetry_timestamp": "2026-10-01T21:10:00Z",
            "notes": "Battery pack voltage imbalance warning detected in Dataproc streaming job."
        },
        {
            "vin": "1G1YC2D47H5100003",
            "model": "UrbanTransit-Bus",
            "fleet": "EastCoast-Metro",
            "status": "active",
            "mileage_km": 68900.2,
            "battery_health_pct": 91.5,
            "dtc_fault_codes": [],
            "last_seen_city": "New York",
            "last_telemetry_timestamp": "2026-10-01T21:20:00Z",
            "notes": "Normal telemetry ingest pipeline via Composer DAG 'ingest_nyc_metro'."
        },
        {
            "vin": "1G1YC2D47H5100004",
            "model": "VoltTrack-EV",
            "fleet": "WestCoast-Delivery",
            "status": "maintenance",
            "mileage_km": 88350.8,
            "battery_health_pct": 79.4,
            "dtc_fault_codes": ["P0A1F"],
            "last_seen_city": "Seattle",
            "last_telemetry_timestamp": "2026-10-01T18:05:00Z",
            "notes": "Scheduled for battery cooling subsystem inspection."
        }
    ]

    for vehicle in sample_vehicles:
        doc_id = vehicle["vin"]
        collection_ref.document(doc_id).set(vehicle)
        print(f"Seeded vehicle VIN {doc_id} ({vehicle['model']})")

    print(f"Successfully seeded {len(sample_vehicles)} vehicles into Firestore collection 'vehicles'!")

if __name__ == "__main__":
    seed()
