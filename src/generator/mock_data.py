import json
import random
import uuid
from datetime import datetime

def generate_mock_records(count=1000):
    records = []
    districts = ["Gasabo", "Kicukiro", "Nyarugenge", "Musanze", "Huye"]
    for i in range(count):
        record = {
            "record_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat(),
            "national_id": f"11995800{random.randint(100000, 999999)}",
            "full_name": f"Citizen_{i}",
            "phone_number": f"+25078{random.randint(1000000, 9999999)}",
            "district": random.choice(districts),
            "metric_type": "soil_ph",
            "metric_value": round(random.uniform(4.5, 8.5), 2),
            "yield_estimate_kg": random.randint(100, 5000)
        }
        records.append(record)
    return records

if __name__ == "__main__":
    data = generate_mock_records(5000)
    with open("data/raw/telemetry_batch_1.json", "w") as f:
        json.dump(data, f, indent=2)
    print("Generated 5,000 raw records in data/raw/telemetry_batch_1.json")
