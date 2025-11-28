import time
import random
import os
from faker import Faker
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# 12-Factor App Config
url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
token = os.getenv("INFLUXDB_TOKEN", "my-super-secret-auth-token")
org = os.getenv("INFLUXDB_ORG", "fintech_org")
bucket = os.getenv("INFLUXDB_BUCKET", "fraud_bucket")

# Initialize Clients
fake = Faker()
client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

print("🚀 Starting transaction simulation...")

def generate_transaction():
    """Generates synthetic data with basic fraud rules."""
    user = fake.name()
    amount = round(random.uniform(10.0, 5000.0), 2)
    merchant = fake.company()
    card_provider = random.choice(["Visa", "Mastercard", "Amex"])
    
    # Fraud Logic: High amount or random 5% chance
    is_fraud = False
    if amount > 3000 or random.random() < 0.05:
        is_fraud = True
        risk_level = "CRITICAL"
    elif amount > 1000:
        risk_level = "HIGH"
    else:
        risk_level = "LOW"

    return {
        "user": user,
        "amount": amount,
        "merchant": merchant,
        "card": card_provider,
        "is_fraud": is_fraud,
        "risk_level": risk_level
    }

try:
    while True:
        data = generate_transaction()
        
        # Build Data Point (Tags=Indexed, Fields=Raw Data)
        point = Point("transactions") \
            .tag("status", "fraud" if data["is_fraud"] else "legit") \
            .tag("risk_level", data["risk_level"]) \
            .tag("card_provider", data["card"]) \
            .field("amount", data["amount"]) \
            .field("user", data["user"]) \
            .field("merchant", data["merchant"])

        write_api.write(bucket=bucket, org=org, record=point)
        
        # Verbose output
        icon = "🔴" if data["is_fraud"] else "🟢"
        print(f"{icon} Tx: ${data['amount']} | Risk: {data['risk_level']} | Sent to InfluxDB")
        
        # Simulate network latency
        time.sleep(random.uniform(0.1, 1.0))

except KeyboardInterrupt:
    print("\n🛑 Simulation stopped by user.")
    client.close()