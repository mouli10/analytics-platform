import random
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
import os

fake = Faker()

def generate_transactions(n=500):
    rows = []
    now = datetime.utcnow()
    for i in range(n):
        created_at = now - timedelta(minutes=random.randint(0, 60*24*30))
        amount = random.randint(5_00, 500_00)  # cents
        status = random.choice(["succeeded", "failed", "refunded"])
        currency = "usd"
        customer_id = fake.uuid4()
        product_id = random.choice(["basic", "pro", "enterprise"])
        rows.append(
            {
                "transaction_id": fake.uuid4(),
                "customer_id": customer_id,
                "product_id": product_id,
                "amount_cents": amount,
                "currency": currency,
                "status": status,
                "created_at": created_at,
            }
        )
    return pd.DataFrame(rows)

def generate_app_events(n=1000):
    rows = []
    now = datetime.utcnow()
    event_types = ["signup", "login", "view_page", "click_cta", "start_trial", "upgrade"]
    for i in range(n):
        created_at = now - timedelta(minutes=random.randint(0, 60*24*30))
        user_id = fake.uuid4()
        event_type = random.choice(event_types)
        rows.append(
            {
                "event_id": fake.uuid4(),
                "user_id": user_id,
                "event_type": event_type,
                "event_timestamp": created_at,
            }
        )
    return pd.DataFrame(rows)

def main():
    os.makedirs("data", exist_ok=True)
    tx = generate_transactions()
    ev = generate_app_events()
    tx.to_csv("data/transactions.csv", index=False)
    ev.to_csv("data/app_events.csv", index=False)
    print("Generated data/transactions.csv and data/app_events.csv")

if __name__ == "__main__":
    main()
