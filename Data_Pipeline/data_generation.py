import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

# -----------------------------
# CONFIG
# -----------------------------
NUM_USERS = 1000
NUM_TRANSACTIONS = 100000   # 🔥 UPDATED (70k–1L supported)
FRAUD_RATIO = 0.03

LOCATIONS = [
    "chennai", "coimbatore", "madurai", "tiruchirappalli", "salem",
    "tirunelveli", "erode", "vellore", "thoothukudi", "dindigul",
    "thanjavur", "nagercoil", "hosur", "avadi", "kumbakonam",
    "cuddalore", "kanchipuram", "karur", "sivakasi", "tambaram",
    "karaikudi", "namakkal", "pudukkottai", "tiruvannamalai", "rajapalayam"
]

DEVICES = ["android", "ios", "web"]

# -----------------------------
# PATH SETUP
# -----------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "synthetic_upi_transactions.csv")
os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)

# -----------------------------
# USER PROFILES
# -----------------------------
users = []

for user_id in range(NUM_USERS):
    users.append({
        "user_id": user_id,
        "avg_amount": np.random.uniform(200, 8000),
        "std_amount": np.random.uniform(50, 2000),
        "home_location": random.choice(LOCATIONS),
        "primary_device": random.choice(DEVICES),
        "txn_frequency": np.random.uniform(1, 5)
    })

# -----------------------------
# TRANSACTION GENERATION
# -----------------------------
transactions = []

start_time = datetime.now()

last_txn_time = {u["user_id"]: start_time for u in users}
last_location = {u["user_id"]: u["home_location"] for u in users}

for txn_id in range(NUM_TRANSACTIONS):

    user = random.choice(users)
    user_id = user["user_id"]

    # TIME
    delta_minutes = np.random.exponential(scale=10)
    txn_time = last_txn_time[user_id] + timedelta(minutes=delta_minutes)
    last_txn_time[user_id] = txn_time

    # AMOUNT DISTRIBUTION (REALISTIC)
    r = np.random.rand()

    if r < 0.6:
        amount = np.random.uniform(10, 500)
    elif r < 0.9:
        amount = np.random.uniform(500, 5000)
    else:
        amount = np.random.uniform(5000, 50000)

    amount *= np.random.uniform(0.8, 1.2)
    amount = max(1, amount)

    # DEVICE
    if np.random.rand() < 0.9:
        device = user["primary_device"]
        is_new_device = 0
    else:
        device = random.choice(DEVICES)
        is_new_device = 1

    # LOCATION
    if np.random.rand() < 0.85:
        location = user["home_location"]
    else:
        location = random.choice(LOCATIONS)

    prev_loc = last_location[user_id]
    location_change = int(location != prev_loc)
    last_location[user_id] = location

    # TIME FEATURES
    hour = txn_time.hour
    is_night = int(hour < 5 or hour > 23)

    # VELOCITY
    velocity = np.random.poisson(lam=user["txn_frequency"])
    time_since_last = delta_minutes

    # DERIVED FEATURES
    high_amount_flag = int(amount > user["avg_amount"] * 2)
    amount_deviation = abs(amount - user["avg_amount"])
    transaction_count_last_hour = int(velocity)
    merchant_risk_score = np.random.uniform(0, 1)

    # FRAUD LOGIC
    risk_score = (
        0.30 * high_amount_flag +
        0.20 * is_new_device +
        0.15 * location_change +
        0.15 * (velocity > 3) +
        0.10 * (amount_deviation > user["avg_amount"]) +
        0.10 * merchant_risk_score +
        np.random.uniform(0, 0.15)
    )

    is_fraud = 1 if risk_score > 0.6 else 0

    # STORE
    transactions.append({
        "transaction_id": txn_id,
        "user_id": user_id,
        "merchant_id": random.randint(1, 500),

        "amount": round(amount, 2),
        "user_avg_amount": round(user["avg_amount"], 2),
        "amount_deviation": round(amount_deviation, 2),

        "timestamp": txn_time,

        "device_type": device,
        "is_new_device": is_new_device,

        "location": location,
        "previous_location": prev_loc,
        "location_change": location_change,

        "velocity": velocity,
        "transaction_count_last_hour": transaction_count_last_hour,
        "time_since_last_txn": round(time_since_last, 2),

        "merchant_risk_score": round(merchant_risk_score, 3),

        "hour": hour,
        "is_night": is_night,
        "high_amount_flag": high_amount_flag,

        "is_fraud": is_fraud
    })

# -----------------------------
# DATAFRAME
# -----------------------------
df = pd.DataFrame(transactions)

# -----------------------------
# ADJUST FRAUD RATIO
# -----------------------------
fraud_df = df[df["is_fraud"] == 1]
legit_df = df[df["is_fraud"] == 0]

target_fraud_count = int(FRAUD_RATIO * len(df))

if len(fraud_df) > target_fraud_count:
    fraud_df = fraud_df.sample(target_fraud_count, random_state=42)

df = pd.concat([legit_df, fraud_df]).sample(frac=1, random_state=42).reset_index(drop=True)

# -----------------------------
# SAVE
# -----------------------------
df.to_csv(DATA_PATH, index=False)

print("✅ Dataset generated successfully!")
print(f"📁 Saved at: {DATA_PATH}")
print("Fraud ratio:", df["is_fraud"].mean())
print("Total rows:", len(df))
print(df.head())