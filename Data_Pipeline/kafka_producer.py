import pandas as pd
import random
import json
import time
import sys
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# -----------------------------
# CONFIG
# -----------------------------
TOPIC = "upi-transactions"
BOOTSTRAP_SERVERS = ["localhost:9092"]
# This dynamically finds the data folder no matter whose computer it is on
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "synthetic_upi_transactions.csv")

BATCH_SIZE = 1000
SLEEP_TIME = 0.1 

# -----------------------------
# LOAD DATA
# -----------------------------
print("📥 Loading dataset...")
try:
    df = pd.read_csv(DATA_PATH)
    print(f"✅ Loaded {len(df)} base transactions")
except Exception as e:
    print(f"❌ Error loading CSV: {e}")
    sys.exit(1)

# -----------------------------
# KAFKA PRODUCER
# -----------------------------
print(f"🔗 Attempting to connect to Kafka at {BOOTSTRAP_SERVERS}...")
try:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        api_version=(3, 6, 1), 
        acks=1,
        retries=5,
        request_timeout_ms=30000
    )
    print("🚀 Kafka Producer connected successfully!")
except NoBrokersAvailable:
    print("❌ ERROR: No Brokers Available.")
    print("Check if Kafka is running and listeners are set to localhost:9092")
    sys.exit(1)
except Exception as e:
    print(f"❌ Unexpected Error: {e}")
    sys.exit(1)

global_txn_id = 200000 

def generate_transaction(row):
    global global_txn_id
    txn = row.to_dict().copy()
    
    # Clean numeric types for JSON serialization
    txn = {k: (float(v) if isinstance(v, (int, float)) else v) for k, v in txn.items()}
    
    # Assign a new unique ID
    txn["transaction_id"] = global_txn_id
    global_txn_id += 1
    
    # Add slight randomization for 'Live' simulation
    txn["amount"] = round(txn["amount"] * random.uniform(0.9, 1.1), 2)
    return txn

# -----------------------------
# STREAMING LOOP
# -----------------------------
print("🔥 Starting Real-Time Data Stream (With Heist Injection)...")
count = 0
start_time = time.time()

try:
    while True:
        # Sample a batch from the dataframe
        batch = df.sample(BATCH_SIZE)
        
        for _, row in batch.iterrows():
            txn = generate_transaction(row)
            
            # 🚨 THE HEIST INJECTION: Force a blatant fraud every 5000 transactions
            if global_txn_id % 5000 == 0:
                txn["amount"] = 950000.00          # Massive amount
                txn["device_type"] = "unknown"     # Unrecognized device
                txn["location"] = "foreign_ip"     # Impossible location
                txn["velocity"] = 150.0            # 150 txns in a minute
                txn["time_since_last_txn"] = 0.01  # Fraction of a second
                txn["merchant_risk_score"] = 1.0   # 100% merchant risk
                txn["is_fraud"] = 1.0

            producer.send(TOPIC, value=txn)
        
        # Flush ensures all messages in the batch are actually sent
        producer.flush()
        
        count += BATCH_SIZE
        elapsed = time.time() - start_time
        tps = count / elapsed if elapsed > 0 else 0
        
        print(f"📊 Sent: {count} transactions | TPS: {int(tps)} | Last ID: {global_txn_id-1}")
        
        time.sleep(SLEEP_TIME)

except KeyboardInterrupt:
    print("\n🛑 Stopping Producer...")
    producer.close()