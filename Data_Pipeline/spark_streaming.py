import os
import joblib
import logging
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

# --- CONFIG ---
TOPIC = "upi-transactions"
KAFKA_BOOTSTRAP = "localhost:9092"
MASTER_URL = "local[*]" 
MONGO_URI = "mongodb://localhost:27017/"  # MongoDB Connection String

# Initialize Logger
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("FraudDetection")

# --- LOAD MODELS & ENCODERS ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(f"📥 Loading models from {BASE_DIR}/models...")

try:
    rf_model = joblib.load(os.path.join(BASE_DIR, "models", "rf_model.pkl"))
    xgb_model = joblib.load(os.path.join(BASE_DIR, "models", "xgb_model.pkl"))
    THRESHOLD = joblib.load(os.path.join(BASE_DIR, "models", "threshold.pkl"))
    le_device = joblib.load(os.path.join(BASE_DIR, "models", "device_encoder.pkl"))
    le_location = joblib.load(os.path.join(BASE_DIR, "models", "location_encoder.pkl"))
    print(f"✅ Models loaded | Current Threshold: {THRESHOLD}")
except Exception as e:
    print(f"❌ Error loading models: {e}")
    exit(1)

# --- MONGODB INITIALIZATION (THE CLEAN SLATE) ---
print("🧹 Connecting to MongoDB to clear old records...")
try:
    init_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    init_db = init_client["upi_fraud_db"]
    init_db.frauds.drop()  # This instantly deletes the old collection
    print("✨ MongoDB 'frauds' collection is wiped clean for a fresh run!")
except Exception as e:
    print(f"⚠️ Could not clear MongoDB. Make sure mongod is running. Error: {e}")

# --- SPARK SESSION ---
spark = SparkSession.builder \
    .appName("UPI Fraud Detection Local") \
    .master(MASTER_URL) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# --- FULL SCHEMA (20 Columns) ---
schema = StructType([
    StructField("transaction_id", DoubleType()),
    StructField("user_id", DoubleType()),
    StructField("merchant_id", DoubleType()),
    StructField("amount", DoubleType()),
    StructField("user_avg_amount", DoubleType()),
    StructField("amount_deviation", DoubleType()),
    StructField("timestamp", StringType()),
    StructField("device_type", StringType()),
    StructField("is_new_device", DoubleType()),
    StructField("location", StringType()),
    StructField("previous_location", StringType()),
    StructField("location_change", DoubleType()),
    StructField("velocity", DoubleType()),
    StructField("transaction_count_last_hour", DoubleType()),
    StructField("time_since_last_txn", DoubleType()),
    StructField("merchant_risk_score", DoubleType()),
    StructField("hour", DoubleType()),
    StructField("is_night", DoubleType()),
    StructField("high_amount_flag", DoubleType()),
    StructField("is_fraud", DoubleType())
])

def preprocess_batch(df_pd):
    df_pd = df_pd.copy()
    
    device_mapping = {val: i for i, val in enumerate(le_device.classes_)}
    df_pd["device_type"] = df_pd["device_type"].map(device_mapping).fillna(0).astype(int)
    
    location_mapping = {val: i for i, val in enumerate(le_location.classes_)}
    df_pd["location"] = df_pd["location"].map(location_mapping).fillna(0).astype(int)
    
    expected_features = rf_model.feature_names_in_
    for col_name in expected_features:
        if col_name not in df_pd.columns:
            df_pd[col_name] = 0.0
            
    return df_pd[expected_features]

def process_batch(df, batch_id):
    df_pd = df.toPandas()
    
    if df_pd.empty:
        return
        
    print(f"\n🔥 Batch {batch_id} | 📊 Received {len(df_pd)} transactions")

    # Predict
    X = preprocess_batch(df_pd)
    rf_probs = rf_model.predict_proba(X)[:, 1]
    xgb_probs = xgb_model.predict_proba(X)[:, 1]
    df_pd["fraud_score"] = (rf_probs + xgb_probs) / 2
    
    # Filter and Alert
    frauds = df_pd[df_pd["fraud_score"] > THRESHOLD]
    
    if len(frauds) > 0:
        print(f"🚨 ALERT: {len(frauds)} potential fraud(s) detected!")
        
        # --- MONGODB INJECTION ---
        try:
            # Connect to MongoDB
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
            db = client["upi_fraud_db"]
            collection = db["frauds"]
            
            # Convert Pandas DataFrame to a list of dictionaries for MongoDB
            fraud_records = frauds.to_dict("records")
            
            # Append batch details and timestamp to know exactly when our pipeline caught it
            for record in fraud_records:
                record["pipeline_detected_at"] = datetime.now()
                record["batch_id"] = batch_id
                record["total_batch_txns"] = len(df_pd)
                
            # Insert into the database
            collection.insert_many(fraud_records)
            print(f"💾 Successfully saved {len(frauds)} flagged transactions to MongoDB!")
            
        except Exception as e:
            print(f"❌ MongoDB Error: Could not save to database. {e}")
        # -------------------------

        for _, row in frauds.iterrows():
            print(f"   👉 ID: {int(row['transaction_id'])} | Score: {row['fraud_score']:.3f} | Amt: ₹{row['amount']}")
    else:
        print("✅ Batch processed: No fraud detected.")

# --- EXECUTION ---
print(f"🚀 Connecting to Local Kafka at {KAFKA_BOOTSTRAP}...")

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*").fillna(0)

# Start the Stream
query = df_parsed.writeStream \
    .foreachBatch(process_batch) \
    # Ensure you have BASE_DIR defined at the top of your file (which you already do)
    .option("checkpointLocation", os.path.join(BASE_DIR, "checkpoint")) \
    .trigger(processingTime="2 seconds") \
    .start()

print("⚡ Local Streaming pipeline is LIVE. Monitor this terminal.")
query.awaitTermination()