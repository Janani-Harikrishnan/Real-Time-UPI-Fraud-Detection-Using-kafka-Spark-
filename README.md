# Real-Time UPI Fraud Detection Pipeline

An end-to-end Big Data engineering project that detects fraudulent UPI transactions in real-time using a machine learning ensemble (Random Forest & XGBoost) integrated with Spark Streaming and Kafka.

## 🚀 System Architecture



The pipeline follows these stages:
1. **Data Generation**: A synthetic transaction generator simulates real-world UPI behavior and "heist" scenarios.
2. **Ingestion**: Kafka Producers stream transaction data into a dedicated topic.
3. **Processing**: Apache Spark Streaming consumes the data, applies feature engineering, and runs model inference.
4. **Storage & Serving**: Detected frauds are persisted in **MongoDB**, while live metrics are pushed to a **Flask API**.
5. **Visualization**: A **React.js** dashboard provides real-time alerts and fraud analytics.

## 🛠️ Tech Stack

- **Languages:** Python, JavaScript (ES6+)
- **Big Data:** Apache Spark (Structured Streaming), Apache Kafka
- **Machine Learning:** XGBoost, Scikit-Learn, Joblib
- **Backend:** Flask, MongoDB (PyMongo)
- **Frontend:** React.js, Axios

## 📂 Project Structure

```text
BDA_PROJECT/
├── frontend/           # React.js UI (Dashboard)
├── backend/            # Flask API (Metric Aggregator)
├── scripts/            # Core Processing Logic
│   ├── data_gen.py     # Synthetic data creation
│   ├── producer.py     # Kafka stream ingestion
│   ├── training.py     # ML Model training (XGBoost/RF)
│   ├── streaming.py    # Spark processing & MongoDB sink
│   └── bridge.py       # Spark to Flask live connector
├── models/             # Saved .pkl models & encoders
└── data/               # CSV datasets (ignored in Git)
