from flask import Flask, jsonify
from flask_cors import CORS
from pymongo import MongoClient

app = Flask(__name__)
CORS(app)

# Connect to your local MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["upi_fraud_db"]
collection = db["frauds"]

@app.route('/api/latest-batch', methods=['GET'])
def get_latest_batch():
    # 1. Find the exact timestamp of the most recent batch that had frauds
    latest_record = list(collection.find({}, {"_id": 0}).sort("pipeline_detected_at", -1).limit(1))
    
    if not latest_record:
        return jsonify({"status": "empty", "message": "No frauds detected yet."})

    latest_time = latest_record[0]["pipeline_detected_at"]
    batch_id = latest_record[0].get("batch_id", 0)
    total_txns = latest_record[0].get("total_batch_txns", 0)

    # 2. Get ALL fraud records that share this exact timestamp
    frauds = list(collection.find({"pipeline_detected_at": latest_time}, {"_id": 0}))

    return jsonify({
        "status": "success",
        "batch_id": batch_id,
        "fraud_count": len(frauds),
        "total_txns": total_txns,
        "timestamp": latest_time,
        "data": frauds
    })

@app.route('/api/last-five-batches', methods=['GET'])
def get_last_five():
    # MongoDB Aggregation to group by timestamp, sort descending, and grab the last 5 distinct batches
    pipeline = [
        {
            "$group": {
                "_id": "$pipeline_detected_at",
                "batch_id": {"$first": "$batch_id"},
                "total_txns": {"$first": "$total_batch_txns"},
                "frauds": {"$push": "$$ROOT"}
            }
        },
        {"$sort": {"_id": -1}},
        {"$limit": 5}
    ]
    
    results = list(collection.aggregate(pipeline))
    
    # Strip out the non-serializable MongoDB ObjectIds from the nested arrays
    for batch in results:
        for f in batch["frauds"]:
            if "_id" in f:
                del f["_id"]
                
    return jsonify({"status": "success", "history": results})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)