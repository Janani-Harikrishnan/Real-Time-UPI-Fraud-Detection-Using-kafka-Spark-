from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Global metrics storage
stats = {
    "total": 0,
    "detected": 0, # Predicted by AI
    "actual": 0,   # Actual 'is_fraud' label from data
    "missed": 0,   # Model said 0, but actual was 1
    "alerts": []
}

@app.route('/update', methods=['POST'])
def update():
    global stats
    data = request.json
    stats["total"] += data["batch_size"]
    stats["detected"] += data["detected"]
    stats["actual"] += data["actual"]
    stats["missed"] += data["missed"]
    # Prepend new alerts and keep only the latest 10
    stats["alerts"] = (data["alerts"] + stats["alerts"])[:10]
    return jsonify({"status": "ok"})

@app.route('/stats', methods=['GET'])
def get_stats():
    return jsonify(stats)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)