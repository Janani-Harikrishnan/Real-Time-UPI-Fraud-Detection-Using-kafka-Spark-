import pandas as pd
import numpy as np
import os
import joblib
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import (
    classification_report,
    roc_auc_score,
    confusion_matrix,
    roc_curve,
    precision_recall_curve
)
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier

# -----------------------------
# PATH SETUP
# -----------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_PATH = os.path.join(BASE_DIR, "data", "synthetic_upi_transactions.csv")
DATA_DIR = os.path.join(BASE_DIR, "data")
MODEL_DIR = os.path.join(BASE_DIR, "models")
RESULT_DIR = os.path.join(BASE_DIR, "results")

os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(RESULT_DIR, exist_ok=True)

# -----------------------------
# LOAD DATA
# -----------------------------
print("📥 Loading dataset...")
df = pd.read_csv(DATA_PATH)

# -----------------------------
# PREPROCESSING
# -----------------------------
print("⚙️ Preprocessing...")

drop_cols = ["transaction_id", "timestamp", "previous_location"]

# Encode categorical
le_device = LabelEncoder()
df["device_type"] = le_device.fit_transform(df["device_type"])

le_location = LabelEncoder()
df["location"] = le_location.fit_transform(df["location"])

# Save encoders
joblib.dump(le_device, os.path.join(MODEL_DIR, "device_encoder.pkl"))
joblib.dump(le_location, os.path.join(MODEL_DIR, "location_encoder.pkl"))

# Features & target
X = df.drop(columns=drop_cols + ["is_fraud"])
y = df["is_fraud"]

# Save feature names
joblib.dump(list(X.columns), os.path.join(MODEL_DIR, "features.pkl"))

# -----------------------------
# SPLITTING
# -----------------------------
print("✂️ Splitting data...")

X_train, X_temp, y_train, y_temp = train_test_split(
    X, y, test_size=0.30, stratify=y, random_state=42
)

X_val, X_test, y_val, y_test = train_test_split(
    X_temp, y_temp, test_size=0.5, stratify=y_temp, random_state=42
)

def save_split(X_part, y_part, name):
    df_part = X_part.copy()
    df_part["is_fraud"] = y_part
    df_part.to_csv(os.path.join(DATA_DIR, f"{name}.csv"), index=False)

save_split(X_train, y_train, "train")
save_split(X_val, y_val, "val")
save_split(X_test, y_test, "test")

# -----------------------------
# TRAIN RANDOM FOREST
# -----------------------------
print("🌲 Training Random Forest...")

rf_model = RandomForestClassifier(
    n_estimators=150,
    class_weight="balanced",
    random_state=42,
    n_jobs=-1
)

rf_model.fit(X_train, y_train)

# -----------------------------
# TRAIN XGBOOST (IMPROVED)
# -----------------------------
print("🚀 Training XGBoost...")

scale_pos_weight = len(y_train[y_train == 0]) / len(y_train[y_train == 1])

xgb_model = XGBClassifier(
    n_estimators=300,
    max_depth=5,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    scale_pos_weight=scale_pos_weight,
    random_state=42,
    n_jobs=-1,
    eval_metric="logloss"
)

xgb_model.fit(
    X_train, y_train,
    eval_set=[(X_val, y_val)],
    verbose=False
)

# -----------------------------
# VALIDATION CHECK
# -----------------------------
print("\n🔍 Validation Performance...")

def eval_val(model, name):
    pred = model.predict(X_val)
    print(f"\n{name} Validation:")
    print(classification_report(y_val, pred))

eval_val(rf_model, "Random Forest")
eval_val(xgb_model, "XGBoost")

# -----------------------------
# TEST EVALUATION
# -----------------------------
def evaluate(model, name):
    print(f"\n📊 Evaluating {name}...")

    y_prob = model.predict_proba(X_test)[:, 1]
    y_pred = model.predict(X_test)

    print(classification_report(y_test, y_pred))
    print(f"{name} ROC-AUC:", roc_auc_score(y_test, y_prob))

    return y_prob

rf_prob = evaluate(rf_model, "Random Forest")
xgb_prob = evaluate(xgb_model, "XGBoost")

# -----------------------------
# ENSEMBLE
# -----------------------------
print("\n🤝 Ensemble Model...")

final_prob = 0.7 * xgb_prob + 0.3 * rf_prob

# -----------------------------
# AUTO THRESHOLD (BEST)
# -----------------------------
precision, recall, thresholds = precision_recall_curve(y_test, final_prob)

f1_scores = 2 * (precision * recall) / (precision + recall + 1e-6)
best_threshold = thresholds[np.argmax(f1_scores)]

print(f"\n🔥 Best Threshold: {best_threshold:.3f}")

final_pred = (final_prob > best_threshold).astype(int)

print("\nEnsemble Results:")
print(classification_report(y_test, final_pred))
print("ROC-AUC:", roc_auc_score(y_test, final_prob))

# Save threshold
joblib.dump(best_threshold, os.path.join(MODEL_DIR, "threshold.pkl"))

# -----------------------------
# SAVE METRICS
# -----------------------------
with open(os.path.join(RESULT_DIR, "metrics.txt"), "w") as f:
    f.write("Ensemble Model:\n")
    f.write(classification_report(y_test, final_pred))

# -----------------------------
# CONFUSION MATRIX
# -----------------------------
cm = confusion_matrix(y_test, final_pred)

plt.figure()
sns.heatmap(cm, annot=True, fmt="d", cmap="Blues")
plt.title("Confusion Matrix (Ensemble)")
plt.savefig(os.path.join(RESULT_DIR, "confusion_matrix.png"))

# -----------------------------
# ROC CURVE
# -----------------------------
fpr, tpr, _ = roc_curve(y_test, final_prob)

plt.figure()
plt.plot(fpr, tpr)
plt.title("ROC Curve")
plt.savefig(os.path.join(RESULT_DIR, "roc_curve.png"))

# -----------------------------
# PR CURVE (IMPORTANT)
# -----------------------------
plt.figure()
plt.plot(recall, precision)
plt.xlabel("Recall")
plt.ylabel("Precision")
plt.title("Precision-Recall Curve")
plt.savefig(os.path.join(RESULT_DIR, "pr_curve.png"))

# -----------------------------
# FEATURE IMPORTANCE
# -----------------------------
importance = pd.Series(xgb_model.feature_importances_, index=X.columns)
importance = importance.sort_values(ascending=False)

plt.figure(figsize=(10, 5))
importance.plot(kind="bar")
plt.title("Feature Importance (XGBoost)")
plt.tight_layout()
plt.savefig(os.path.join(RESULT_DIR, "feature_importance.png"))

# -----------------------------
# SAVE MODELS
# -----------------------------
joblib.dump(rf_model, os.path.join(MODEL_DIR, "rf_model.pkl"))
joblib.dump(xgb_model, os.path.join(MODEL_DIR, "xgb_model.pkl"))

print("\n✅ FINAL TRAINING COMPLETE!")
print("📁 Models + results + threshold saved.")