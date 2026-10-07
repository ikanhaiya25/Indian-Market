# Destination: ml/train.py
from pathlib import Path

import mlflow
import pandas as pd
from sklearn.metrics import accuracy_score, f1_score
from xgboost import XGBClassifier

PROJECT_ROOT = Path(__file__).resolve().parents[1]
dataset_path = PROJECT_ROOT / "ml" / "dataset.parquet"
tracking_path = PROJECT_ROOT / "ml" / "mlflow_tracking"
features = ["sma_5", "sma_20", "rsi_14", "vwap", "volume"]

df = pd.read_parquet(dataset_path).sort_values("timestamp")
if len(df) < 100 or df["target"].nunique() < 2:
    raise ValueError("Need at least 100 labeled rows and both target classes to train")

cutoff = df["timestamp"].quantile(0.8)
train = df[df["next_timestamp"] < cutoff]
test = df[df["timestamp"] >= cutoff]
if train.empty or test.empty:
    raise ValueError("Not enough time coverage for a chronological train/test split")

mlflow.set_tracking_uri(tracking_path.as_uri())
mlflow.set_experiment("market-direction")
with mlflow.start_run():
    model = XGBClassifier(
        n_estimators=100,
        max_depth=4,
        learning_rate=0.05,
        eval_metric="logloss",
        random_state=42,
    )
    model.fit(train[features], train["target"])
    predictions = model.predict(test[features])
    mlflow.log_params({"n_estimators": 100, "max_depth": 4, "learning_rate": 0.05})
    mlflow.log_metrics({
        "accuracy": accuracy_score(test["target"], predictions),
        "f1": f1_score(test["target"], predictions, zero_division=0),
    })
    mlflow.xgboost.log_model(model, artifact_path="model")
    print(f"Test accuracy: {accuracy_score(test['target'], predictions):.3f}")
