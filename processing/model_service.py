"""Model training and prediction helpers for one-time training policy."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional

import joblib
import pandas as pd
from sklearn.dummy import DummyClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


logger = logging.getLogger("ModelService")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODEL_DIR = PROJECT_ROOT / "data" / "processed"
MODEL_PATH = MODEL_DIR / "congestion_model.joblib"

FEATURE_COLUMNS = [
    "speed_kmph",
    "weather_temp_c",
    "humidity_pct",
    "accident_severity",
    "congestion_km",
]


def _prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    features = pd.DataFrame(index=df.index)
    for col in FEATURE_COLUMNS:
        features[col] = pd.to_numeric(df.get(col), errors="coerce").fillna(0)
    return features


def train_model_once(processed_df: pd.DataFrame, model_path: Path = MODEL_PATH, force: bool = False) -> Path:
    model_path.parent.mkdir(parents=True, exist_ok=True)
    if model_path.exists() and not force:
        logger.info("Model exists, skip retraining: %s", model_path)
        return model_path

    if processed_df.empty:
        raise ValueError("Cannot train model from empty processed dataset")

    x = _prepare_features(processed_df)
    y = processed_df.get("congestion_flag", pd.Series([0] * len(processed_df))).fillna(0).astype(int)

    if y.nunique() < 2:
        model = DummyClassifier(strategy="constant", constant=int(y.iloc[0] if len(y) else 0))
        model.fit(x, y)
    else:
        model = Pipeline(
            steps=[
                ("scaler", StandardScaler()),
                ("clf", LogisticRegression(max_iter=300, random_state=42)),
            ]
        )
        model.fit(x, y)

    payload = {
        "model": model,
        "feature_columns": FEATURE_COLUMNS,
    }
    joblib.dump(payload, model_path)
    logger.info("Model trained and saved: %s", model_path)
    return model_path


def load_model(model_path: Path = MODEL_PATH) -> Optional[Dict]:
    if not model_path.exists():
        return None
    payload = joblib.load(model_path)
    logger.info("Model loaded into memory: %s", model_path)
    return payload


def predict_probability(model_bundle: Dict, rows: List[dict]) -> List[float]:
    if not rows:
        return []

    df = pd.DataFrame(rows)
    feature_columns = model_bundle.get("feature_columns", FEATURE_COLUMNS)
    x = pd.DataFrame(index=df.index)
    for col in feature_columns:
        x[col] = pd.to_numeric(df.get(col), errors="coerce").fillna(0)

    model = model_bundle["model"]
    if hasattr(model, "predict_proba"):
        probabilities = model.predict_proba(x)
        if probabilities.shape[1] == 1:
            return [float(probabilities[i][0]) for i in range(len(probabilities))]
        return [float(probabilities[i][1]) for i in range(len(probabilities))]

    # Fallback for models without predict_proba.
    preds = model.predict(x)
    return [float(v) for v in preds]
