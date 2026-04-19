"""Model training and prediction — XGBoost with feature engineering."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional

import joblib
import numpy as np
import pandas as pd
from sklearn.dummy import DummyClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.calibration import CalibratedClassifierCV

try:
    from xgboost import XGBClassifier
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False

from sklearn.ensemble import GradientBoostingClassifier  # fallback nếu không có xgboost

logger = logging.getLogger("ModelService")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODEL_DIR = PROJECT_ROOT / "data" / "processed"
MODEL_PATH = MODEL_DIR / "congestion_model.joblib"

# Base features từ data
BASE_FEATURES = [
    "speed_kmph",
    "weather_temp_c",
    "humidity_pct",
    "accident_severity",
    "congestion_km",
]

# Engineered features (tính từ base features)
ENGINEERED_FEATURES = [
    "speed_kmph",
    "weather_temp_c",
    "humidity_pct",
    "accident_severity",
    "congestion_km",
    # Feature engineering
    "speed_sq",           # speed^2 — tắc đường phi tuyến với tốc độ
    "speed_inv",          # 1/speed — khi speed gần 0, tắc rất cao
    "weather_risk",       # nhiệt độ cao + độ ẩm cao = nguy cơ cao
    "accident_x_congestion",  # interaction: tai nạn × tắc nghẽn
    "low_speed_flag",     # binary: speed < 20
    "very_low_speed_flag",# binary: speed < 10
]

# Forecast target standards (recommended order)
FORECAST_TARGETS: Dict[str, Dict[str, str]] = {
    "congestion_level": {
        "task": "classification",
        "note": "Low / Moderate / High",
        "model_hint": "XGBoost / RandomForest",
    },
    "estimated_delay_minutes": {
        "task": "regression",
        "note": "Delay prediction in minutes",
        "model_hint": "XGBoost Regressor",
    },
    "travel_time_minutes": {
        "task": "regression_time_series",
        "note": "ETA/travel-time optimization target",
        "model_hint": "XGBoost + time-lag or LSTM",
    },
}


def validate_training_targets(df: pd.DataFrame) -> None:
    """Hard quality gate: require aligned targets before training.

    This enforces internal consistency expected by the forecast standard:
    - `congestion_level` exists and matches speed thresholds.
    - `estimated_delay_minutes` and `travel_time_minutes` are non-negative.
    - `travel_time_minutes` is always >= `estimated_delay_minutes`.
    """
    required = ["speed_kmph", "congestion_level", "estimated_delay_minutes", "travel_time_minutes"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required target columns: {missing}")

    speed = pd.to_numeric(df["speed_kmph"], errors="coerce")
    expected = pd.Series("Low", index=df.index)
    expected[(speed < 40) & (speed >= 20)] = "Moderate"
    expected[speed < 20] = "High"

    level = df["congestion_level"].astype(str)
    delay = pd.to_numeric(df["estimated_delay_minutes"], errors="coerce")
    travel = pd.to_numeric(df["travel_time_minutes"], errors="coerce")

    invalid = (
        speed.isna()
        | (level != expected)
        | delay.isna()
        | travel.isna()
        | (delay < 0)
        | (travel < 0)
        | (travel < delay)
    )

    bad = int(invalid.sum())
    if bad > 0:
        raise ValueError(
            f"Training data is not 100% target-aligned: {bad}/{len(df)} rows violate forecast standard"
        )


def _engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Tạo thêm features từ raw data để tăng accuracy."""
    out = pd.DataFrame(index=df.index)

    speed = pd.to_numeric(df.get("speed_kmph"), errors="coerce").fillna(0).clip(lower=0)
    temp = pd.to_numeric(df.get("weather_temp_c"), errors="coerce").fillna(30)
    humidity = pd.to_numeric(df.get("humidity_pct"), errors="coerce").fillna(70)
    accident = pd.to_numeric(df.get("accident_severity"), errors="coerce").fillna(0)
    congestion = pd.to_numeric(df.get("congestion_km"), errors="coerce").fillna(0)

    out["speed_kmph"] = speed
    out["weather_temp_c"] = temp
    out["humidity_pct"] = humidity
    out["accident_severity"] = accident
    out["congestion_km"] = congestion

    # Engineered
    out["speed_sq"] = speed ** 2
    out["speed_inv"] = 1.0 / (speed + 1.0)          # tránh chia 0
    out["weather_risk"] = (temp / 40.0) * (humidity / 100.0)  # 0→1
    out["accident_x_congestion"] = accident * congestion
    out["low_speed_flag"] = (speed < 20).astype(int)
    out["very_low_speed_flag"] = (speed < 10).astype(int)

    return out


def train_model_once(
    processed_df: pd.DataFrame,
    model_path: Path = MODEL_PATH,
    force: bool = False,
) -> Path:
    model_path.parent.mkdir(parents=True, exist_ok=True)

    if model_path.exists() and not force:
        logger.info("Model exists, skip retraining: %s", model_path)
        return model_path

    if processed_df.empty:
        raise ValueError("Cannot train model from empty processed dataset")

    validate_training_targets(processed_df)

    X = _engineer_features(processed_df)
    y = (
        processed_df.get("congestion_flag", pd.Series([0] * len(processed_df)))
        .fillna(0)
        .astype(int)
    )

    # Fallback nếu chỉ có 1 class
    if y.nunique() < 2:
        logger.warning("Only 1 class in labels — using DummyClassifier")
        model = DummyClassifier(strategy="constant", constant=int(y.iloc[0] if len(y) else 0))
        model.fit(X, y)
        joblib.dump({"model": model, "feature_columns": ENGINEERED_FEATURES}, model_path)
        return model_path

    # Train/test split để đánh giá
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # Tính class weight để xử lý imbalanced data
    pos_count = int(y_train.sum())
    neg_count = int(len(y_train) - pos_count)
    scale_pos_weight = neg_count / pos_count if pos_count > 0 else 1.0

    if XGBOOST_AVAILABLE:
        logger.info("Training XGBoost classifier...")
        model = XGBClassifier(
            n_estimators=300,
            max_depth=6,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=3,
            gamma=0.1,
            reg_alpha=0.1,       # L1 regularization
            reg_lambda=1.0,      # L2 regularization
            scale_pos_weight=scale_pos_weight,
            use_label_encoder=False,
            eval_metric="logloss",
            random_state=42,
            n_jobs=-1,
        )
    else:
        logger.warning("XGBoost not installed — falling back to GradientBoostingClassifier")
        model = GradientBoostingClassifier(
            n_estimators=200,
            max_depth=5,
            learning_rate=0.05,
            subsample=0.8,
            min_samples_leaf=10,
            random_state=42,
        )

    model.fit(X_train, y_train)

    # Đánh giá model
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]

    try:
        auc = roc_auc_score(y_test, y_prob)
        logger.info("Model AUC-ROC: %.4f", auc)
    except Exception:
        pass

    report = classification_report(y_test, y_pred, target_names=["normal", "congested"])
    logger.info("Classification Report:\n%s", report)

    # Log feature importance
    if hasattr(model, "feature_importances_"):
        importances = dict(zip(ENGINEERED_FEATURES, model.feature_importances_))
        sorted_imp = sorted(importances.items(), key=lambda x: x[1], reverse=True)
        logger.info("Feature importances: %s", sorted_imp)

    payload = {
        "model": model,
        "feature_columns": ENGINEERED_FEATURES,
        "algorithm": "XGBoost" if XGBOOST_AVAILABLE else "GradientBoosting",
    }
    joblib.dump(payload, model_path)
    logger.info("✅ Model saved: %s (algo=%s)", model_path, payload["algorithm"])
    return model_path


def load_model(model_path: Path = MODEL_PATH) -> Optional[Dict]:
    if not model_path.exists():
        return None
    payload = joblib.load(model_path)
    algo = payload.get("algorithm", "unknown")
    logger.info("Model loaded: %s (algo=%s)", model_path, algo)
    return payload


def _resolve_feature_columns(model_bundle: Dict, model) -> List[str]:
    """Resolve expected feature columns from runtime model metadata.

    Priority:
    1) model.feature_names_in_ (most reliable for sklearn pipelines)
    2) saved payload field: feature_columns
    3) current engineered feature list
    """
    names = getattr(model, "feature_names_in_", None)
    if names is not None:
        cols = [str(c) for c in list(names)]
        if cols:
            return cols

    payload_cols = model_bundle.get("feature_columns")
    if isinstance(payload_cols, list) and payload_cols:
        return [str(c) for c in payload_cols]

    return list(ENGINEERED_FEATURES)


def _build_compatible_feature_frame(rows: List[dict], expected_cols: List[str]) -> pd.DataFrame:
    """Build a feature frame compatible with an existing persisted model.

    This keeps prediction stable across model versions trained with different
    feature sets (legacy base features vs engineered features).
    """
    raw_df = pd.DataFrame(rows)
    engineered_df = _engineer_features(raw_df)

    out = pd.DataFrame(index=engineered_df.index)
    for col in expected_cols:
        if col in engineered_df.columns:
            out[col] = engineered_df[col]
        elif col in raw_df.columns:
            out[col] = pd.to_numeric(raw_df[col], errors="coerce").fillna(0)
        else:
            # Preserve column shape for old models that expect now-missing inputs.
            out[col] = 0.0
    return out


def predict_probability(model_bundle: Dict, rows: List[dict]) -> List[float]:
    if not rows:
        return []

    model = model_bundle["model"]
    expected_cols = _resolve_feature_columns(model_bundle, model)
    X = _build_compatible_feature_frame(rows, expected_cols)

    if hasattr(model, "predict_proba"):
        probs = model.predict_proba(X)
        # Lấy xác suất class 1 (congested)
        if probs.shape[1] == 1:
            return [float(p[0]) for p in probs]
        return [float(p[1]) for p in probs]

    preds = model.predict(X)
    return [float(v) for v in preds]
