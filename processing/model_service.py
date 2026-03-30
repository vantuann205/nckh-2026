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


def predict_probability(model_bundle: Dict, rows: List[dict]) -> List[float]:
    if not rows:
        return []

    df = pd.DataFrame(rows)
    model = model_bundle["model"]

    # Dùng feature engineering giống lúc train
    X = _engineer_features(df)

    if hasattr(model, "predict_proba"):
        probs = model.predict_proba(X)
        # Lấy xác suất class 1 (congested)
        if probs.shape[1] == 1:
            return [float(p[0]) for p in probs]
        return [float(p[1]) for p in probs]

    preds = model.predict(X)
    return [float(v) for v in preds]
