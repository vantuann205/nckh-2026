"""Bootstrap processed dataset and one-time model artifact before starting API."""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from processing.model_service import train_model_once
from processing.offline_pipeline import ensure_processed_dataset


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("BootstrapAssets")


def main():
    logger.info("Preparing processed dataset")
    
    # Try to load existing processed dataset
    # If it doesn't exist, async loader will build it in background
    df = None
    try:
        df = ensure_processed_dataset(force_rebuild=False)
        logger.info("✅ Processed dataset loaded: %d rows", len(df))
    except Exception as e:
        logger.warning(f"⚠️ Could not load processed dataset: {e}")
        logger.info("This is OK - async loader will build it in background")
        df = None

    # Ensure model artifact exists for predictions (critical)
    logger.info("Ensuring model artifact (train once)")
    if df is not None and not df.empty:
        train_model_once(df, force=False)
    else:
        logger.warning("Skipping model training - processed dataset not available yet")
        logger.info("Model will be trained when dataset is available")
    
    logger.info("✅ Bootstrap ready (async loader will handle data loading)")


if __name__ == "__main__":
    main()
