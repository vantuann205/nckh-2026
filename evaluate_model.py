"""Script để đánh giá model và in ra metrics chi tiết."""

import logging
from processing.offline_pipeline import ensure_processed_dataset
from processing.model_service import train_model_once

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

if __name__ == "__main__":
    print("=" * 80)
    print("ĐÁNH GIÁ MODEL PHÂN LOẠI ÙN TẮC GIAO THÔNG")
    print("=" * 80)
    
    # Load dataset
    print("\n1. Loading processed dataset...")
    df = ensure_processed_dataset(force_rebuild=False)
    print(f"   ✓ Dataset loaded: {len(df)} rows")
    
    # Train model với force=True để xem metrics
    print("\n2. Training model and evaluating...")
    model_path = train_model_once(df, force=True)
    print(f"   ✓ Model saved to: {model_path}")
    
    print("\n" + "=" * 80)
    print("HOÀN TẤT!")
    print("=" * 80)
