"""
Nexus – Model Rollback Utility

Allows operators to quickly rollback the production model to a previous version
in case of performance degradation or false positive spikes.

Usage:
    python rollback_model.py --timestamp 20260315_120000
"""

import shutil
import argparse
from pathlib import Path
from common.logging_utils import get_logger

logger = get_logger("nexus.rollback")

MODEL_DIR = Path("/app/model")
PRODUCTION_MODEL = MODEL_DIR / "model.json"
PRODUCTION_META = MODEL_DIR / "metadata.json"

def rollback(timestamp: str):
    target_model = MODEL_DIR / f"model_v{timestamp}.json"
    target_meta = MODEL_DIR / f"metadata_v{timestamp}.json"
    
    if not target_model.exists() or not target_meta.exists():
        logger.error("Rollback failed: Version '%s' not found in %s", timestamp, MODEL_DIR)
        # List available versions
        versions = sorted([f.stem.replace("model_v", "") for f in MODEL_DIR.glob("model_v*.json")])
        if versions:
            logger.info("Available versions: %s", ", ".join(versions))
        return False

    logger.info("Rolling back production model to version: %s", timestamp)
    
    # Backup current before overwriting (safety first)
    backup_ts = "pre_rollback_backup"
    shutil.copy(PRODUCTION_MODEL, MODEL_DIR / f"model_v{backup_ts}.json")
    shutil.copy(PRODUCTION_META, MODEL_DIR / f"metadata_v{backup_ts}.json")
    
    # Perform rollback
    shutil.copy(target_model, PRODUCTION_MODEL)
    shutil.copy(target_meta, PRODUCTION_META)
    
    logger.info("Rollback successful. Service will hot-reload the model within 60s.")
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rollback production ML model")
    parser.add_argument("--timestamp", required=True, help="Timestamp of the model to restore (e.g. 20260315_120000)")
    parser.add_argument("--model-dir", default=None, help="Override model directory (default: /app/model, fallback to ./model in dev)")
    args = parser.parse_args()

    # Resolve model directory: CLI override > /app/model > local ./model (dev fallback)
    if args.model_dir:
        effective_dir = Path(args.model_dir)
    elif MODEL_DIR.exists():
        effective_dir = MODEL_DIR
    else:
        effective_dir = Path(__file__).parent / "model"

    # Temporarily remap module-level paths so rollback() uses the resolved directory
    import ml_models.rollback_model as _self
    _self.MODEL_DIR = effective_dir
    _self.PRODUCTION_MODEL = effective_dir / "model.json"
    _self.PRODUCTION_META = effective_dir / "metadata.json"

    rollback(args.timestamp)
