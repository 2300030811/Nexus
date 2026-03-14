# common/model_utils.py
"""Shared utilities for ML model versioning and metadata."""

import json
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np


def _serialise_metrics(metrics: dict) -> dict:
    """Convert numpy floats to plain Python floats for JSON serialisation."""
    out = {}
    for k, v in metrics.items():
        if isinstance(v, dict):
            out[k] = _serialise_metrics(v)
        elif isinstance(v, (np.floating, np.integer)):
            out[k] = float(v)
        else:
            out[k] = v
    return out


def save_versioned_model(
    model,                          # xgb.XGBClassifier
    model_dir: Path,
    metadata: dict[str, Any],
    source_df=None,                 # optional pandas DataFrame for data hash
) -> tuple[Path, Path]:
    """
    Save model + metadata to both current and timestamped paths.

    Returns (current_model_path, versioned_model_path).
    """
    model_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    current_path   = model_dir / "model.json"
    versioned_path = model_dir / f"model_v{timestamp}.json"

    model.save_model(str(current_path))
    model.save_model(str(versioned_path))

    # Data fingerprint
    if source_df is not None:
        data_hash = hashlib.sha256(
            source_df.values.tobytes()
        ).hexdigest()[:16]
    else:
        data_hash = "unknown"

    full_meta = {
        "training_timestamp": timestamp,
        "data_hash":          data_hash,
        **metadata,
    }
    full_meta["metrics"] = _serialise_metrics(
        full_meta.get("metrics", {})
    )

    meta_current   = model_dir / "metadata.json"
    meta_versioned = model_dir / f"metadata_v{timestamp}.json"

    for path in (meta_current, meta_versioned):
        with open(path, "w") as f:
            json.dump(full_meta, f, indent=2)

    return current_path, versioned_path


def load_metadata(model_dir: Path) -> dict | None:
    meta_path = model_dir / "metadata.json"
    if not meta_path.exists():
        return None
    with open(meta_path) as f:
        return json.load(f)


def list_model_versions(model_dir: Path) -> list[dict]:
    """Return all versioned models sorted newest-first."""
    versions = []
    for path in sorted(model_dir.glob("model_v*.json"), reverse=True):
        ts_str  = path.stem.replace("model_v", "")
        meta_p  = model_dir / f"metadata_v{ts_str}.json"
        meta    = json.loads(meta_p.read_text()) if meta_p.exists() else {}
        versions.append({
            "timestamp":  ts_str,
            "model_path": str(path),
            "f1":         meta.get("metrics", {}).get("f1"),
            "data_hash":  meta.get("data_hash"),
        })
    return versions
