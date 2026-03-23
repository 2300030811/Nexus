# ADR 0001: Model Governance and Promotion Gates

## Status
Accepted

## Context
As the Anomaly Detection service integrates heavily into the Nexus operation loop, we need a method to evolve the model without degrading business operations. Currently, `train_model.py` produces the baseline model, but continuous training on production data (`retrain_production_model.py`) brings the risk of a corrupted feedback loop—especially if the model trains on its own false positives. Also, the data distribution expected by the model may drift, reducing its accuracy.

## Decision
We are adopting a Model Governance Architecture consisting of:
1. **Candidate Training and Promotion Gates**: The retraining pipeline will evaluate the F1 score of the new candidate model against the current production baseline. Only candidates that meet or exceed the F1 score are promoted to `./model/model.json`. Otherwise, they are saved as `candidate_{timestamp}.json` for manual review.
2. **False Positive Isolation**: When operators flag anomalies as 'False Positives' via the review dashboard, these are explicitly filtered out from training data labels (`is_anomaly=0` implicitly applied instead of reinforcing the anomaly flag).
3. **Automated Drift Detection**: `drift_monitor.py` automatically measures Population Stability Index (PSI) and Anomaly Rate shifts. We have introduced an `--auto-retrain` flag that triggers retraining when drift exceeds threshold limits (PSI > 0.2).
4. **Fast Rollbacks**: `rollback_model.py` provides an immediate escape hatch, swapping the underlying JSON model files and replacing them with standard backups, enabling a 60-second recovery MTTR.

## Consequences
- **Pros:** Safety gates prevent automated model degradation, breaking the negative feedback loop. Rollbacks offer high operational resilience.
- **Cons:** A degraded model score requires human intervention to manually review candidates that failed the gate, increasing MLOps operational overhead.

## Related
- `ml_models/retrain_production_model.py`
- `ml_models/drift_monitor.py`
- `ml_models/rollback_model.py`
