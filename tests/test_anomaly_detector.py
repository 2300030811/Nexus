import pytest
pd = pytest.importorskip("pandas")
np = pytest.importorskip("numpy")
from unittest.mock import MagicMock

from ml_models.detect_anomalies import score_metrics

class TestScoreMetrics:
    def test_score_metrics_basic(self):
        # Create a dummy model
        mock_model = MagicMock()
        mock_model.predict_proba.return_value = np.array([[0.9, 0.1], [0.1, 0.9], [0.8, 0.2]])
        mock_model.predict.return_value = np.array([0, 1, 0])
        
        # Create dummy recent metrics
        data = {
            "window_start": ["2026-03-01T10:00:00", "2026-03-01T10:05:00", "2026-03-01T10:10:00"],
            "window_end": ["2026-03-01T10:05:00", "2026-03-01T10:10:00", "2026-03-01T10:15:00"],
            "category": ["Electronics", "Footwear", "Apparel"],
            "region": ["Delhi", "Maharashtra", "Karnataka"],
            "order_count": [10, 5, 20],
            "total_revenue": [1000.0, 500.0, 2000.0],
            "avg_order_value": [100.0, 100.0, 100.0]
        }
        df = pd.DataFrame(data)
        
        # Test scoring
        anomalies_df = score_metrics(mock_model, df)
        
        # Only the row where predict==1 should be returned
        assert len(anomalies_df) == 1
        
        # The returned row should be for Footwear
        assert anomalies_df.iloc[0]["category"] == "Footwear"
        
        # Check computed columns are present
        assert "expected_revenue" in anomalies_df.columns
        assert "revenue_ratio" in anomalies_df.columns
        assert "anomaly_score" in anomalies_df.columns
        assert "severity" in anomalies_df.columns
