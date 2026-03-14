import pytest
from unittest.mock import MagicMock, patch
from kafka_producer.producer import SimulationState

class TestSimulationState:
    def test_check_mode_true(self):
        state = SimulationState()
        state._conn = MagicMock()
        state._conn.closed = False
        cur = state._conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = ("true",)
        assert state.check_mode() is True

    def test_check_mode_false(self):
        state = SimulationState()
        state._conn = MagicMock()
        state._conn.closed = False
        cur = state._conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = ("false",)
        assert state.check_mode() is False

    @patch("kafka_producer.producer.get_db_config")
    @patch("kafka_producer.producer.psycopg2")
    def test_check_mode_fallback(self, mock_psycopg, mock_get_cfg):
        mock_get_cfg.return_value = {}
        mock_psycopg.connect.side_effect = Exception("DB Down")
        
        state = SimulationState()
        # Should catch exception and default to False
        assert state.check_mode() is False

def test_on_error_calls_dlq_with_correct_event():
    """Verifies that on_error captures the right event, not a later one."""
    from kafka_producer.producer import make_error_handler
    
    # Mock DLQ
    mock_dlq = MagicMock()
    
    events = [{"event_id": "A"}, {"event_id": "B"}]
    # Create handlers for both, but only fire 'A'
    handler_a = make_error_handler(events[0], mock_dlq)
    handler_b = make_error_handler(events[1], mock_dlq)
    
    handler_a(Exception("send failed"))
    
    # Verify handler_a used event 'A' even if handler_b exists
    mock_dlq.put.assert_called_once_with(events[0], "send failed")
