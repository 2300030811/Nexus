import pytest
from unittest.mock import MagicMock, patch
from kafka_producer.producer import check_simulation_mode

class TestProducerSimMode:
    @patch("kafka_producer.producer.get_db_config")
    @patch("kafka_producer.producer.psycopg2")
    def test_check_simulation_mode_true(self, mock_psycopg, mock_get_cfg):
        mock_get_cfg.return_value = {}
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        
        mock_psycopg.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_cur.fetchone.return_value = ("true",)
        
        sim_mode = check_simulation_mode()
        assert sim_mode is True
        mock_cur.execute.assert_called()

    @patch("kafka_producer.producer.get_db_config")
    @patch("kafka_producer.producer.psycopg2")
    def test_check_simulation_mode_false(self, mock_psycopg, mock_get_cfg):
        mock_get_cfg.return_value = {}
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        
        mock_psycopg.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_cur.fetchone.return_value = ("false",)
        
        sim_mode = check_simulation_mode()
        assert sim_mode is False

    @patch("kafka_producer.producer.get_db_config")
    @patch("kafka_producer.producer.psycopg2")
    def test_check_simulation_mode_fallback(self, mock_psycopg, mock_get_cfg):
        mock_get_cfg.return_value = {}
        mock_psycopg.connect.side_effect = Exception("DB Down")
        
        # Should catch exception and default to False
        sim_mode = check_simulation_mode()
        assert sim_mode is False

def test_on_error_logs_correct_event():
    """Verifies that on_error captures the right event, not a later one."""
    import json
    import os
    from kafka_producer.producer import make_error_handler
    
    dlq_file = "dlq_events.jsonl"
    if os.path.exists(dlq_file):
        os.remove(dlq_file)
        
    try:
        events = [{"event_id": "A"}, {"event_id": "B"}]
        # Create handlers for both, but only fire 'A'
        handler_a = make_error_handler(events[0])
        handler_b = make_error_handler(events[1]) # b is 'current' in a loop elsewhere
        
        handler_a(Exception("send failed"))
        
        with open(dlq_file, "r") as f:
            saved = json.loads(f.readline())
            
        assert saved["event_id"] == "A"
    finally:
        if os.path.exists(dlq_file):
            os.remove(dlq_file)
