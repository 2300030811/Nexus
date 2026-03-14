from unittest.mock import MagicMock
import sys

# Dashboard-specific mocks
sys.modules["streamlit_autorefresh"] = MagicMock()
sys.modules["plotly"] = MagicMock()
sys.modules["plotly.express"] = MagicMock()
sys.modules["plotly.graph_objects"] = MagicMock()
