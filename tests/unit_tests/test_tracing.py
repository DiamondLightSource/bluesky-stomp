from unittest.mock import Mock

import pytest
from stomp.connect import StompConnection11 as Connection  # type: ignore


@pytest.fixture
def mock_connection() -> Mock:
    return Mock(spec=Connection)
