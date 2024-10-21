import pytest
from unittest.mock import Mock, patch
import pandas as pd
from redshift_etl_dag import get_redshift_connection, table_exists, get_last_date

@pytest.fixture
def mock_conn():
    mock = Mock()
    mock_cursor = Mock()
    mock.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = [1]
    return mock

# Test para verificar la conexión a Redshift
@patch('redshift_etl_dag.redshift_connector.connect')
def test_get_redshift_connection(mock_connect):
    mock_connect.return_value = Mock()
    conn = get_redshift_connection()
    assert conn is not None

# Test para verificar si la tabla existe
def test_table_exists(mock_conn):
    cursor_mock = mock_conn.cursor.return_value
    cursor_mock.fetchone.side_effect = [[1], [0]]

    assert table_exists(mock_conn, 'cotizaciones_dolares') is True
    assert table_exists(mock_conn, 'non_existent_table') is False

# Test para obtener la última fecha registrada
def test_get_last_date(mock_conn):
    cursor_mock = mock_conn.cursor.return_value
    cursor_mock.fetchone.return_value = ['2024-10-10']
    last_date = get_last_date(mock_conn)
    assert last_date == pd.to_datetime('2024-10-10')
