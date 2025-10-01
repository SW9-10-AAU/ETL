import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

import unittest
from unittest.mock import patch
from connect import connect_to_db

class TestConnect(unittest.TestCase):
    
    @patch('psycopg.connect')
    @patch('os.getenv')
    def test_connect(self, mock_getenv, mock_psycopg_connect):
        # Mock environment variable and database connection
        mock_getenv.return_value = 'postgresql://user:pass@localhost/dbname'
        mock_psycopg_connect.return_value = 'mock_connection'

        conn = connect_to_db()

        # Assert that psycopg.connect was called with the correct argument
        mock_psycopg_connect.assert_called_with('postgresql://user:pass@localhost/dbname')

        # Check if connection was returned
        self.assertEqual(conn, 'mock_connection')

if __name__ == '__main__':
    unittest.main()