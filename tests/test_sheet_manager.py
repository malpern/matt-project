import unittest
from unittest.mock import Mock, patch
from src.sheet_manager import SheetManager

class TestSheetManager(unittest.TestCase):

    @patch('src.sheet_manager.pygsheets')  # Mock pygsheets in the SheetManager module
    def setUp(self, mock_pygsheets):
        # Configure the mock pygsheets.authorize to return a mock client
        self.mock_gc = Mock()
        mock_pygsheets.authorize.return_value = self.mock_gc

        # Initialize SheetManager with the mocked pygsheets client
        self.sheet_manager = SheetManager()

    def test_add_unmatched_sessions(self):
        # Mock the all_values list to simulate existing data in the sheet
        all_values = [
            ["Date", "Client Name", "Type", "Current Session", "Amount", "Due", "Monthly Calc", "Status"],
            ["08/26/2024", "Dale Scaiano", "Individual", "2 of 10", "$XXX", "DUE???", "MONTHLY CALC??", "EXISTING CLIENT"],
            ["09/01/2024", "Existing Client", "Individual", "3 of 5", "$XXX", "DUE???", "MONTHLY CALC??", "EXISTING CLIENT"]
        ]

        # Mock unmatched sessions
        unmatched_sessions = [
            {'date': '10/07/2024', 'client_name': 'Dale Scaiano'},         # Existing client
            {'date': '10/08/2024', 'client_name': 'New Client'},           # New client
            {'date': '10/09/2024', 'client_name': 'Existing Client'},      # Existing client
            {'date': '10/10/2024', 'client_name': 'Corrupted Session'},    # Existing client with corrupted session
            {'date': '10/11/2024', 'client_name': 'Another New Client'},   # New client
        ]

        # Define side effects for find_last_client_row
        # Row 2 for "Dale Scaiano", None for "New Client", Row 3 for "Existing Client",
        # Row 3 for "Corrupted Session" (with corrupted data), None for "Another New Client"
        self.sheet_manager.find_last_client_row = Mock()
        self.sheet_manager.find_last_client_row.side_effect = [2, None, 3, 3, None]

        # Manually corrupt the session for "Corrupted Session"
        all_values[2][3] = "invalid_session"

        # Call the method under test
        new_rows = self.sheet_manager.add_unmatched_sessions(unmatched_sessions, all_values)

        # Assertions
        self.assertEqual(len(new_rows), 5)

        # 1. Dale Scaiano
        self.assertEqual(new_rows[0][1], "Dale Scaiano")
        self.assertEqual(new_rows[0][3], "1 of 10")  # Decremented from 2 of 10
        self.assertEqual(new_rows[0][7], "EXISTING CLIENT")

        # 2. New Client
        self.assertEqual(new_rows[1][1], "New Client")
        self.assertEqual(new_rows[1][3], "1 of 1")  # New client starts at 1 of 1
        self.assertEqual(new_rows[1][7], "NEW CLIENT")

        # 3. Existing Client
        self.assertEqual(new_rows[2][1], "Existing Client")
        self.assertEqual(new_rows[2][3], "2 of 5")  # Decremented from 3 of 5
        self.assertEqual(new_rows[2][7], "EXISTING CLIENT")

        # 4. Corrupted Session
        self.assertEqual(new_rows[3][1], "Corrupted Session")
        self.assertEqual(new_rows[3][3], "1 of 1")  # Invalid session format, defaults to 1 of 1
        self.assertEqual(new_rows[3][7], "EXISTING CLIENT")

        # 5. Another New Client
        self.assertEqual(new_rows[4][1], "Another New Client")
        self.assertEqual(new_rows[4][3], "1 of 1")  # New client starts at 1 of 1
        self.assertEqual(new_rows[4][7], "NEW CLIENT")

    def test_decrement_session(self):
        # Test normal decrement
        result = self.sheet_manager.decrement_session("3 of 5")
        self.assertEqual(result, "2 of 5")

        # Test decrement at minimum
        result = self.sheet_manager.decrement_session("1 of 5")
        self.assertEqual(result, "1 of 5")  # Should not go below 1

        # Test invalid format
        result = self.sheet_manager.decrement_session("invalid")
        self.assertEqual(result, "1 of 1")  # Should default to "1 of 1"

        # Test no decrement needed
        result = self.sheet_manager.decrement_session("1 of 1")
        self.assertEqual(result, "1 of 1")

    def test_get_all_values_missing_sheet(self):
        with self.assertRaises(TypeError):
            self.sheet_manager.get_all_values()

if __name__ == '__main__':
    unittest.main()
