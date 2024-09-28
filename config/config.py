import os

# Google Calendar Settings
CALENDAR_ID = 'f4lathletics@gmail.com'

# Google Sheets Settings
SPREADSHEET_NAME = 'Matt-data-2024-test'

# Authentication Settings
SCOPES = [
    'https://www.googleapis.com/auth/calendar.readonly',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/drive.readonly'  # Add this scope
]
TOKEN_FILE = os.path.join(os.path.dirname(__file__), 'token.json')  # Absolute path
CLIENT_SECRET_FILE = os.path.join(os.path.dirname(__file__), 'sheets.googleapis.com-python.json')  # Absolute path