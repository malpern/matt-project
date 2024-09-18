# Google Calendar Sheets Automation

This project automates the process of syncing data between Google Calendar and Google Sheets.

## Prerequisites

- Python 3.7 or higher
- A Google Cloud project with the Google Calendar API and Google Sheets API enabled
- `client_secret.json` file for OAuth 2.0 authentication

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/google-calendar-sheets-automation.git
   cd google-calendar-sheets-automation
   ```

2. Create a virtual environment (optional but recommended):
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
   ```

3. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

4. Place your `client_secret.json` file in the project root directory.

## Usage

1. Generate the token file (only needed once):
   ```
   python generate_token.py
   ```

2. Run the main script:
   ```
   python google_calendar_sheets_automation.py
   ```

## Configuration

- Update the `SPREADSHEET_NAME` and `CALENDAR_ID` constants in `google_calendar_sheets_automation.py` to match your Google Sheet and Calendar.

## Troubleshooting

- If you encounter authentication issues, delete the `token.json` file and run `generate_token.py` again.
- Make sure your Google Cloud project has the necessary APIs enabled and the OAuth consent screen configured.

## License

This project is licensed under the MIT License.