import os
from datetime import datetime, timedelta

import pygsheets
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from collections import defaultdict


def main():
    try:
        print("Starting the script...")
        # Authorize using OAuth 2.0 credentials
        print("Authorizing with pygsheets...")
        gc = pygsheets.authorize(client_secret='client_secret.json')
        print("Authorization successful.")
        
        # Clear or create "CLIENT LIST" and "LAST WEEK" tabs
        clear_or_create_tabs(gc)

        # Find the correct "Sales & Sessions Completed" tab for the current year
        current_year = datetime.now().year
        sales_tab_name = (f"Sales & Sessions Completed {current_year}")
        sales_sheet = None

        print(f"Searching for '{sales_tab_name}' tab...")
        for worksheet in gc.open('Matt-data-2024-test').worksheets():
            if sales_tab_name in worksheet.title:
                sales_sheet = worksheet
                break

        if not sales_sheet:
            print(f"Error: Could not find the '{sales_tab_name}' tab.")
            return
        print(f"Found '{sales_tab_name}' tab.")

        # Get all values from the "CLIENT NAME" column
        print("Fetching client names...")
        client_name_col = sales_sheet.find("CLIENT NAME")[0].col
        client_names = sales_sheet.get_col(
            client_name_col, include_tailing_empty=False
        )[1:]
        print(f"Found {len(client_names)} client names.")

        # Count sessions for each client
        print("Counting sessions for each client...")
        session_counts = {}
        for name in client_names:
            session_counts[name] = session_counts.get(name, 0) + 1

        # Remove duplicates and sort the list
        unique_client_names = sorted(set(client_names))
        print(f"Found {len(unique_client_names)} unique clients.")

        # Open the "CLIENT LIST" tab
        print("Opening 'CLIENT LIST' tab...")
        client_list_sheet = gc.open('Matt-data-2024-test').worksheet_by_title(
            "CLIENT LIST"
        )

        # Add column headers to the "CLIENT LIST" tab
        print("Updating column headers...")
        client_list_sheet.update_values(
            'A1:B1', [['CLIENT NAME', 'SESSIONS COMPLETED']]
        )

        # Update "CLIENT LIST" tab
        if unique_client_names:
            print("Preparing data for updating...")
            # Prepare data for updating
            update_data = [
                [name, session_counts.get(name, 0)]
                for name in unique_client_names
            ]

            # Sort the data by sessions completed in descending order
            update_data.sort(key=lambda x: x[1], reverse=True)
    
            print("Clearing existing data...")
            # Clear existing data and update with all data
            client_list_sheet.clear('A2')
            print("Updating with new data...")
            client_list_sheet.update_values('A2', update_data)

            # Freeze the header row
            client_list_sheet.frozen_rows = 1

            print(f"Updated {len(update_data)} client(s) in the CLIENT LIST "
                  f"tab.")
        else:
            print("No clients found.")

        # Process calendar events and update "LAST WEEK" tab
        clients_met = process_calendar_events(gc)

        # Check if clients_met is not empty before creating TEMP tab
        if clients_met:
            print("Clients met last week:")
            for client, data in clients_met.items():
                print(f"  {client}: {data['sessions']} sessions on "
                      f"{', '.join(str(d) for d in data['dates'])}")
            # Create and populate the "TEMP" tab
            create_temp_tab(gc, clients_met)
        else:
            print("No clients met last week. Skipping TEMP tab creation.")

    except Exception as e:
        print(f"An error occurred in the main function: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        print("Script execution completed.")


def clear_or_create_tabs(gc):
    spreadsheet = gc.open('Matt-data-2024-test')
    
    # Clear or create "CLIENT LIST" tab
    try:
        client_list_sheet = spreadsheet.worksheet_by_title("CLIENT LIST")
        print("Clearing existing 'CLIENT LIST' tab...")
        client_list_sheet.clear()
    except pygsheets.exceptions.WorksheetNotFound:
        print("Creating 'CLIENT LIST' tab...")
        client_list_sheet = spreadsheet.add_worksheet("CLIENT LIST")

    # Clear or create "LAST WEEK" tab
    try:
        last_week_sheet = spreadsheet.worksheet_by_title("LAST WEEK")
        print("Clearing existing 'LAST WEEK' tab...")
        last_week_sheet.clear()
    except pygsheets.exceptions.WorksheetNotFound:
        print("Creating 'LAST WEEK' tab...")
        last_week_sheet = spreadsheet.add_worksheet("LAST WEEK")

    print("Tabs 'CLIENT LIST' and 'LAST WEEK' are ready.")


def process_calendar_events(gc):
    try:
        print("Checking for existing credentials...")
        creds = None
        if os.path.exists('token.json'):
            print("Found token.json, loading credentials...")
            creds = Credentials.from_authorized_user_file(
                'token.json',
                ['https://www.googleapis.com/auth/calendar.readonly']
            )
        
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                print("Refreshing expired credentials...")
                creds.refresh(Request())
            else:
                print("No valid credentials found. Starting new auth flow...")
                flow = InstalledAppFlow.from_client_secrets_file(
                    'client_secret.json',
                    ['https://www.googleapis.com/auth/calendar.readonly']
                )
                creds = flow.run_local_server(port=0)
            print("Saving new credentials...")
            with open('token.json', 'w') as token:
                token.write(creds.to_json())

        print("Building Calendar service...")
        service = build('calendar', 'v3', credentials=creds)

        # Calculate date range for the previous week
        today = datetime.now().date()
        start_of_week = today - timedelta(days=today.weekday() + 7)
        end_of_week = start_of_week + timedelta(days=6)
        print(f"Fetching events from {start_of_week} to {end_of_week}...")

        # Fetch events from the calendar
        events_result = service.events().list(
            calendarId='f4lathletics@gmail.com',
            timeMin=start_of_week.isoformat() + 'T00:00:00Z',
            timeMax=end_of_week.isoformat() + 'T23:59:59Z',
            singleEvents=True,
            orderBy='startTime'
        ).execute()
        events = events_result.get('items', [])
        print(f"Found {len(events)} events.")

        # Process events and update the "LAST WEEK" tab
        print("Opening 'LAST WEEK' tab...")
        spreadsheet = gc.open('Matt-data-2024-test')
        last_week_sheet = spreadsheet.worksheet_by_title("LAST WEEK")

        print("Fetching client data from 'CLIENT LIST' tab...")
        client_list_sheet = spreadsheet.worksheet_by_title("CLIENT LIST")
        client_data = client_list_sheet.get_all_values()[1:]  # Exclude header
        client_dict = {row[0]: 0 for row in client_data if row}

        print("Processing events to find client matches...")
        clients_met = defaultdict(lambda: {'dates': set(), 'sessions': 0})
        for event in events:
            event_date_str = event['start'].get('dateTime', event['start'].get('date'))
            try:
                event_date = parse_date(event_date_str)
            except ValueError:
                print(f"Warning: Unable to parse event date '{event_date_str}'. Skipping event.")
                continue

            event_title = event.get('summary', '')
            event_description = event.get('description', '')

            # Check for client name matches (including partial matches)
            for client in client_dict:
                client_parts = client.lower().split()
                if any(part in event_title.lower() or 
                       part in event_description.lower() 
                       for part in client_parts):
                    clients_met[client]['dates'].add(event_date)
                    clients_met[client]['sessions'] += 1
                    break

        if clients_met:
            print(f"Updating 'LAST WEEK' tab with {len(clients_met)} "
                  f"entries...")
            max_sessions = max(data['sessions'] for data in clients_met.values())

            # Create headers based on max_sessions
            headers = (['CLIENT NAME', 'SESSIONS COMPLETED'] + 
                       [f'Session {i}' for i in range(1, max_sessions + 1)])
            last_week_sheet.update_values('A1', [headers])
            last_week_sheet.frozen_rows = 1

            # Prepare update data
            update_data = []
            for client, data in clients_met.items():
                row = [client, data['sessions']]
                for date in sorted(data['dates']):
                    row.append(date.strftime('%a %m/%d'))
                update_data.append(row)

            # Sort the data by sessions completed in descending order
            update_data.sort(key=lambda x: x[1], reverse=True)

            # Clear existing data and update with new data
            last_week_sheet.clear('A2')
            last_week_sheet.update_values('A2', update_data)
            print(f"Updated {len(update_data)} client(s) in the LAST WEEK tab.")
        else:
            print("No clients met with in the previous week.")

        return clients_met

    except Exception as e:
        print(f"An error occurred while processing calendar events: {str(e)}")
        import traceback
        traceback.print_exc()

    print("Calendar event processing completed.")
    return {}  # Return an empty dict if there was an error


def create_temp_tab(gc, clients_met):
    print("Creating and populating 'TEMP' tab...")
    print(f"Number of clients met: {len(clients_met)}")
    
    spreadsheet = gc.open('Matt-data-2024-test')
    
    # Clear or create "TEMP" tab
    try:
        temp_sheet = spreadsheet.worksheet_by_title("TEMP")
        print("Clearing existing 'TEMP' tab...")
        temp_sheet.clear()
    except pygsheets.exceptions.WorksheetNotFound:
        print("Creating 'TEMP' tab...")
        temp_sheet = spreadsheet.add_worksheet("TEMP")

    # Prepare headers including "MATCH STATUS"
    headers = ['CLIENT NAME', 'DATE', 'SESSIONS COMPLETED', 'MATCH STATUS']
    temp_sheet.update_values('A1:D1', [headers])
    temp_sheet.frozen_rows = 1

    # Get the "Sales & Sessions Completed" tab for the current year
    current_year = datetime.now().year
    sales_tab_name = f"Sales & Sessions Completed {current_year}"
    try:
        sales_sheet = spreadsheet.worksheet_by_title(sales_tab_name)
    except pygsheets.exceptions.WorksheetNotFound:
        print(f"Error: '{sales_tab_name}' tab not found. Cannot perform "
              f"match checks.")
        sales_client_dates = set()
    else:
        # Get all data from the sales sheet
        sales_data = sales_sheet.get_all_values()
        if not sales_data:
            print(f"Warning: '{sales_tab_name}' tab is empty.")
            sales_client_dates = set()
        else:
            # Assuming 'CLIENT NAME' and 'DATE' columns exist in sales_data
            try:
                client_col = sales_data[0].index("CLIENT NAME")
                date_col = sales_data[0].index("DATE")
            except ValueError:
                print("Error: 'CLIENT NAME' or 'DATE' column not found in "
                      "sales sheet.")
                sales_client_dates = set()
            else:
                # Create a set of tuples for faster lookup
                sales_client_dates = set(
                    (row[client_col].strip().lower(), parse_date(row[date_col]))
                    for row in sales_data[1:]
                    if len(row) > max(client_col, date_col) and row[date_col].strip()
                )
                print(f"Number of entries in sales_client_dates: {len(sales_client_dates)}")

    # Prepare data for the TEMP tab
    temp_data = []
    for client, data in clients_met.items():
        for date in data['dates']:
            date_str = date.strftime('%a %m/%d')
            match_status = ("MATCH" if (client.strip().lower(), date) in
                            sales_client_dates else "NO MATCH")
            temp_data.append([
                client,
                date_str,
                data['sessions'],
                match_status
            ])

    print(f"Number of entries in temp_data: {len(temp_data)}")

    # Sort data chronologically by date
    temp_data.sort(key=lambda x: datetime.strptime(x[1], '%a %m/%d'))

    # Update the TEMP sheet
    if temp_data:
        try:
            temp_sheet.update_values('A2', temp_data, extend=True)
            print(f"Added {len(temp_data)} entries to the 'TEMP' tab.")
        except Exception as e:
            print(f"Error updating TEMP sheet: {str(e)}")
            import traceback
            traceback.print_exc()
    else:
        print("No data to add to the 'TEMP' tab.")

    print("'TEMP' tab creation and population completed.")


def parse_date(date_str):
    """
    Parse the date string into a date object.
    Handles both ISO format (YYYY-MM-DD) and 'm/d/yyyy' format.
    """
    date_str = date_str.strip()
    try:
        # Try ISO format first (YYYY-MM-DD)
        return datetime.strptime(date_str[:10], '%Y-%m-%d').date()
    except ValueError:
        # If that fails, try m/d/yyyy format
        return datetime.strptime(date_str, '%m/%d/%Y').date()


if __name__ == "__main__":
    main()
