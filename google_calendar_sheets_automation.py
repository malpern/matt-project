import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Union
import logging

import pygsheets
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from collections import defaultdict

# Constants
SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']
SPREADSHEET_NAME = 'Matt-data-2024-test'
CALENDAR_ID = 'f4lathletics@gmail.com'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class GoogleAuth:
    @staticmethod
    def get_credentials() -> Credentials:
        logging.info("Checking for existing credentials...")
        creds = None
        if os.path.exists('token.json'):
            logging.info("Found token.json, loading credentials...")
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                logging.info("Refreshing expired credentials...")
                creds.refresh(Request())
            else:
                logging.info("No valid credentials found. Starting new auth flow...")
                flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', SCOPES)
                creds = flow.run_local_server(port=0)
            logging.info("Saving new credentials...")
            with open('token.json', 'w') as token:
                token.write(creds.to_json())

        return creds

class SheetManager:
    def __init__(self, gc: pygsheets.client.Client, spreadsheet_name: str):
        self.gc = gc
        self.spreadsheet = self.gc.open(spreadsheet_name)

    def clear_or_create_tab(self, tab_name: str) -> pygsheets.Worksheet:
        try:
            sheet = self.spreadsheet.worksheet_by_title(tab_name)
            logging.info(f"Clearing existing '{tab_name}' tab...")
            sheet.clear()
        except pygsheets.exceptions.WorksheetNotFound:
            logging.info(f"Creating '{tab_name}' tab...")
            sheet = self.spreadsheet.add_worksheet(tab_name)
        return sheet

    def get_sheet(self, tab_name: str) -> pygsheets.Worksheet:
        return self.spreadsheet.worksheet_by_title(tab_name)

    def find_sales_sheet(self, year: int) -> pygsheets.Worksheet:
        sales_tab_name = f"Sales & Sessions Completed {year}"
        logging.info(f"Searching for '{sales_tab_name}' tab...")
        for worksheet in self.spreadsheet.worksheets():
            if sales_tab_name in worksheet.title:
                return worksheet
        raise ValueError(f"Could not find the '{sales_tab_name}' tab.")

class CalendarManager:
    def __init__(self, service):
        self.service = service

    @staticmethod
    def get_previous_week_range() -> Tuple[datetime, datetime]:
        today = datetime.now().date()
        start_of_week = today - timedelta(days=today.weekday() + 7)
        end_of_week = start_of_week + timedelta(days=6)
        return start_of_week, end_of_week

    def fetch_calendar_events(self, start_of_week: datetime, end_of_week: datetime) -> List[Dict]:
        logging.info(f"Fetching events from {start_of_week} to {end_of_week}...")
        events_result = self.service.events().list(
            calendarId=CALENDAR_ID,
            timeMin=start_of_week.isoformat() + 'T00:00:00Z',
            timeMax=end_of_week.isoformat() + 'T23:59:59Z',
            singleEvents=True,
            orderBy='startTime'
        ).execute()
        events = events_result.get('items', [])
        logging.info(f"Found {len(events)} events.")
        return events

class DataProcessor:
    @staticmethod
    def process_events(events: List[Dict], client_dict: Dict[str, int]) -> Dict[str, Dict[str, Union[List[Dict], int]]]:
        logging.info("Processing events to find client matches...")
        clients_met = defaultdict(lambda: {'events': [], 'sessions': 0})
        for event in events:
            event_date_str = event['start'].get('dateTime', event['start'].get('date'))
            try:
                event_date = DataProcessor.parse_date(event_date_str)
            except ValueError:
                logging.warning(f"Unable to parse event date '{event_date_str}'. Skipping event.")
                continue

            event_title = event.get('summary', '')
            event_description = event.get('description', '')

            for client in client_dict:
                client_parts = client.lower().split()
                if any(part in event_title.lower() or part in event_description.lower() for part in client_parts):
                    clients_met[client]['events'].append(event)
                    clients_met[client]['sessions'] += 1
                    break

        return clients_met

    @staticmethod
    def parse_date(date_str: str) -> datetime.date:
        date_str = date_str.strip()
        if not date_str:
            raise ValueError("Empty date string")
        try:
            return datetime.strptime(date_str[:10], '%Y-%m-%d').date()
        except ValueError:
            return datetime.strptime(date_str, '%m/%d/%Y').date()

class GoogleCalendarSheetsAutomation:
    def __init__(self):
        self.gc = pygsheets.authorize(client_secret='client_secret.json')
        self.sheet_manager = SheetManager(self.gc, SPREADSHEET_NAME)
        creds = GoogleAuth.get_credentials()
        calendar_service = build('calendar', 'v3', credentials=creds)
        self.calendar_manager = CalendarManager(calendar_service)
        self.data_processor = DataProcessor()

    def run(self):
        try:
            logging.info("Starting the script...")
            try:
                self.create_backup()
                print("Backup created successfully.")
            except Exception as e:
                logging.error(f"Failed to create backup: {str(e)}")
                print(f"Failed to create backup. Error: {str(e)}")
                print("Do you want to continue without a backup? (y/n)")
                if input().lower() != 'y':
                    print("Exiting script.")
                    return

            self.clear_or_create_tabs()
            self.update_client_list()
            clients_met = self.process_calendar_events()
            if clients_met:
                self.create_sessions_tab(clients_met)
            self.reorder_tabs()
            if not self.add_unmatched_sessions():
                print("Exiting script as requested.")
                return

        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")
            import traceback
            traceback.print_exc()
        finally:
            logging.info("Script execution completed.")

    def create_backup(self):
        logging.info("Creating backup of 'Sales & Sessions Completed' tab...")
        current_year = datetime.now().year
        sales_tab_name = f"Sales & Sessions Completed {current_year}"
        
        try:
            # Delete any existing backup
            existing_backup_sheets = [sheet for sheet in self.sheet_manager.spreadsheet.worksheets() if sheet.title.startswith(f"BACKUP_{sales_tab_name}")]
            for sheet in existing_backup_sheets:
                self.sheet_manager.spreadsheet.del_worksheet(sheet)
                logging.info(f"Deleted existing backup: '{sheet.title}'")
            
            sales_sheet = self.sheet_manager.get_sheet(sales_tab_name)
            backup_name = f"BACKUP_{sales_tab_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            backup_sheet = self.sheet_manager.spreadsheet.add_worksheet(backup_name)
            
            # Step 1: Copy all data from the original sheet to the backup
            data = sales_sheet.get_all_values()
            
            # Get the dimensions of the original sheet
            rows = len(data)
            cols = len(data[0]) if data else 0
            
            # Resize the backup sheet to match the original
            backup_sheet.resize(rows=rows, cols=cols)
            
            # Copy data in chunks to avoid exceeding limits
            chunk_size = 1000  # Adjust this value if needed
            for i in range(0, rows, chunk_size):
                end = min(i + chunk_size, rows)
                backup_sheet.update_values(f'A{i+1}', data[i:end])
            
            logging.info("Data copied successfully.")
            
            logging.info(f"Backup created: '{backup_name}'")
        except Exception as e:
            logging.error(f"Failed to create backup: {str(e)}")
            raise

    def clear_or_create_tabs(self):
        self.sheet_manager.clear_or_create_tab("CLIENT LIST")
        self.sheet_manager.clear_or_create_tab("LAST WEEK")
        logging.info("Tabs 'CLIENT LIST' and 'LAST WEEK' are ready.")

    def update_client_list(self):
        sales_sheet = self.sheet_manager.find_sales_sheet(datetime.now().year)
        logging.info(f"Found '{sales_sheet.title}' tab.")

        logging.info("Fetching client names...")
        client_name_col = sales_sheet.find("CLIENT NAME")[0].col
        client_names = sales_sheet.get_col(client_name_col, include_tailing_empty=False)[1:]
        logging.info(f"Found {len(client_names)} client names.")

        logging.info("Counting sessions for each client...")
        session_counts = {}
        for name in client_names:
            session_counts[name] = session_counts.get(name, 0) + 1

        unique_client_names = sorted(set(client_names))
        logging.info(f"Found {len(unique_client_names)} unique clients.")

        logging.info("Opening 'CLIENT LIST' tab...")
        client_list_sheet = self.sheet_manager.get_sheet("CLIENT LIST")

        logging.info("Updating column headers...")
        client_list_sheet.update_values('A1:B1', [['CLIENT NAME', 'SESSIONS COMPLETED']])

        if unique_client_names:
            logging.info("Preparing data for updating...")
            update_data = [
                [name, session_counts.get(name, 0)]
                for name in unique_client_names
            ]

            update_data.sort(key=lambda x: x[1], reverse=True)
    
            logging.info("Clearing existing data...")
            client_list_sheet.clear('A2')
            logging.info("Updating with new data...")
            client_list_sheet.update_values('A2', update_data)

            client_list_sheet.frozen_rows = 1

            logging.info(f"Updated {len(update_data)} client(s) in the CLIENT LIST tab.")
        else:
            logging.info("No clients found.")

    def process_calendar_events(self) -> Dict[str, Dict[str, Union[List[Dict], int]]]:
        start_of_week, end_of_week = self.calendar_manager.get_previous_week_range()
        events = self.calendar_manager.fetch_calendar_events(start_of_week, end_of_week)
        
        logging.info("Opening 'LAST WEEK' tab...")
        last_week_sheet = self.sheet_manager.get_sheet("LAST WEEK")

        logging.info("Fetching client data from 'CLIENT LIST' tab...")
        client_list_sheet = self.sheet_manager.get_sheet("CLIENT LIST")
        client_data = client_list_sheet.get_all_values()[1:]
        client_dict = {row[0]: 0 for row in client_data if row}

        clients_met = self.data_processor.process_events(events, client_dict)
        self.update_last_week_tab(clients_met)
        
        return clients_met

    def update_last_week_tab(self, clients_met: Dict[str, Dict[str, Union[List[Dict], int]]]):
        last_week_sheet = self.sheet_manager.get_sheet("LAST WEEK")

        if clients_met:
            logging.info(f"Updating 'LAST WEEK' tab with {len(clients_met)} entries...")
            max_sessions = max(data['sessions'] for data in clients_met.values())

            headers = ['CLIENT NAME', 'SESSIONS COMPLETED'] + [f'Session {i}' for i in range(1, max_sessions + 1)]
            last_week_sheet.update_values('A1', [headers])
            last_week_sheet.frozen_rows = 1

            update_data = []
            for client, data in clients_met.items():
                row = [client, data['sessions']]
                for event in sorted(data['events'], key=lambda e: e['start'].get('dateTime', e['start'].get('date'))):
                    event_date = datetime.fromisoformat(event['start'].get('dateTime', event['start'].get('date'))[:10])
                    row.append(event_date.strftime('%a %m/%d'))
                update_data.append(row)

            update_data.sort(key=lambda x: x[1], reverse=True)

            last_week_sheet.clear('A2')
            last_week_sheet.update_values('A2', update_data)
            logging.info(f"Updated {len(update_data)} client(s) in the LAST WEEK tab.")
        else:
            logging.info("No clients met with in the previous week.")

    def create_sessions_tab(self, clients_met: Dict[str, Dict[str, Union[List[Dict], int]]]):
        logging.info("Creating and populating 'SESSIONS' tab...")
        logging.info(f"Number of clients met: {len(clients_met)}")
        
        sessions_sheet = self.sheet_manager.clear_or_create_tab("SESSIONS")

        headers = ['CLIENT NAME', 'DATE', 'TIME', 'MATCH STATUS']
        sessions_sheet.update_values('A1:D1', [headers])
        sessions_sheet.frozen_rows = 1

        current_year = datetime.now().year
        sales_tab_name = f"Sales & Sessions Completed {current_year}"
        try:
            sales_sheet = self.sheet_manager.get_sheet(sales_tab_name)
        except pygsheets.exceptions.WorksheetNotFound:
            logging.error(f"Error: '{sales_tab_name}' tab not found. Cannot perform match checks.")
            sales_client_dates = set()
        else:
            sales_data = sales_sheet.get_all_values()
            if not sales_data:
                logging.warning(f"Warning: '{sales_tab_name}' tab is empty.")
                sales_client_dates = set()
            else:
                try:
                    client_col = sales_data[0].index("CLIENT NAME")
                    date_col = sales_data[0].index("DATE")
                except ValueError:
                    logging.error("Error: 'CLIENT NAME' or 'DATE' column not found in sales sheet.")
                    sales_client_dates = set()
                else:
                    start_of_week, end_of_week = self.calendar_manager.get_previous_week_range()
                    sales_client_dates = set(
                        (row[client_col].strip().lower(), self.data_processor.parse_date(row[date_col]))
                        for row in sales_data[1:]
                        if len(row) > max(client_col, date_col) and row[date_col].strip()
                        and start_of_week <= self.data_processor.parse_date(row[date_col]) <= end_of_week
                    )
                    logging.info(f"Number of entries in sales_client_dates: {len(sales_client_dates)}")

        sessions_data = []
        for client, data in clients_met.items():
            for event in data['events']:
                event_date = event['start'].get('dateTime', event['start'].get('date'))
                date_obj = datetime.fromisoformat(event_date.replace('Z', '+00:00'))
                date_str = date_obj.strftime('%a %m/%d')
                time_str = date_obj.strftime('%I:%M %p')
                match_status = "MATCH" if (client.strip().lower(), date_obj.date()) in sales_client_dates else "NO MATCH"
                sessions_data.append([
                    client,
                    date_str,
                    time_str,
                    match_status
                ])

        logging.info(f"Number of entries in sessions_data: {len(sessions_data)}")

        sessions_data.sort(key=lambda x: datetime.strptime(f"{x[1]} {x[2]}", '%a %m/%d %I:%M %p'))
        
        if sessions_data:
            try:
                sessions_sheet.update_values('A2', sessions_data, extend=True)
                logging.info(f"Added {len(sessions_data)} entries to the 'SESSIONS' tab.")
            except Exception as e:
                logging.error(f"Error updating SESSIONS sheet: {str(e)}")
                import traceback
                traceback.print_exc()
        else:
            logging.info("No data to add to the 'SESSIONS' tab.")

        logging.info("'SESSIONS' tab creation and population completed.")

    def add_unmatched_sessions(self):
        logging.info("Adding unmatched sessions to 'Sales & Sessions Completed' tab...")
        
        # Get the SESSIONS tab
        sessions_sheet = self.sheet_manager.get_sheet("SESSIONS")
        sessions_data = sessions_sheet.get_all_values()
        
        # Get the Sales & Sessions Completed tab
        current_year = datetime.now().year
        sales_tab_name = f"Sales & Sessions Completed {current_year}"
        sales_sheet = self.sheet_manager.get_sheet(sales_tab_name)
        sales_data = sales_sheet.get_all_values()
        
        # Filter unmatched sessions
        unmatched_sessions = [row for row in sessions_data[1:] if row[3] == "NO MATCH"]
        
        for session in unmatched_sessions:
            client_name = session[0]
            session_date = datetime.strptime(session[1], '%a %m/%d').replace(year=current_year)
            
            # Find the appropriate row to insert the new session
            insert_row = 1  # Default to inserting at the top if no match found
            for i, row in enumerate(reversed(sales_data)):
                if row and row[0]:  # Check if the row and date cell are not empty
                    try:
                        row_date = self.data_processor.parse_date(row[0])
                        if row_date == session_date.date():
                            insert_row = len(sales_data) - i
                            break
                        elif row_date < session_date.date():
                            insert_row = len(sales_data) - i + 1
                            break
                    except ValueError:
                        logging.warning(f"Skipping row with invalid date: {row[0]}")
                        continue
            
            # Prepare the new row data
            new_row = [
                session_date.strftime('%m/%d/%Y'),  # Date
                client_name,  # Client Name
                "Individual",  # Assuming all unmatched sessions are individual
                "x of x",  # Placeholder for session count
                "$XXX",  # Placeholder for price
                "DUE???",  # Placeholder for payment status
                "MONTLY CALC??",  # Placeholder for monthly calculation
                "NO MATCH, INSERTED"  # Indication that this was an unmatched session
            ]
            
            # Insert the new row
            sales_sheet.insert_rows(insert_row, values=[new_row])
            
            # Ask for confirmation
            print(f"Inserted unmatched session for {client_name} on {session_date.strftime('%m/%d/%Y')}")
            while True:
                confirmation = input("Is this correct? (y/a/n/q): ")
                if confirmation.lower() == 'n':
                    print("Apologies for the error. Removing the recently added row.")
                    sales_sheet.delete_rows(insert_row)
                    break
                elif confirmation.lower() == 'q':
                    print("Quitting unmatched session processing.")
                    sales_sheet.delete_rows(insert_row)
                    return False  # Return False to indicate the user wants to quit
                elif confirmation.lower() == 'a':
                    logging.info("Proceeding with the remaining unmatched sessions without confirmation.")
                    return True  # Return True to indicate successful completion
                elif confirmation.lower() == 'y':
                    break
                else:
                    print("Invalid input. Please enter 'y', 'a', 'n', or 'q'.")
        
        logging.info("Finished adding unmatched sessions.")
        return True  # Return True to indicate successful completion

    def reorder_tabs(self):
        logging.info("Reordering tabs...")
        desired_order = [
            f"Sales & Sessions Completed {datetime.now().year}",
            "LAST WEEK",
            "SESSIONS",
            "CLIENT LIST"
        ]
        
        worksheets = self.sheet_manager.spreadsheet.worksheets()
        current_order = [ws.title for ws in worksheets]
        
        for index, tab_name in enumerate(desired_order):
            if tab_name in current_order:
                current_index = current_order.index(tab_name)
                if current_index != index:
                    worksheet = worksheets[current_index]
                    worksheet.index = index + 1  # pygsheets uses 1-based indexing
                    logging.info(f"Moved '{tab_name}' to position {index + 1}")
            else:
                logging.warning(f"Tab '{tab_name}' not found in the spreadsheet")
        
        logging.info("Tab reordering completed")

if __name__ == "__main__":
    automation = GoogleCalendarSheetsAutomation()
    automation.run()
