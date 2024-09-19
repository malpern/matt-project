import logging
from datetime import datetime
from typing import Dict, List, Union
import pygsheets

class SheetManager:
    def __init__(self, gc: pygsheets.client.Client, spreadsheet_name: str, calendar_manager, data_processor):
        self.gc = gc
        self.spreadsheet = self.gc.open(spreadsheet_name)
        self.logger = logging.getLogger(__name__)
        self.calendar_manager = calendar_manager  # Store the CalendarManager instance
        self.data_processor = data_processor  # Store the DataProcessor instance

    def get_client_dict(self) -> Dict[str, int]:
        self.logger.info("Fetching client data from 'CLIENT LIST' tab...")
        client_list_sheet = self.get_sheet("CLIENT LIST")
        client_data = client_list_sheet.get_all_values()[1:]
        client_dict = {}
        for row in client_data:
            if row:
                try:
                    # Convert session count to integer, default to 0 if empty
                    client_dict[row[0]] = int(row[1]) if row[1] else 0
                except ValueError:
                    self.logger.warning(f"Invalid session count for client '{row[0]}': '{row[1]}'")
                    client_dict[row[0]] = 0
        return client_dict

    def clear_or_create_tab(self, tab_name: str) -> pygsheets.Worksheet:
        try:
            sheet = self.spreadsheet.worksheet_by_title(tab_name)
            self.logger.info(f"Clearing existing '{tab_name}' tab...")
            sheet.clear()
        except pygsheets.exceptions.WorksheetNotFound:
            self.logger.info(f"Creating '{tab_name}' tab...")
            sheet = self.spreadsheet.add_worksheet(tab_name)
        return sheet

    def get_sheet(self, tab_name: str) -> pygsheets.Worksheet:
        return self.spreadsheet.worksheet_by_title(tab_name)

    def find_sales_sheet(self, year: int) -> pygsheets.Worksheet:
        sales_tab_name = f"Sales & Sessions Completed {year}"
        self.logger.info(f"Searching for '{sales_tab_name}' tab...")
        for worksheet in self.spreadsheet.worksheets():
            if sales_tab_name in worksheet.title:
                return worksheet
        raise ValueError(f"Could not find the '{sales_tab_name}' tab.")

    def create_backup(self):
        self.logger.info("Creating backup of 'Sales & Sessions Completed' tab...")
        current_year = datetime.now().year
        sales_tab_name = f"Sales & Sessions Completed {current_year}"
        
        try:
            # Delete any existing backup
            existing_backup_sheets = [
                sheet for sheet in self.spreadsheet.worksheets()
                if sheet.title.startswith(f"BACKUP_{sales_tab_name}")
            ]
            for sheet in existing_backup_sheets:
                self.spreadsheet.del_worksheet(sheet)
                self.logger.info(f"Deleted existing backup: '{sheet.title}'")
            
            sales_sheet = self.get_sheet(sales_tab_name)
            backup_name = f"BACKUP_{sales_tab_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            backup_sheet = self.spreadsheet.add_worksheet(backup_name)
            
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
            
            self.logger.info("Data copied successfully.")
            self.logger.info(f"Backup created: '{backup_name}'")
        except Exception as e:
            self.logger.error(f"Failed to create backup: {str(e)}")
            raise

    def update_client_list(self):
        sales_sheet = self.find_sales_sheet(datetime.now().year)
        self.logger.info(f"Found '{sales_sheet.title}' tab.")

        self.logger.info("Fetching client names...")
        client_name_col = sales_sheet.find("CLIENT NAME")[0].col
        client_names = sales_sheet.get_col(client_name_col, include_tailing_empty=False)[1:]
        self.logger.info(f"Found {len(client_names)} client names.")

        self.logger.info("Counting sessions for each client...")
        session_counts = {}
        for name in client_names:
            session_counts[name] = session_counts.get(name, 0) + 1

        unique_client_names = sorted(set(client_names))
        self.logger.info(f"Found {len(unique_client_names)} unique clients.")

        self.logger.info("Opening 'CLIENT LIST' tab...")
        client_list_sheet = self.get_sheet("CLIENT LIST")

        self.logger.info("Updating column headers...")
        client_list_sheet.update_values('A1:B1', [['CLIENT NAME', 'SESSIONS COMPLETED']])

        if unique_client_names:
            self.logger.info("Preparing data for updating...")
            update_data = [
                [name, session_counts.get(name, 0)]
                for name in unique_client_names
            ]

            update_data.sort(key=lambda x: x[1], reverse=True)
    
            self.logger.info("Clearing existing data...")
            client_list_sheet.clear('A2')
            self.logger.info("Updating with new data...")
            client_list_sheet.update_values('A2', update_data)

            client_list_sheet.frozen_rows = 1

            self.logger.info(f"Updated {len(update_data)} client(s) in the CLIENT LIST tab.")
        else:
            self.logger.info("No clients found.")

    def update_last_week_tab(self, clients_met: Dict[str, Dict[str, Union[List[Dict], int]]]):
        last_week_sheet = self.get_sheet("LAST WEEK")

        if clients_met:
            self.logger.info(f"Updating 'LAST WEEK' tab with {len(clients_met)} entries...")
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
            self.logger.info(f"Updated {len(update_data)} client(s) in the LAST WEEK tab.")
        else:
            self.logger.info("No clients met with in the previous week.")

    def create_sessions_tab(self, clients_met: Dict[str, Dict[str, Union[List[Dict], int]]]):
        self.logger.info("Creating and populating 'SESSIONS' tab...")
        self.logger.info(f"Number of clients met: {len(clients_met)}")
        
        sessions_sheet = self.clear_or_create_tab("SESSIONS")

        headers = ['CLIENT NAME', 'DATE', 'TIME', 'MATCH STATUS']
        sessions_sheet.update_values('A1:D1', [headers])
        sessions_sheet.frozen_rows = 1

        current_year = datetime.now().year
        sales_tab_name = f"Sales & Sessions Completed {current_year}"
        try:
            sales_sheet = self.get_sheet(sales_tab_name)
        except pygsheets.exceptions.WorksheetNotFound:
            self.logger.error(f"Error: '{sales_tab_name}' tab not found. Cannot perform match checks.")
            sales_client_dates = set()
        else:
            sales_data = sales_sheet.get_all_values()
            if not sales_data:
                self.logger.warning(f"Warning: '{sales_tab_name}' tab is empty.")
                sales_client_dates = set()
            else:
                try:
                    client_col = sales_data[0].index("CLIENT NAME")
                    date_col = sales_data[0].index("DATE")
                except ValueError:
                    self.logger.error("Error: 'CLIENT NAME' or 'DATE' column not found in sales sheet.")
                    sales_client_dates = set()
                else:
                    start_of_week, end_of_week = self.calendar_manager.get_previous_week_range()
                    sales_client_dates = set(
                        (row[client_col].strip().lower(), self.data_processor.parse_date(row[date_col]))
                        for row in sales_data[1:]
                        if len(row) > max(client_col, date_col) and row[date_col].strip()
                        and start_of_week <= self.data_processor.parse_date(row[date_col]) <= end_of_week
                    )
                    self.logger.info(f"Number of entries in sales_client_dates: {len(sales_client_dates)}")

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

        self.logger.info(f"Number of entries in sessions_data: {len(sessions_data)}")

        sessions_data.sort(key=lambda x: datetime.strptime(f"{x[1]} {x[2]}", '%a %m/%d %I:%M %p'))
        
        if sessions_data:
            try:
                sessions_sheet.update_values('A2', sessions_data, extend=True)
                self.logger.info(f"Added {len(sessions_data)} entries to the 'SESSIONS' tab.")
            except Exception as e:
                self.logger.error(f"Error updating SESSIONS sheet: {str(e)}")
                import traceback
                traceback.print_exc()
        else:
            self.logger.info("No data to add to the 'SESSIONS' tab.")

        self.logger.info("'SESSIONS' tab creation and population completed.")

    def add_unmatched_sessions(self) -> bool:
        self.logger.info("Adding unmatched sessions to 'Sales & Sessions Completed' tab...")
        
        # Get the SESSIONS tab
        sessions_sheet = self.get_sheet("SESSIONS")
        sessions_data = sessions_sheet.get_all_values()
        
        # Get the Sales & Sessions Completed tab
        current_year = datetime.now().year
        sales_tab_name = f"Sales & Sessions Completed {current_year}"
        sales_sheet = self.get_sheet(sales_tab_name)
        sales_data = sales_sheet.get_all_values()
        
        # Filter unmatched sessions
        unmatched_sessions = [row for row in sessions_data[1:] if row[3] == "NO MATCH"]
        
        for session in unmatched_sessions:
            client_name = session[0]
            try:
                session_date = datetime.strptime(session[1], '%a %m/%d').replace(year=current_year)
            except ValueError:
                self.logger.warning(f"Invalid date format for session: {session[1]}")
                continue
            
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
                        self.logger.warning(f"Skipping row with invalid date: {row[0]}")
                        continue
            
            # Prepare the new row data
            new_row = [
                session_date.strftime('%m/%d/%Y'),  # Date
                client_name,                        # Client Name
                "Individual",                       # Assuming all unmatched sessions are individual
                "x of x",                           # Placeholder for session count
                "$XXX",                             # Placeholder for price
                "DUE???",                           # Placeholder for payment status
                "MONTLY CALC??",                    # Placeholder for monthly calculation
                "NO MATCH, INSERTED"                # Indication that this was an unmatched session
            ]
            
            try:
                sales_sheet.insert_rows(insert_row, values=[new_row])
                self.logger.info(f"Inserted unmatched session for {client_name} on {session_date.strftime('%m/%d/%Y')}")
            except Exception as e:
                self.logger.error(f"Error inserting row for {client_name}: {str(e)}")
                self.logger.warning("Skipping this unmatched session and moving to the next one.")
                continue  # Skip to the next unmatched session
            
            # Ask for confirmation
            while True:
                confirmation = input(f"Inserted unmatched session for {client_name} on {session_date.strftime('%m/%d/%Y')}. Is this correct? (y/a/n/q): ")
                if confirmation.lower() == 'n':
                    self.logger.info("User opted to remove the recently added row.")
                    try:
                        sales_sheet.delete_rows(insert_row)
                    except Exception as e:
                        self.logger.error(f"Error deleting row for {client_name}: {str(e)}")
                        self.logger.warning("Unable to remove the recently added row. Proceeding to the next unmatched session.")
                    print("Apologies for the error. Removing the recently added row.")
                    break
                elif confirmation.lower() == 'q':
                    self.logger.info("User opted to quit unmatched session processing.")
                    try:
                        sales_sheet.delete_rows(insert_row)
                    except Exception as e:
                        self.logger.error(f"Error deleting row for {client_name}: {str(e)}")
                        self.logger.warning("Unable to remove the recently added row. Quitting unmatched session processing.")
                    print("Quitting unmatched session processing.")
                    return False  # Indicate that the user wants to quit
                elif confirmation.lower() == 'a':
                    self.logger.info("User opted to proceed without further confirmations.")
                    print("Proceeding with the remaining unmatched sessions without confirmation.")
                    return True  # Indicate successful completion
                elif confirmation.lower() == 'y':
                    self.logger.info("User confirmed the inserted row.")
                    break
                else:
                    print("Invalid input. Please enter 'y', 'a', 'n', or 'q'.")
        
        self.logger.info("Finished adding unmatched sessions.")
        return True  # Indicate successful completion

    def reorder_tabs(self):
        self.logger.info("Reordering tabs...")
        desired_order = [
            f"Sales & Sessions Completed {datetime.now().year}",
            "LAST WEEK",
            "SESSIONS",
            "CLIENT LIST"
        ]
        
        worksheets = self.spreadsheet.worksheets()
        current_order = [ws.title for ws in worksheets]
        
        for index, tab_name in enumerate(desired_order):
            if tab_name in current_order:
                current_index = current_order.index(tab_name)
                if current_index != index:
                    worksheet = worksheets[current_index]
                    worksheet.index = index + 1  # pygsheets uses 1-based indexing
                    self.logger.info(f"Moved '{tab_name}' to position {index + 1}")
            else:
                self.logger.warning(f"Tab '{tab_name}' not found in the spreadsheet")
        
        self.logger.info("Tab reordering completed")