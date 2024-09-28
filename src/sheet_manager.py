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
        print("Adding unmatched sessions to 'Sales & Sessions Completed' tab...")
        self.logger.info("Adding unmatched sessions to 'Sales & Sessions Completed' tab...")
        
        # Get the SESSIONS tab
        sessions_sheet = self.get_sheet("SESSIONS")
        sessions_data = sessions_sheet.get_all_values()
        
        # Get the latest data from the Sales & Sessions Completed tab
        current_year = datetime.now().year
        sales_tab_name = f"Sales & Sessions Completed {current_year}"
        try:
            sales_sheet = self.get_sheet(sales_tab_name)
        except Exception as e:
            print(f"Error accessing '{sales_tab_name}' sheet: {str(e)}")
            self.logger.error(f"Error accessing '{sales_tab_name}' sheet: {str(e)}")
            return False
        
        # Filter unmatched sessions
        unmatched_sessions = [row for row in sessions_data[1:] if row[3] == "NO MATCH"]
        
        if not unmatched_sessions:
            print("No unmatched sessions found.")
            self.logger.info("No unmatched sessions found.")
            return True

        print(f"Found {len(unmatched_sessions)} unmatched sessions.")
        self.logger.info(f"Found {len(unmatched_sessions)} unmatched sessions.")

        # Prepare new rows for all unmatched sessions
        new_rows = []
        for session in unmatched_sessions:
            client_name = session[0]
            try:
                session_date = datetime.strptime(session[1], '%a %m/%d').replace(year=current_year)
                new_row = [
                    session_date.strftime('%m/%d/%Y'),
                    client_name,
                    "Individual",
                    "x of x",
                    "$XXX",
                    "DUE???",
                    "MONTHLY CALC??",
                    "NO MATCH, INSERTED"
                ]
                new_rows.append(new_row)
                
                # Print the inserted row to the console if the client name is Dale Scaiano
                if client_name.lower() == "dale scaiano":
                    print(f"Inserted row for Dale Scaiano: {new_row}")
                    self.logger.info(f"Inserted row for Dale Scaiano: {new_row}")
                
                # Look backwards for the last occurrence of the same client
                all_values = sales_sheet.get_all_values()
                for row in reversed(all_values[1:]):  # Skip header
                    if row[1].strip().lower() == client_name.strip().lower():
                        print(f"Last occurrence of {client_name}: {row}")
                        self.logger.info(f"Last occurrence of {client_name}: {row}")
                        break
                else:
                    print(f"No previous occurrence found for {client_name}")
                    self.logger.info(f"No previous occurrence found for {client_name}")
                
            except ValueError:
                print(f"Warning: Invalid date format for session: {session[1]}")
                self.logger.warning(f"Invalid date format for session: {session[1]}")

        # Confirm with the user
        confirmation = input(f"Add {len(new_rows)} unmatched sessions to the end of the sheet? (y/n): ")
        if confirmation.lower() != 'y':
            print("User opted not to add unmatched sessions.")
            self.logger.info("User opted not to add unmatched sessions.")
            return False

        try:
            # Refresh the sheet and get the current number of rows
            sales_sheet.refresh()
            all_values = sales_sheet.get_all_values()
            current_row_count = len(all_values)
            print(f"Current row count in '{sales_tab_name}': {current_row_count}")
            self.logger.info(f"Current row count in '{sales_tab_name}': {current_row_count}")
            
            # Find the last non-empty row
            last_non_empty_row = current_row_count
            for i in range(current_row_count - 1, -1, -1):
                if any(all_values[i]):
                    last_non_empty_row = i + 1
                    break
            
            print(f"Last non-empty row: {last_non_empty_row}")
            self.logger.info(f"Last non-empty row: {last_non_empty_row}")
            
            # Resize the sheet if necessary
            required_rows = last_non_empty_row + len(new_rows)
            if required_rows > sales_sheet.rows:
                print(f"Resizing sheet to {required_rows} rows...")
                self.logger.info(f"Resizing sheet to {required_rows} rows...")
                sales_sheet.resize(rows=required_rows)
            
            # Append all new rows at once
            print(f"Appending {len(new_rows)} rows to the sheet...")
            self.logger.info(f"Appending {len(new_rows)} rows to the sheet...")
            
            # Use update_values to add new rows, starting from the first empty row
            start_cell = f'A{last_non_empty_row + 1}'
            end_column = chr(ord('A') + len(new_rows[0]) - 1)  # Calculate the last column letter
            end_cell = f'{end_column}{last_non_empty_row + len(new_rows)}'
            range_to_update = f'{start_cell}:{end_cell}'
            
            print(f"Updating range: {range_to_update}")
            self.logger.info(f"Updating range: {range_to_update}")
            
            sales_sheet.update_values(range_to_update, new_rows)
            
            # After adding new rows, get all values from the sheet
            all_values = sales_sheet.get_all_values()

            # Separate header and data
            header = all_values[0]
            data = all_values[1:]

            # Sort data based on the date column (assuming it's the first column)
            sorted_data = sorted(data, key=lambda x: datetime.strptime(x[0], '%m/%d/%Y'))

            # Prepare the sorted data with the header
            sorted_values = [header] + sorted_data

            # Clear the sheet and update with sorted values
            sales_sheet.clear()
            sales_sheet.update_values('A1', sorted_values)

            self.logger.info(f"Successfully added {len(new_rows)} unmatched sessions and sorted the sheet.")
        except Exception as e:
            self.logger.error(f"Error adding unmatched sessions or sorting: {str(e)}")
            self.logger.info("Please check the spreadsheet manually.")
            
            # Additional debugging information
            self.logger.info(f"Spreadsheet ID: {self.spreadsheet.id}")
            self.logger.info(f"Sheet ID: {sales_sheet.id}")
            self.logger.info(f"Sheet title: {sales_sheet.title}")
            self.logger.info(f"Sheet dimensions: {sales_sheet.rows} rows x {sales_sheet.cols} columns")
            return False

        return True

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