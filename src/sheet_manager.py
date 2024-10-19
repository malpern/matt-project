import logging
from datetime import datetime, timedelta
from typing import Dict, List, Union
import pygsheets
from collections import defaultdict

class SheetManager:
    def __init__(self, gc: pygsheets.client.Client, spreadsheet_name: str, calendar_manager, data_processor):
        self.gc = gc
        self.spreadsheet = self.gc.open(spreadsheet_name)
        self.logger = logging.getLogger(__name__)
        self.calendar_manager = calendar_manager  # Store the CalendarManager instance
        self.data_processor = data_processor  # Store the DataProcessor instance
        self.column_indices = {}  # Store column indices for each sheet

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
            
            # Copy all data from the original sheet to the backup
            data = sales_sheet.get_all_values()
            rows = len(data)
            cols = len(data[0]) if data else 0
            
            backup_sheet.resize(rows=rows, cols=cols)
            
            # Copy data in chunks to avoid exceeding limits
            chunk_size = 1000
            for i in range(0, rows, chunk_size):
                end = min(i + chunk_size, rows)
                backup_sheet.update_values(f'A{i+1}', data[i:end])
            
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
        last_week_sheet = self.clear_or_create_tab("LAST WEEK")

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
        sessions_sheet = self.clear_or_create_tab("SESSIONS")

        headers = ['CLIENT NAME', 'DATE', 'TIME', 'MATCH STATUS']
        sessions_sheet.update_values('A1:D1', [headers])
        sessions_sheet.frozen_rows = 1

        current_year = datetime.now().year
        sales_sheet = self.find_sales_sheet(current_year)
        sales_data = sales_sheet.get_all_values()

        client_col = sales_data[0].index("CLIENT NAME")
        date_col = sales_data[0].index("DATE")

        start_of_week, end_of_week = self.calendar_manager.get_previous_week_range()
        
        self.logger.info(f"Checking for matches between {start_of_week} and {end_of_week}")

        sales_client_dates = {}
        for row in sales_data[1:]:
            if len(row) > max(client_col, date_col) and row[date_col].strip():
                try:
                    client = row[client_col].strip().lower()
                    date_str = row[date_col].strip()
                    date_obj = datetime.strptime(date_str, '%m/%d/%Y').date()
                    if client not in sales_client_dates or date_obj > sales_client_dates[client]:
                        sales_client_dates[client] = date_obj
                except ValueError:
                    self.logger.warning(f"Invalid date format in sales sheet: {row[date_col]}")

        sessions_data = []
        for client, data in clients_met.items():
            for event in data['events']:
                event_date = event['start'].get('dateTime', event['start'].get('date'))
                date_obj = datetime.fromisoformat(event_date.replace('Z', '+00:00'))
                date_str = date_obj.strftime('%a %m/%d')
                time_str = date_obj.strftime('%I:%M %p')
                
                if start_of_week <= date_obj.date() <= end_of_week:
                    client_lower = client.strip().lower()
                    match_status = "NO MATCH"
                    if client_lower in sales_client_dates:
                        latest_sales_date = sales_client_dates[client_lower]
                        if date_obj.date() > latest_sales_date:
                            match_status = "NO MATCH"
                        else:
                            match_status = "MATCH"
                    
                    sessions_data.append([client, date_str, time_str, match_status])

        sessions_data.sort(key=lambda x: datetime.strptime(f"{x[1]} {x[2]}", '%a %m/%d %I:%M %p'))
        
        if sessions_data:
            sessions_sheet.update_values('A2', sessions_data)
            self.logger.info(f"Added {len(sessions_data)} entries to the 'SESSIONS' tab.")
        else:
            self.logger.info("No data to add to the 'SESSIONS' tab.")

    def get_column_index(self, sheet, column_name):
        if sheet.title not in self.column_indices:
            header = sheet.get_row(1)
            self.column_indices[sheet.title] = {col.upper(): idx for idx, col in enumerate(header)}
        
        return self.column_indices[sheet.title].get(column_name.upper(), -1)

    def get_current_session(self, sheet, row_index):
        current_session_col = self.get_column_index(sheet, "CURRENT SESSION")
        if current_session_col == -1:
            self.logger.warning("'CURRENT SESSION' column not found in sheet")
            return "1 of 1"
        return sheet.cell((row_index, current_session_col + 1)).value or "1 of 1"

    def decrement_session(self, current_session):
        try:
            current, total = map(int, current_session.split(' of '))
            new_current = max(current - 1, -1)  # Allow negative values
            return f"{new_current} of {total}"
        except ValueError:
            return "0 of 1"  # Default value if parsing fails

    def add_unmatched_sessions(self):
        self.logger.info("Adding unmatched sessions to 'Sales & Sessions Completed' tab...")
        
        sessions_sheet = self.get_sheet("SESSIONS")
        sessions_data = sessions_sheet.get_all_values()[1:]  # Skip header row
        
        current_year = datetime.now().year
        sales_sheet = self.find_sales_sheet(current_year)
        self.ensure_current_session_column(sales_sheet)
        
        unmatched_sessions = [row for row in sessions_data if row[3] == "NO MATCH"]
        
        self.logger.info(f"Unmatched sessions found: {len(unmatched_sessions)}")
        
        if not unmatched_sessions:
            self.logger.info("No unmatched sessions found.")
            return

        new_rows = []
        for session in unmatched_sessions:
            client_name = session[0]
            session_date = datetime.strptime(f"{session[1]} {current_year}", '%a %m/%d %Y')
            session_time = datetime.strptime(session[2], '%I:%M %p').time()
            session_datetime = datetime.combine(session_date, session_time)
            
            last_client_row_index = self.find_last_client_row(sales_sheet, client_name)
            if last_client_row_index:
                current_session = self.get_current_session(sales_sheet, last_client_row_index)
                new_current_session = self.decrement_session(current_session)
            else:
                new_current_session = "1 of 1"  # Default for new clients
            
            new_row = [
                session_datetime.strftime('%m/%d/%Y'),
                client_name,
                "Individual",
                new_current_session,
                "$XXX",
                "DUE???",
                "MONTHLY CALC??",
                "NO MATCH, INSERTED"
            ]
            new_rows.append(new_row)

        if new_rows:
            try:
                # Find the last row with data
                all_values = sales_sheet.get_all_values()
                last_row_with_data = next((i for i, row in reversed(list(enumerate(all_values, start=1))) if any(row)), 0)
                
                self.logger.info(f"Last row with data: {last_row_with_data}")
                
                # Check if we have enough rows
                total_rows = sales_sheet.rows
                rows_needed = last_row_with_data + len(new_rows)
                
                if rows_needed > total_rows:
                    rows_to_add = rows_needed - total_rows
                    sales_sheet.add_rows(rows_to_add)
                    self.logger.info(f"Added {rows_to_add} rows to the sheet.")
                
                # Now insert the new rows
                start_row = last_row_with_data + 1
                end_row = start_row + len(new_rows) - 1
                sales_sheet.update_values(f'A{start_row}:H{end_row}', new_rows)
                
                self.logger.info(f"Added {len(new_rows)} unmatched sessions to the Sales & Sessions Completed tab.")
            except Exception as e:
                self.logger.error(f"Error adding unmatched sessions: {str(e)}")
        else:
            self.logger.info("No new rows added.")

    def find_last_client_row(self, sheet, client_name):
        all_values = sheet.get_all_values()
        for idx, row in enumerate(reversed(all_values[1:]), start=2):
            if row[1].strip().lower() == client_name.strip().lower():
                return len(all_values) - idx + 1
        return None

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

    def sort_sales_sheet(self, sheet):
        self.logger.info("Sorting the Sales & Sessions Completed sheet...")
        all_values = sheet.get_all_values()
        last_row_with_data = next((i for i, row in reversed(list(enumerate(all_values, start=1))) if any(row)), 0)
        sheet.sort_range(start='A2', end=f'I{last_row_with_data}', basecolumnindex=0, sortorder='DESCENDING')
        self.logger.info("Sheet sorted successfully.")

    def ensure_current_session_column(self, sheet):
        headers = sheet.get_row(1)
        if 'CURRENT SESSION' not in headers:
            self.logger.info("'CURRENT SESSION' column not found. Adding it...")
            insert_index = headers.index('Individual') + 1 if 'Individual' in headers else len(headers)
            
            # Check if we need to expand the sheet
            if insert_index >= sheet.cols:
                cols_to_add = insert_index - sheet.cols + 1
                sheet.add_cols(cols_to_add)
                self.logger.info(f"Added {cols_to_add} column(s) to the sheet.")
            
            # Now insert the new column
            sheet.insert_cols(insert_index, 1)
            sheet.cell((1, insert_index + 1)).value = 'CURRENT SESSION'
            self.logger.info("'CURRENT SESSION' column added successfully.")
        
        # Update column indices
        self.column_indices[sheet.title] = {col.upper(): idx for idx, col in enumerate(sheet.get_row(1), start=1)}
