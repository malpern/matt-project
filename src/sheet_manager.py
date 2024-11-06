import logging
from datetime import datetime, timedelta
from typing import Dict, List, Union
import pygsheets
from collections import defaultdict, Counter
import time
from googleapiclient.errors import HttpError
import sys
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)

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
        """Clear the specified tab or create it if it doesn't exist."""
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

    def find_sales_sheet(self, year: int) -> Union[pygsheets.Worksheet, None]:
        """Find the sales sheet for a given year."""
        sheet_name = f"Sales & Sessions Completed {year}"
        try:
            sheet = self.spreadsheet.worksheet('title', sheet_name)
            self.logger.info(f"Found '{sheet_name}' tab.")
            return sheet
        except Exception as e:
            self.logger.warning(f"Error finding sheet '{sheet_name}': {str(e)}")
            return None

    def create_backup(self):
        """Create a backup of the "Sales & Sessions Completed" tab."""
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
        """Update the "CLIENT LIST" tab with unique client names and session counts."""
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

    def update_last_week_tab(self, unmatched_sessions: List[Dict]):
        """Update the "LAST WEEK" tab with the clients met and their session details."""
        last_week_sheet = self.clear_or_create_tab("LAST WEEK")
        
        self.logger.info(f"Updating LAST WEEK tab with {len(unmatched_sessions)} clients")
        
        if not unmatched_sessions:
            self.logger.info("No clients met with in the previous week.")
            return

        # Group sessions by client
        clients_met = {}
        for session in unmatched_sessions:
            client_name = session['client_name']
            if client_name not in clients_met:
                clients_met[client_name] = {
                    'sessions': [],
                    'count': 0
                }
            clients_met[client_name]['sessions'].append(session)
            clients_met[client_name]['count'] += 1

        # Create headers
        headers = ['CLIENT NAME', 'SESSIONS COMPLETED', 'SESSION DATES']
        last_week_sheet.update_values('A1', [headers])
        last_week_sheet.frozen_rows = 1

        # Prepare data rows
        update_data = []
        for client_name, data in clients_met.items():
            # Sort sessions by date
            sorted_sessions = sorted(data['sessions'], key=lambda x: x['date'])
            
            # Format dates as "Day MM/DD"
            formatted_dates = []
            for session in sorted_sessions:
                try:
                    date_obj = datetime.strptime(session['date'], '%m/%d/%Y')
                    formatted_date = date_obj.strftime('%a %m/%d')
                    formatted_dates.append(formatted_date)
                except ValueError:
                    self.logger.warning(f"Invalid date format for session: {session['date']}")
                    formatted_dates.append(session['date'])

            row = [
                client_name,
                data['count'],
                ', '.join(formatted_dates)
            ]
            update_data.append(row)

        if update_data:
            # Sort by number of sessions (descending)
            update_data.sort(key=lambda x: x[1], reverse=True)
            last_week_sheet.update_values('A2', update_data)
            self.logger.info(f"Updated {len(update_data)} client(s) in the LAST WEEK tab.")
        else:
            self.logger.info("No data to add to the LAST WEEK tab.")

    def create_sessions_tab(self, unmatched_sessions: List[Dict]):
        """Create the SESSIONS tab with chronological listing of all sessions."""
        sessions_sheet = self.clear_or_create_tab("SESSIONS")
        
        if not unmatched_sessions:
            self.logger.info("No sessions to add to SESSIONS tab.")
            return

        # Set up headers
        headers = ['DATE', 'TIME', 'CLIENT NAME', 'STATUS']
        sessions_sheet.update_values('A1', [headers])
        sessions_sheet.frozen_rows = 1

        # Sort sessions by date
        sorted_sessions = sorted(unmatched_sessions, 
                               key=lambda x: datetime.strptime(x['date'], '%m/%d/%Y'))

        # Prepare data rows
        update_data = []
        for session in sorted_sessions:
            row = [
                session['date'],
                session['time'],
                session['client_name'],
                'UNMATCHED'  # Default status
            ]
            update_data.append(row)

        if update_data:
            sessions_sheet.update_values('A2', update_data)
            self.logger.info(f"Added {len(update_data)} sessions to the SESSIONS tab.")
        else:
            self.logger.info("No sessions to add to the SESSIONS tab.")

        # Auto-resize columns
        try:
            sessions_sheet.adjust_column_width(1, len(headers))
        except Exception as e:
            self.logger.warning(f"Failed to auto-resize columns: {str(e)}")

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
        """
        Update session count based on the specified rules:
        - "7 of 12" -> "8 of 12"
        - "2 of 3" -> "3 of 3"
        - "1 of 1" -> "2 of 1"
        """
        logger.debug(f"Incrementing session: '{current_session}'")
        try:
            current, total = map(int, current_session.strip().split(' of '))
            logger.debug(f"Parsed session - Current: {current}, Total: {total}")

            # Increment the current session number
            new_current = current + 1
            logger.debug(f"Incremented current from {current} to {new_current}")

            new_session = f"{new_current} of {total}"
            logger.debug(f"New session string: '{new_session}'")
            return new_session
        except ValueError as e:
            logger.error(f"ValueError: {e} - Invalid session format: '{current_session}'. Defaulting to '1 of 1'")
            return "1 of 1"

    def similar(self, a: str, b: str, threshold: float = 0.85) -> bool:
        """
        Return True if strings are similar above threshold.
        Handles None values and strips/lowercases strings before comparison.
        """
        if not a or not b:  # Handle None or empty strings
            return False
        return SequenceMatcher(None, a.strip().lower(), b.strip().lower()).ratio() > threshold

    def get_canonical_name(self, client_name: str, all_values: List[List]) -> str:
        """
        Find the most common spelling of a client name from historical entries.
        Returns original name if no matches found.
        """
        # Create a case-insensitive dictionary to track spellings
        name_variants = {}  # lowercase -> original case mapping
        name_counts = Counter()  # count occurrences case-insensitively
        
        self.logger.debug(f"\nSearching for variants of '{client_name}':")
        
        for row in all_values[1:]:  # Skip header row
            if row and len(row) > 1:
                current_name = row[1].strip()
                if self.similar(current_name, client_name):
                    lowercase_name = current_name.lower()
                    self.logger.debug(f"Found variant: '{current_name}' (lowercase: '{lowercase_name}')")
                    name_counts[lowercase_name] += 1
                    # Keep first occurrence of a particular capitalization
                    if lowercase_name not in name_variants:
                        name_variants[lowercase_name] = current_name
        
        if not name_counts:
            self.logger.debug("No variants found")
            return client_name
            
        # Get the most common name (case-insensitive)
        most_common_lowercase = name_counts.most_common(1)[0][0]
        canonical_name = name_variants[most_common_lowercase]
        
        self.logger.info(f"Name variants found for '{client_name}':")
        self.logger.info(f"- Variants: {dict(name_variants)}")
        self.logger.info(f"- Counts: {dict(name_counts)}")
        self.logger.info(f"- Selected canonical form: '{canonical_name}'")
        
        return canonical_name

    def add_unmatched_sessions(self, unmatched_sessions: List[Dict], all_values: List[List]):
        """Add unmatched sessions to the sales sheet."""
        self.logger.info(f"Adding {len(unmatched_sessions)} unmatched sessions to the sheet.")
        
        sales_sheet = self.find_sales_sheet(datetime.now().year)
        last_row = self.find_last_row_with_data(sales_sheet)
        self.logger.info(f"Last row with data: {last_row}")
        
        new_rows = []
        for session in unmatched_sessions:
            original_name = session['client_name']
            # Get the canonical name before processing
            client_name = self.get_canonical_name(original_name, all_values)
            date = session['date']
            
            # Find the last entry for this client
            last_client_data = None
            for row in reversed(all_values):
                if row and len(row) > 1 and self.similar(row[1], client_name):
                    last_client_data = row
                    break
            
            # Set default values
            last_price = "???"
            current_session = "1 of 1"
            client_status = "NEW CLIENT" if not last_client_data else ""
            payment_status = ""
            
            if last_client_data:
                if len(last_client_data) > 4 and last_client_data[4]:
                    last_price = last_client_data[4]
                if len(last_client_data) > 3 and last_client_data[3]:
                    try:
                        current, total = map(int, last_client_data[3].split(' of '))
                        current_session = f"{current + 1} of {total}"
                        if current + 1 >= total and last_price != "???":
                            price = float(last_price.replace('$', ''))
                            total_due = int(price * total)
                            payment_status = f'DUE: ${total_due}'
                    except (ValueError, AttributeError):
                        current_session = "1 of 1"
            
            new_row = [
                date,
                client_name,  # Use the canonical name here
                'Individual',
                current_session,
                last_price,
                payment_status,
                'MONTHLY CALC??',
                client_status
            ]
            new_rows.append(new_row)
            self.logger.info(f"Added row {last_row + len(new_rows)} for '{client_name}': {new_row}")
        
        # Extend the sheet if necessary
        if new_rows:
            needed_rows = last_row + len(new_rows)
            current_rows = sales_sheet.rows
            if needed_rows > current_rows:
                rows_to_add = needed_rows - current_rows + 100  # Add extra buffer
                self.logger.info(f"Extending sheet by {rows_to_add} rows")
                sales_sheet.add_rows(rows_to_add)
            
            # Update the sheet with new rows
            range_to_update = f'A{last_row + 1}:H{last_row + len(new_rows)}'
            sales_sheet.update_values(range_to_update, new_rows)
            self.logger.info(f"Updated Google Sheet with {len(new_rows)} new rows, from row {last_row + 1} to {last_row + len(new_rows)}.")
        
        return new_rows

    def find_last_client_row(self, all_values, client_col, client_name):
        search_client_name = client_name.strip().lower()
        logger.debug(f"Searching for client: '{client_name}' in column index {client_col}")
        for i in range(len(all_values) - 1, 0, -1):  # Start from the last row, go up
            if client_col < len(all_values[i]):
                sheet_client_name = all_values[i][client_col].strip().lower()
                logger.debug(f"Comparing '{sheet_client_name}' with '{search_client_name}' at row {i+1}")
                if sheet_client_name == search_client_name:
                    date = all_values[i][0]
                    current_session = all_values[i][3] if len(all_values[i]) > 3 else "N/A"  # Column D is index 3
                    logger.info(f"Found client: '{client_name}', Row: {i+1}, Date: {date}, Current Session: '{current_session}'")
                    return i + 1
        
        logger.error(f'Client name "{client_name}" not found')
        return None

    def get_current_session_value(self, all_values, row_index, current_session_col):
        try:
            return all_values[row_index - 1][current_session_col]
        except IndexError:
            self.logger.warning(f"'CURRENT SESSION' column not found for row {row_index}. Using default value.")
            return "1 of 1"  # Default value

    def create_new_row(self, session_date, client_name, new_current_session):
        return [
            session_date.strftime('%m/%d/%Y'),
            client_name,
            "Individual",
            new_current_session,
            "$XXX",
            "DUE???",
            "MONTHLY CALC??",
            "NO MATCH, INSERTED"
        ]

    def add_new_rows_to_sheet(self, sheet, new_rows):
        last_row = sheet.rows
        rows_to_add = len(new_rows)
        if last_row + rows_to_add > sheet.rows:
            sheet.add_rows(rows_to_add)
            self.logger.info(f"Added {rows_to_add} new row(s) to sheet")
        
        self.api_call_with_retry(sheet.update_values, f'A{last_row + 1}:H{last_row + rows_to_add}', new_rows)
        self.logger.info(f"Inserted {rows_to_add} new row(s)")

    def reorder_tabs(self, tab_order: List[str] = None):
        """Reorder the tabs in the spreadsheet according to the specified order."""
        if tab_order is None:
            tab_order = [
                "Sales & Sessions Completed 2024",
                "LAST WEEK",
                "SESSIONS",
                "CLIENT LIST"
            ]

        try:
            # Get all worksheets
            worksheets = {ws.title: ws for ws in self.spreadsheet.worksheets()}
            
            # Reorder tabs using index property
            for index, tab_name in enumerate(tab_order):
                if tab_name in worksheets:
                    worksheet = worksheets[tab_name]
                    if worksheet.index != index:
                        worksheet.index = index
                        self.logger.info(f"Moved '{tab_name}' to position {index + 1}")
                else:
                    self.logger.warning(f"Tab '{tab_name}' not found in spreadsheet")
            
            self.logger.info("Tabs reordered successfully")
        except Exception as e:
            self.logger.error(f"Error reordering tabs: {str(e)}")

    def sort_sales_sheet(self, sheet):
        """Sort the "Sales & Sessions Completed" tab by date in descending order."""
        self.logger.info("Sorting the Sales & Sessions Completed sheet...")
        all_values = sheet.get_all_values()
        last_row_with_data = next((i for i, row in reversed(list(enumerate(all_values, start=1))) if any(row)), 0)
        sheet.sort_range(start='A2', end=f'I{last_row_with_data}', basecolumnindex=0, sortorder='DESCENDING')
        self.logger.info("Sheet sorted successfully.")

    def ensure_current_session_column(self, sheet):
        """Ensure the "CURRENT SESSION" column exists in the "Sales & Sessions Completed" tab."""
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

    def api_call_with_retry(self, func, *args, **kwargs):
        max_retries = 5
        retry_delay = 1
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except HttpError as e:
                if e.resp.status == 429:
                    if attempt < max_retries - 1:
                        sleep_time = retry_delay * (2 ** attempt)
                        self.logger.warning(f"Rate limit hit. Retrying in {sleep_time} seconds...")
                        time.sleep(sleep_time)
                    else:
                        raise
                else:
                    raise

    def get_all_values(self, sheet):
        if not isinstance(sheet, pygsheets.Worksheet):
            self.logger.error("The 'sheet' argument must be an instance of pygsheets.Worksheet.")
            raise TypeError("Invalid sheet type.")
        return self.api_call_with_retry(sheet.get_all_values)

    def update_values(self, sheet, range, values):
        return self.api_call_with_retry(sheet.update_values, range, values)

    def process_events(self, events):
        for event in events:
            client_name = self.extract_client_name(event)
            if client_name:
                # Process the event with the found client name
                # Your existing logic here
                pass
            else:
                # Log the error and continue with the next event
                logger.error(f'Client name "{event.get("summary", "")}" not found')

    def extract_client_name(self, event):
        # Your existing client name extraction logic here
        # If no client name is found, return None instead of raising an exception
        summary = event.get('summary', '')
        # Your extraction logic here
        if not client_name:
            return None
        return client_name

    def some_internal_method(self):
        # Example method within SheetManager
        sheet = self.get_sheet("Some Tab")
        all_values = self.get_all_values(sheet)
        # Continue processing...

    def find_last_row_with_data(self, worksheet):
        """Find the last row in the worksheet that contains data."""
        values = worksheet.get_all_values()
        for i in range(len(values) - 1, -1, -1):
            if any(values[i]):
                return i + 1  # +1 because sheet rows are 1-indexed
        return 1  # Return 1 if the sheet is empty

    def calculate_monthly_revenue(self):
        """Calculate and update monthly revenue totals in the Sales & Sessions Completed tab."""
        try:
            # Get the Sales & Sessions Completed sheet for current year
            current_year = datetime.now().year
            sales_sheet = self.find_sales_sheet(current_year)
            self.logger.info(f"Calculating monthly revenue for {sales_sheet.title}")

            # Get all values
            data = sales_sheet.get_all_values()
            if len(data) < 2:  # Check if there's data beyond headers
                self.logger.info("No data found for revenue calculation")
                return

            # Find the date and price columns
            headers = data[0]
            try:
                date_col = headers.index('Date')
                price_col = headers.index('PRICE PER SESSION')
            except ValueError as e:
                self.logger.error(f"Required column not found: {e}")
                return

            # Group data by month and calculate totals
            monthly_totals = {}
            last_row_indices = {}
            
            for row_idx, row in enumerate(data[1:], start=1):  # Skip header row
                try:
                    # Parse date
                    date_str = row[date_col]
                    if not date_str:
                        continue
                        
                    date = datetime.strptime(date_str, '%m/%d/%Y')
                    month_key = date.strftime('%Y-%m')
                    
                    # Get price value
                    price_str = row[price_col]
                    if price_str:
                        # Remove '$' and convert to float
                        price = float(price_str.replace('$', '').replace(',', ''))
                        
                        # Add to monthly total
                        monthly_totals[month_key] = monthly_totals.get(month_key, 0) + price
                        
                        # Update last row index for this month
                        last_row_indices[month_key] = row_idx + 1  # +1 because row_idx is 0-based

                except (ValueError, IndexError) as e:
                    self.logger.warning(f"Error processing row {row_idx + 1}: {e}")
                    continue

            # Update the monthly totals in column G of the last row of each month
            batch_updates = []
            for month_key, total in monthly_totals.items():
                if month_key in last_row_indices:
                    row_num = last_row_indices[month_key]
                    formatted_total = f"${total:,.2f}"
                    batch_updates.append({
                        'range': f'G{row_num}',
                        'values': [[formatted_total]]
                    })
                    self.logger.info(f"Month {month_key}: Total ${total:,.2f} (Row {row_num})")

            # Apply all updates in a single batch operation
            if batch_updates:
                sales_sheet.batch_update(batch_updates)
                self.logger.info(f"Updated monthly totals for {len(batch_updates)} months")
            else:
                self.logger.info("No monthly totals to update")

        except Exception as e:
            self.logger.error(f"Error calculating monthly revenue: {e}")
            raise



