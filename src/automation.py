import logging
from typing import Dict, List, Union
from datetime import datetime

from src.auth import GoogleAuth
from src.calendar_manager import CalendarManager
from src.data_processor import DataProcessor
from src.sheet_manager import SheetManager
from config.config import SPREADSHEET_NAME

def main():
    # Set up logging configuration
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.StreamHandler()  # Output logs to the console
        ]
    )

    automation = GoogleCalendarSheetsAutomation()
    automation.run()

class GoogleCalendarSheetsAutomation:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        creds = GoogleAuth.get_credentials()
        self.gc = GoogleAuth.authorize()
        self.spreadsheet = self.gc.open('Matt-data-2024-test')
        self.sheet_manager = SheetManager(
            gc=self.gc, 
            spreadsheet_name='Matt-data-2024-test',
            calendar_manager=CalendarManager(creds),
            data_processor=DataProcessor()
        )
        self.calendar_manager = self.sheet_manager.calendar_manager

    def run(self):
        try:
            self.logger.info("Starting the script...")
            self.sheet_manager.create_backup()
            self.sheet_manager.clear_or_create_tab("CLIENT LIST")
            self.sheet_manager.clear_or_create_tab("LAST WEEK")
            self.sheet_manager.update_client_list()
            clients_met = self.process_calendar_events()
            if clients_met:
                # Ensure clients_met is a dictionary
                if isinstance(clients_met, list):
                    clients_met_dict = {client['client_name']: client for client in clients_met}
                else:
                    clients_met_dict = clients_met
                self.sheet_manager.create_sessions_tab(clients_met_dict)
            # Get unmatched sessions
            unmatched_sessions = self.calendar_manager.get_unmatched_sessions()

            # Get the current year's Sales & Sessions Completed sheet
            current_year = datetime.now().year
            sales_sheet = self.sheet_manager.find_sales_sheet(current_year)
            
            # Fetch all_values from the sales sheet
            all_values = self.sheet_manager.get_all_values(sales_sheet)
            
            # Call add_unmatched_sessions with both arguments
            self.sheet_manager.add_unmatched_sessions(unmatched_sessions, all_values)
            self.reorder_tabs()
        except Exception as e:
            self.logger.error(f"An error occurred: {str(e)}")
            import traceback
            traceback.print_exc()
        finally:
            self.logger.info("Script execution completed.")

    def create_backup(self):
        logging.info("Creating backup of 'Sales & Sessions Completed' tab...")
        self.sheet_manager.create_backup()

    def clear_or_create_tabs(self):
        self.sheet_manager.clear_or_create_tab("CLIENT LIST")
        self.sheet_manager.clear_or_create_tab("LAST WEEK")
        logging.info("Tabs 'CLIENT LIST' and 'LAST WEEK' are ready.")

    def update_client_list(self):
        self.sheet_manager.update_client_list()

    def process_calendar_events(self) -> Dict[str, Dict[str, Union[List[Dict], int]]]:
        unmatched_sessions = self.calendar_manager.get_unmatched_sessions()
        return unmatched_sessions

    def update_last_week_tab(self, clients_met: Dict[str, Dict[str, Union[List[Dict], int]]]):
        self.sheet_manager.update_last_week_tab(clients_met)

    def create_sessions_tab(self, clients_met: Dict[str, Dict[str, Union[List[Dict], int]]]):
        self.sheet_manager.create_sessions_tab(clients_met)

    def reorder_tabs(self):
        self.sheet_manager.reorder_tabs()

    def process_event(self, event):
        self.logger.debug(f"Processing event: {event.get('summary', 'No summary')}")
        # Your existing code here
        client_name = self.extract_client_name(event)
        self.logger.debug(f"Extracted client name: {client_name}")
        # Rest of your code

    def extract_client_name(self, event):
        # Your existing code here
        self.logger.debug(f"Attempting to extract client name from: {event.get('summary', 'No summary')}")
        # Rest of your extraction logic

if __name__ == "__main__":
    main()
