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
        self.gc = GoogleAuth.authorize()
        creds = GoogleAuth.get_credentials()
        self.calendar_manager = CalendarManager(creds)
        self.data_processor = DataProcessor()
        self.sheet_manager = SheetManager(self.gc, SPREADSHEET_NAME, self.calendar_manager, self.data_processor)  # Pass DataProcessor
        
    def run(self):
        try:
            logging.info("Starting the script...")
            self.create_backup()
            self.clear_or_create_tabs()
            self.update_client_list()
            clients_met = self.process_calendar_events()
            if clients_met:
                self.create_sessions_tab(clients_met)
            self.reorder_tabs()
            self.sheet_manager.add_unmatched_sessions()  # Call the method from SheetManager
        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")
            import traceback
            traceback.print_exc()
        finally:
            logging.info("Script execution completed.")

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
        start_of_week, end_of_week = self.calendar_manager.get_previous_week_range()
        events = self.calendar_manager.fetch_calendar_events(start_of_week, end_of_week)
        
        client_dict = self.sheet_manager.get_client_dict()
        clients_met = self.data_processor.process_events(events, client_dict)
        self.update_last_week_tab(clients_met)
        
        return clients_met

    def update_last_week_tab(self, clients_met: Dict[str, Dict[str, Union[List[Dict], int]]]):
        self.sheet_manager.update_last_week_tab(clients_met)

    def create_sessions_tab(self, clients_met: Dict[str, Dict[str, Union[List[Dict], int]]]):
        self.sheet_manager.create_sessions_tab(clients_met)

    def reorder_tabs(self):
        self.sheet_manager.reorder_tabs()

if __name__ == "__main__":
    main()