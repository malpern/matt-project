import logging
from typing import Dict, List, Union
from datetime import datetime

from src.auth import GoogleAuth
from src.calendar_manager import CalendarManager
from src.data_processor import DataProcessor
from src.sheet_manager import SheetManager
from config.config import SPREADSHEET_NAME
from src.revenue_summary import RevenueSummary

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
        gc = GoogleAuth.authorize()
        self.calendar_manager = CalendarManager(creds)
        self.data_processor = DataProcessor()
        self.sheet_manager = SheetManager(gc, SPREADSHEET_NAME, self.calendar_manager, self.data_processor)
        # Initialize RevenueSummary
        self.revenue_summary = RevenueSummary(self.sheet_manager)

    def run(self):
        """Main execution method."""
        try:
            self.logger.info("Starting the script...")
            
            # Create backup of existing sheet
            self.sheet_manager.create_backup()
            
            # Clear or create necessary tabs
            self.sheet_manager.clear_or_create_tab("CLIENT LIST")
            self.sheet_manager.clear_or_create_tab("LAST WEEK")
            self.sheet_manager.clear_or_create_tab("SESSIONS")
            
            # Update client list with sessions count
            self.sheet_manager.update_client_list()
            
            # Process calendar events and update LAST WEEK and SESSIONS tabs
            unmatched_sessions = self.calendar_manager.get_unmatched_sessions()
            
            if unmatched_sessions:
                # Update LAST WEEK tab with session counts and dates
                self.sheet_manager.update_last_week_tab(unmatched_sessions)
                
                # Create SESSIONS tab with chronological listing
                self.sheet_manager.create_sessions_tab(unmatched_sessions)
                
                # Get the current sales sheet data and add unmatched sessions
                sales_sheet = self.sheet_manager.find_sales_sheet(datetime.now().year)
                if sales_sheet:
                    all_values = self.sheet_manager.get_all_values(sales_sheet)
                    self.sheet_manager.add_unmatched_sessions(unmatched_sessions, all_values)
            
            # Reorder tabs
            self.sheet_manager.reorder_tabs([
                "Sales & Sessions Completed 2024",
                "LAST WEEK",
                "SESSIONS",
                "CLIENT LIST"
            ])
            
            self.logger.info("Script execution completed successfully.")
        except Exception as e:
            self.logger.error(f"An error occurred: {str(e)}")
            import traceback
            traceback.print_exc()

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
