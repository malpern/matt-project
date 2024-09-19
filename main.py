import logging
from src.automation import GoogleCalendarSheetsAutomation

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    automation = GoogleCalendarSheetsAutomation()
    automation.run()