import logging
from src.automation import GoogleCalendarSheetsAutomation

logging.basicConfig(
    level=logging.INFO,  # Changed from DEBUG to INFO
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)

if __name__ == "__main__":
    automation = GoogleCalendarSheetsAutomation()
    automation.run()
