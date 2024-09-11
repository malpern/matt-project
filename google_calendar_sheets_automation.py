import pygsheets
import autocalendar
from datetime import datetime, timedelta

def count_matt_events(calendar_service, start_date, end_date):
    events = autocalendar.get_events(calendar_service, start_date, end_date)
    return sum(1 for event in events if "Matt" in event['summary'])

def main():
    # Set up OAuth for Google Calendar
    calendar_service = autocalendar.setup_oauth()

    # Set up pygsheets
    gc = pygsheets.authorize()
    
    # Open the "matt-test" spreadsheet
    sh = gc.open("matt-test")
    wks = sh.sheet1  # Assuming you want to use the first sheet

    # Calculate date range for the last week
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)

    # Count "Matt" events
    matt_count = count_matt_events(calendar_service, start_date, end_date)

    # Update cell B2 with the count
    wks.update_value('B2', matt_count)

    print(f"Updated sheet with {matt_count} Matt events.")

if __name__ == "__main__":
    main()