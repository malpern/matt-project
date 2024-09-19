import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

CALENDAR_ID = 'f4lathletics@gmail.com'

class CalendarManager:
    def __init__(self, service):
        self.service = service

    @staticmethod
    def get_previous_week_range() -> Tuple[datetime, datetime]:
        today = datetime.now().date()
        start_of_week = today - timedelta(days=today.weekday() + 7)
        end_of_week = start_of_week + timedelta(days=6)
        return start_of_week, end_of_week

    def fetch_calendar_events(self, start_of_week: datetime, end_of_week: datetime) -> List[Dict]:
        logging.info(f"Fetching events from {start_of_week} to {end_of_week}...")
        events_result = self.service.events().list(
            calendarId=CALENDAR_ID,
            timeMin=start_of_week.isoformat() + 'T00:00:00Z',
            timeMax=end_of_week.isoformat() + 'T23:59:59Z',
            singleEvents=True,
            orderBy='startTime'
        ).execute()
        events = events_result.get('items', [])
        logging.info(f"Found {len(events)} events.")
        return events