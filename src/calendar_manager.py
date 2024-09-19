import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from googleapiclient.discovery import build
from config.config import CALENDAR_ID

class CalendarManager:
    def __init__(self, creds):
        self.service = build('calendar', 'v3', credentials=creds)
        self.calendar_id = CALENDAR_ID  # Move this to a config file later

    @staticmethod
    def get_previous_week_range() -> Tuple[datetime, datetime]:
        today = datetime.now().date()
        start_of_week = today - timedelta(days=today.weekday() + 7)
        end_of_week = start_of_week + timedelta(days=6)
        return start_of_week, end_of_week

    def fetch_calendar_events(self, start_of_week: datetime, end_of_week: datetime) -> List[Dict]:
        logging.info(f"Fetching events from {start_of_week} to {end_of_week}...")
        events_result = self.service.events().list(
            calendarId=self.calendar_id,
            timeMin=start_of_week.isoformat() + 'T00:00:00Z',
            timeMax=end_of_week.isoformat() + 'T23:59:59Z',
            singleEvents=True,
            orderBy='startTime'
        ).execute()
        events = events_result.get('items', [])
        logging.info(f"Found {len(events)} events.")
        return events