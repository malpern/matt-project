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

    def get_unmatched_sessions(self) -> List[Dict]:
        """Get unmatched sessions from the previous week."""
        start_of_week, end_of_week = self.get_previous_week_range()
        events = self.fetch_calendar_events(start_of_week, end_of_week)
        
        unmatched_sessions = []
        for event in events:
            event_date_str = event['start'].get('dateTime', event['start'].get('date'))
            event_date = datetime.fromisoformat(event_date_str.replace('Z', '+00:00')).date()
            event_time = datetime.fromisoformat(event_date_str.replace('Z', '+00:00')).strftime('%I:%M %p')
            
            client_name = self.extract_client_name(event)
            if client_name:
                unmatched_sessions.append({
                    'date': event_date.strftime('%m/%d/%Y'),
                    'time': event_time,
                    'client_name': client_name
                })
        
        return unmatched_sessions

    def extract_client_name(self, event):
        """Extract client name from event title and description with fuzzy matching."""
        event_title = event.get('summary', '')
        event_description = event.get('description', '')
        
        # Combine title and description for searching
        text_to_search = f"{event_title} {event_description}"
        
        # TODO: Implement fuzzy matching against the client list
        # This should be integrated with the SheetManager to get the current client list
        # and use fuzzy matching to find the best match
        
        # For now, return the first two words of the title as a basic implementation
        words = event_title.split()
        if len(words) >= 2:
            return ' '.join(words[:2]).strip()
        return None
