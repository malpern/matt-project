import logging
from datetime import datetime
from typing import Dict, List, Union
from collections import defaultdict

class DataProcessor:
    @staticmethod
    def process_events(events: List[Dict], client_dict: Dict[str, int]) -> Dict[str, Dict[str, Union[List[Dict], int]]]:
        logging.info("Processing events to find client matches...")
        clients_met = defaultdict(lambda: {'events': [], 'sessions': 0})
        for event in events:
            event_date_str = event['start'].get('dateTime', event['start'].get('date'))
            try:
                event_date = DataProcessor.parse_date(event_date_str)
            except ValueError:
                logging.warning(f"Unable to parse event date '{event_date_str}'. Skipping event.")
                continue

            event_title = event.get('summary', '')
            event_description = event.get('description', '')

            for client in client_dict:
                client_parts = client.lower().split()
                if any(part in event_title.lower() or part in event_description.lower() for part in client_parts):
                    clients_met[client]['events'].append(event)
                    clients_met[client]['sessions'] += 1
                    break

        return clients_met

    @staticmethod
    def parse_date(date_str: str) -> datetime.date:
        date_str = date_str.strip()
        if not date_str:
            raise ValueError("Empty date string")
        try:
            return datetime.strptime(date_str[:10], '%Y-%m-%d').date()
        except ValueError:
            return datetime.strptime(date_str, '%m/%d/%Y').date()