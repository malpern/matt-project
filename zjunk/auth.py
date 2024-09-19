import os
import logging
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']

class GoogleAuth:
    @staticmethod
    def get_credentials() -> Credentials:
        logging.info("Checking for existing credentials...")
        creds = None
        if os.path.exists('config/token.json'):
            logging.info("Found token.json, loading credentials...")
            creds = Credentials.from_authorized_user_file('config/token.json', SCOPES)
        
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                logging.info("Refreshing expired credentials...")
                creds.refresh(Request())
            else:
                logging.info("No valid credentials found. Starting new auth flow...")
                flow = InstalledAppFlow.from_client_secrets_file('config/client_secret.json', SCOPES)
                creds = flow.run_local_server(port=0)
            logging.info("Saving new credentials...")
            with open('config/token.json', 'w') as token:
                token.write(creds.to_json())

        return creds