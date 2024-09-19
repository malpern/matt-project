import os
import logging
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pygsheets
from config.config import SCOPES, TOKEN_FILE, CLIENT_SECRET_FILE

class GoogleAuth:
    @staticmethod
    def get_credentials() -> Credentials:
        logging.info("Checking for existing credentials...")
        creds = None
        if os.path.exists(TOKEN_FILE):
            logging.info(f"Found {TOKEN_FILE}, loading credentials...")
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                logging.info("Refreshing expired credentials...")
                creds.refresh(Request())
            else:
                logging.info("No valid credentials found. Starting new auth flow...")
                flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
                creds = flow.run_local_server(port=0)
            logging.info("Saving new credentials...")
            with open(TOKEN_FILE, 'w') as token:
                token.write(creds.to_json())

        return creds

    @staticmethod
    def authorize():
        return pygsheets.authorize(client_secret=CLIENT_SECRET_FILE)