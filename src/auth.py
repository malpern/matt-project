import os
import logging
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pygsheets
from config.config import SCOPES, TOKEN_FILE, CLIENT_SECRET_FILE
from datetime import datetime, timedelta
from time import sleep

class GoogleAuth:
    _credentials = None
    _credentials_expiration = None

    @staticmethod
    def get_credentials() -> Credentials:
        if GoogleAuth._credentials and GoogleAuth._credentials_expiration > datetime.now():
            logging.info("Using cached credentials.")
            return GoogleAuth._credentials

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

        GoogleAuth._credentials = creds
        GoogleAuth._credentials_expiration = datetime.now() + timedelta(minutes=50)  # Cache for 50 minutes
        return creds

    @staticmethod
    def authorize():
        backoff_time = 1  # Initial backoff time in seconds
        max_backoff_time = 60  # Maximum backoff time in seconds
        num_retries = 5  # Maximum number of retries

        for _ in range(num_retries):
            try:
                return pygsheets.authorize(client_secret=CLIENT_SECRET_FILE)
            except Exception as e:
                logging.warning(f"Error authorizing: {str(e)}")
                logging.info(f"Retrying in {backoff_time} seconds...")
                sleep(backoff_time)
                backoff_time = min(backoff_time * 2, max_backoff_time)

        logging.error("Failed to authorize after multiple retries.")
        raise Exception("Unable to authorize the application.")