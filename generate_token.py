import os
from typing import List
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials


# If modifying these scopes, delete the file token.json.
SCOPES: List[str] = ['https://www.googleapis.com/auth/calendar.readonly']


def main() -> None:
    if os.path.exists('token.json'):
        print("token.json already exists. Delete it if you want to generate "
              "a new one.")
        return

    flow: Flow = Flow.from_client_secrets_file(
        'client_secret.json', SCOPES)
    flow.run_local_server(port=0)

    # Save the credentials for the next run
    credentials: Credentials = flow.credentials
    with open('token.json', 'w') as token:
        token.write(credentials.to_json())
    print("token.json has been generated successfully.")


if __name__ == '__main__':
    main()