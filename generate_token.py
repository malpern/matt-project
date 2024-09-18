import os
from google_auth_oauthlib.flow import Flow


# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']


def main():
    if os.path.exists('token.json'):
        print("token.json already exists. Delete it if you want to generate "
              "a new one.")
        return

    flow = Flow.from_client_secrets_file(
        'client_secret.json', SCOPES)
    flow.run_local_server(port=0)

    # Save the credentials for the next run
    with open('token.json', 'w') as token:
        token.write(flow.credentials.to_json())
    print("token.json has been generated successfully.")


if __name__ == '__main__':
    main()