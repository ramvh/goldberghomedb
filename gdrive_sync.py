'''
- Store all platform orders in separate shared gdrive folders (shopee/lazada/shopify/amazon)
- Use rclone to copy raw orders to local directory
- Use python to connect to and load data into duckdb - raw data
- Use dbt to transform data
'''

import io
import os
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

TOKEN_FILE = "/Users/haleymar/Documents/gbh_discounted_price_automation/token.json"
OAUTH_CLIENT_FILE = "/Users/haleymar/Documents/HM Local Git Repo/google credentials/client_secret_157659633244-2vif5jn8qsfpj8hmklukg3l3cmqqh30i.apps.googleusercontent.com.json"  # OAuth client file

def get_service():
    # Standard Google Auth boilerplate
    scopes = ['https://www.googleapis.com/auth/drive.readonly']
    
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, scopes)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(OAUTH_CLIENT_FILE, scopes)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            f.write(creds.to_json())

    return build('drive', 'v3', credentials=creds)


def download_folder(folder_id, local_path):
    service = get_service()
    # List files in the specific platform folder
    results = service.files().list(
        q=f"'{folder_id}' in parents and trashed = false",
        fields="files(id, name)"
    ).execute()
    
    for file in results.get('files', []):
        file['name'] = file['name'] + '.csv'
        request = service.files().export(fileId=file['id'], mimeType='text/csv')
        fh = io.FileIO(os.path.join(local_path, file['name']), 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        print(f"✅ Downloaded {file['name']}")