import os
import google.oauth2.credentials
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow

class Auth():

    def get_service(self):
        flow = InstalledAppFlow.from_client_secrets_file(self.CLIENT_SECRETS_FILE, self.SCOPES)
        credentials = flow.run_console()
        return build(self.API_SERVICE_NAME, self.API_VERSION, credentials = credentials)

    def execute_api_request(self, client_library_function, **kwargs):
        response = client_library_function(
            **kwargs
        ).execute()

        return response

    def auth(self):
        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
        self.youtubeAnalytics = self.get_service()
    
    def get_groups(self):
        groups_response = self.execute_api_request(
            self.youtubeAnalytics.groups().list,
            mine=True
        )
        if len(groups_response["items"]) != 0:
            self.groups.append(groups_response["items"])
            self.has_groups = True
        return
