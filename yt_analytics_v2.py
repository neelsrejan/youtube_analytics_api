import os
import google.oauth2.credentials
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow

import json
import requests
import pandas as pd
from datetime import date
import request_arguments

#SCOPES = ["https://www.googleapis.com/auth/yt-analytics.readonly"]
SCOPES = ["https://www.googleapis.com/auth/youtube",
        "https://www.googleapis.com/auth/youtube.readonly",
        "https://www.googleapis.com/auth/yt-analytics.readonly",
        "https://www.googleapis.com/auth/youtubepartner",
        "https://www.googleapis.com/auth/yt-analytics-monetary.readonly"
        ]

API_SERVICE_NAME = "youtubeAnalytics"
API_VERSION = "v2"
CLIENT_SECRETS_FILE = os.path.join(os.getcwd(), "secrets.json")

def get_service():
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
    credentials = flow.run_console()
    return build(API_SERVICE_NAME, API_VERSION, credentials = credentials)

def execute_api_request(client_library_function, **kwargs):
    response = client_library_function(
        **kwargs
    ).execute()

    print(response)

    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
    data = response["rows"]

    response_df = pd.DataFrame(data=data, columns=col_names)
    print(response_df)

def main():
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    youtubeAnalytics = get_service()
    
    
    API_KEY = input("Please enter your API KEY: ").strip()
    channel_id = "UCWPXl--e3JsJxRG64Of_msA" #input("Please enter your channel id: ").strip()

    num_vids = request_arguments.num_vids(channel_id, API_KEY)
    vid_ids = request_arguments.get_vid_ids(num_vids, channel_id, API_KEY)
    print(len(vid_ids))
    print(vid_ids)

    '''
    today = str(date.today())
    url = f"https://www.googleapis.com/youtube/v3/channels?part=snippet&id={channel_id}&key={API_KEY}"
    date_response = json.loads(requests.get(url).text)["items"][0]["snippet"]["publishedAt"][:10]
    print(today)
    print(date_response)
    '''
    
    execute_api_request(
        youtubeAnalytics.reports().query,
            dimensions="video,country",
            endDate="2021-10-06",
            #filters="video==wDAefGS4B2Q;country==US",
            #filters="video==NaczzJYUCQc;country==US",
            #filters="video==5by1jF54fEo;country==US",
            filters="video==wDAefGS4B2Q,NaczzJYUCQc,5by1jF54fEo;country==US",
            ids="channel==MINE",
            #metrics="views,redViews",
            metrics="views,redViews,comments,likes,dislikes,videosAddedToPlaylists,videosRemovedFromPlaylists,shares,estimatedMinutesWatched,estimatedRedMinutesWatched,averageViewDuration,averageViewPercentage,annotationClickThroughRate,annotationCloseRate,annotationImpressions,annotationClickableImpressions,annotationClosableImpressions,annotationClicks,annotationCloses,cardClickRate,cardTeaserClickRate,cardImpressions,cardTeaserImpressions,cardClicks,cardTeaserClicks,subscribersGained,subscribersLost",
            startDate="2020-09-02"
            )
    

if __name__ == "__main__":
    main()
    
  
