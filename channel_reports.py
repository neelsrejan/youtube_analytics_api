
import os
import google.oauth2.credentials
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow

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

    return response

def basic_user_activity_statistics(has_countries, has_continents, has_subcontinents, has_groups, countries, continents, subcontinents, vid_ids, groups, start_date, end_date):
    
    metrics = ["views", "redViews", "comments", "likes", "dislikes", "videosAddedToPlaylists", "videosRemovedFromPlaylists", "shares", "estimatedMinutesWatched", "estimatedRedMinutesWatched", "averageViewDuration", "averageViewPercentage", "annotationClickThroughRate", "annotationCloseRate", "annotationImpressions", "annotationClickableImpressions", "annotationClosableImpressions", "annotationClicks", "annotationCloses", "cardClickRate", "cardTeaserClickRate", "cardImpressions", "cardTeaserImpressions", "cardClicks", "cardTeaserClicks", "subscribersGained", "subscribersLost"]

    filters = []
    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    filters_1 = ["", "country", "continent", "subContinent"]
    filters_2 = ["", "video", "group"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)

    for i in range(len(filters_1)):
        for j in range(len(filters_2)):
            if i != 0 and j != 0:
                #filters.append(filters_1[i]+ "," + filters_2[j])
                #curr_filter_1 = filters_1[i]
                #curr_filter_2 = filters_2[j]
                if filters_1[i] == "country" and filters_2[j] == "video":
                    for country in countries:
                        print(f"dimensions={filters_1[i] + ',' + filters_2[j]}")
                        print(f"endDate={end_date}")
                        print(f"filters={filters_1[i]}=={country},{filters_2[j]}=={','.join(vid_ids)}")
                        print("ids=channel==MINE")
                        print(f"metrics={','.join(metrics)}")
                        print(f"startDate={start_date}")
                elif filters_1[i] == "country" and filters_2[j] == "group":
                    for country in countries:
                        print(f"dimensions={filters_1[i] + ',' + filters_2[j]}")
                        print(f"endDate={end_date}")
                        print(f"filters={filters_1[i]}=={country},{filters_2[j]}=={','.join(groups)}")
                        print("ids=channel==MINE")
                        print(f"metrics={','.join(metrics)}")
                        print(f"startDate={start_date}")
                elif filters_1[i] == "continent" and filters_2[j] == "video":
                    for continent in continents:
                        print(f"dimensions={filters_1[i] + ',' + filters_2[j]}")
                        print(f"endDate={end_date}")
                        print(f"filters={filters_1[i]}=={continent},{filters_2[j]}=={','.join(vid_ids)}")
                        print("ids=channel==MINE")
                        print(f"metrics={','.join(metrics)}")
                        print(f"startDate={start_date}")
                elif filters_1[i] == "continent" and filters_2[j] == "group":
                    for continent in continents:
                        print(f"dimensions={filters_1[i] + ',' + filters_2[j]}")
                        print(f"endDate={end_date}")
                        print(f"filters={filters_1[i]}=={continent},{filters_2[j]}=={','.join(groups)}")
                        print("ids=channel==MINE")
                        print(f"metrics={','.join(metrics)}")
                        print(f"startDate={start_date}")
                elif filters_1[i] == "subContinent" and filters_2[j] == "video":
                    for subcontinent in subcontinents:
                        print(f"dimensions={filters_1[i] + ',' + filters_2[j]}")
                        print(f"endDate={end_date}")
                        print(f"filters={filters_1[i]}=={subcontinent},{filters_2[j]}=={','.join(vid_ids)}")
                        print("ids=channel==MINE")
                        print(f"metrics={','.join(metrics)}")
                        print(f"startDate={start_date}")
                elif filters_1[i] == "subContinent" and filters_2[j] == "group":
                    for subcontinent in subcontinents:
                        print(f"dimensions={filters_1[i] + ',' + filters_2[j]}")
                        print(f"endDate={end_date}")
                        print(f"filters={filters_1[i]}=={subcontinent},{filters_2[j]}=={','.join(groups)}")
                        print("ids=channel==MINE")
                        print(f"metrics={','.join(metrics)}")
                        print(f"startDate={start_date}")
            elif i == 0 and j != 0:
                #filters.append(filters_2[j])
                if filters_2[j] == "video":
                    print(f"dimensions={filters_2[j]}")
                    print(f"endDate={end_date}")
                    print(f"filters={filters_2[j]}=={','.join(vid_ids)}")
                    print("ids=channel==MINE")
                    print(f"metrics={','.join(metrics)}")
                    print(f"startDate={start_date}")
                elif filters_2[j] == "group":
                    print(f"dimensions={filters_2[j]}")
                    print(f"endDate={end_date}")
                    print(f"filters={filters_2[j]}=={','.join(groups)}")
                    print("ids=channel==MINE")
                    print(f"metrics={','.join(metrics)}")
                    print(f"startDate={start_date}")
            elif i != 0 and j == 0:
                #filters.append(filters_1[i])
                if filters_1[i] == "country":
                    for country in countries:
                        print(f"dimensions={filters_1[i]}")
                        print(f"endDate={end_date}")
                        print(f"filters={filters_1[i]}=={country}")
                        print("ids=channel==MINE")
                        print(f"metrics={','.join(metrics)}")
                        print(f"startDate={start_date}")
                elif filters_1[i] == "continent":
                    for continent in continents:
                        print(f"dimensions={filters_1[i]}")
                        print(f"endDate={end_date}")
                        print(f"filters={filters_1[i]}=={continent}")
                        print("ids=channel==MINE")
                        print(f"metrics={','.join(metrics)}")
                        print(f"startDate={start_date}")
                elif filters_1[i] == "subContinent":
                    for continent in continents:
                        print(f"dimensions={filters_1[i]}")
                        print(f"endDate={end_date}")
                        print(f"filters={filters_1[i]}=={continent}")
                        print("ids=channel==MINE")
                        print(f"metrics={','.join(metrics)}")
                        print(f"startDate={start_date}")
    return

# filters, metrics, countries, continents = basic_user_activity_statistics(False)
