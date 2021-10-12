import json
import requests
import pandas as pd
from datetime import date
import channel_reports_filters
import channel_reports

#col_names = [col_header["name"] for col_header in response["columnHeaders"]]
#data = response["rows"]

#response_df = pd.DataFrame(data=data, columns=col_names)
#print(response_df)

def main():
    
    # Intro to program
    print("Welcome to the Youtube Analytics API!")
    print("\nThis program is intended for you to be able to get as much raw data from the youtube analytics api, to do so for various kinds of reports I will be asking for responses in order to properly get the information you wish to get.")
    
    # Get all information to filter on
    '''
    API_KEY = input("Please enter your API KEY: ").strip()
    channel_id = "UCWPXl--e3JsJxRG64Of_msA" #input("Please enter your channel id: ").strip()
    
    num_vids = channel_reports_filters.num_vids(channel_id, API_KEY)
    vid_ids = channel_reports_filters.get_vid_ids(num_vids, channel_id, API_KEY)
    playlist_ids = channel_reports_filters.get_playlist_ids(channel_id, API_KEY)
    start_date, end_date = channel_reports_filters.ask_dates(channel_id, API_KEY)
    '''
    start_date = "2020-09-02"
    end_date = "2021-10-11"
    vid_ids = ["wDAefGS4B2Q", "NaczzJYUCQc", "5by1jF54fEo", "RbUn1WY3UYE"]
    groups = []

    countries, has_countries = channel_reports_filters.ask_countries()
    continents, has_continents = channel_reports_filters.ask_continents()
    subcontinents, has_subcontinents = channel_reports_filters.ask_subcontinents()
    provinces, has_provinces = channel_reports_filters.ask_provinces()
    print(countries, has_countries)
    print(continents, has_continents)
    print(subcontinents, has_subcontinents)
    print(provinces, has_provinces)
    """
    # Use Youtube Analytics API with Oauth2
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    youtubeAnalytics = get_service()
    """

    channel_reports.basic_user_activity_statistics(has_countries, has_continents, has_subcontinents, False, countries, continents, subcontinents, vid_ids, groups, start_date, end_date)
    print(filters)
    print(f"endDate={end_date}", "ids=channel==MINE", f"metrics={','.join(metrics)}", f"startDate={start_date}")
    #for num_filter in range(len(filters)):
        #print(f"dimensions={filters[num_filter]}", f"endDate={end_date}, f"filters={

    """
    response = execute_api_request(
        youtubeAnalytics.reports().query,
            dimensions="video,country",
            endDate=today,
            filters=f"{filter_videos};country==US",
            ids="channel==MINE",
            metrics="views,redViews,comments,likes,dislikes,videosAddedToPlaylists,videosRemovedFromPlaylists,shares,estimatedMinutesWatched,estimatedRedMinutesWatched,averageViewDuration,averageViewPercentage,annotationClickThroughRate,annotationCloseRate,annotationImpressions,annotationClickableImpressions,annotationClosableImpressions,annotationClicks,annotationCloses,cardClickRate,cardTeaserClickRate,cardImpressions,cardTeaserImpressions,cardClicks,cardTeaserClicks,subscribersGained,subscribersLost",
            startDate=day1
            )
    """

if __name__ == "__main__":
    main()
    
  
