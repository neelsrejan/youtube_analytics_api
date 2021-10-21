import os
from datetime import date
from YT_ANALYTICS import YT_ANALYTICS

def main():
    
    # Intro to program
    print("Welcome to the Youtube Analytics API!")
    print("\nThis program is intended for you to be able to get as much raw data from the youtube analytics api, to do so for various kinds of reports I will be asking for responses in order to properly get the information you wish to get.")

    # Get all information to filter on 
    API_KEY = input("Please enter your API KEY: ").strip()
    channel_id = "UCWPXl--e3JsJxRG64Of_msA" #input("Please enter your channel id: ").strip()

    YT = YT_ANALYTICS(API_KEY, channel_id)
    YT.get_filter_info()

    # Create directories for saving data into
    if not os.path.exists(os.path.join(os.getcwd(), f"{YT.channel_name}_data", f"{date.today()}")):
        data_categories_video = ["basic_user_activity_statistics", "basic_user_activity_US", "user_activity_by_location_over_time", "user_activity_in_US_over_time", "user_activity_by_location", "user_activity_in_US", "user_activity_by_location_over_subscribed_status", "user_activity_in_US_over_subscribed_status", "playback_details_by_location_over_liveOrOnDemand", "playback_details_by_location_over_time", "playback_details_by_country", "playback_details_by_country_averageViewPercentage", "playback_details_in_US", "playback_details_in_US_averageViewPercentage", "video_playback_by_location", "playback_location_details", "traffic_source", "traffic_source_details", "device_type", "operating_system", "operating_system_and_device_type", "viewer_demographics", "engagement_and_content_sharing", "audience_retention", "top_videos", "top_views_in_US", "top_videos_by_subscriber_type", "top_videos_by_yt_product", "top_videos_by_playback_details"]
        data_categories_playlist = ["basic_stats", "time_based", "playlist_activity_by_location", "playlist_activity_in_US", "playback_locations", "playback_locations_details", "traffic_sources", "traffic_sources_details", "device_type", "operating_system", "operating_system_and_device_type", "viewer_demographics", "top_playlists"]
        for category in data_categories_video:
            os.makedirs(os.path.join(os.getcwd(), f"{YT.channel_name}_data", f"{date.today()}", "video_reports", "csv", f"{category}"))

            os.makedirs(os.path.join(os.getcwd(), f"{YT.channel_name}_data", f"{date.today()}", "video_reports", "excel", f"{category}"))
        for category in data_categories_playlist:
            os.makedirs(os.path.join(os.getcwd(), f"{YT.channel_name}_data", f"{date.today()}", "playlist_reports", "csv", f"{category}"))

            os.makedirs(os.path.join(os.getcwd(), f"{YT.channel_name}_data", f"{date.today()}", "playlist_reports", "excel", f"{category}"))

    #YT.basic_user_activity_statistics()
    #YT.basic_user_activity_US()
    #YT.user_activity_by_location_over_time()
    #YT.user_activity_in_US_over_time()
    #YT.user_activity_by_location()
    #YT.user_activity_in_US()
    #YT.user_activity_by_location_over_subscribed_status()
    #YT.user_activity_in_US_over_subscribed_status()
    #YT.playback_details_by_location_over_liveOrOnDemand()
    #YT.playback_details_by_location_over_time()
    #YT.playback_details_by_country()
    #YT.playback_details_by_country_averageViewPercentage()
    #YT.playback_details_in_US()
    #YT.playback_details_in_US_averageViewPercentage()
    #YT.video_playback_by_location()
    #YT.playback_location_details()
    #YT.traffic_source()
    #YT.traffic_source_details()
    #YT.device_type()
    YT.operating_system()

if __name__ == "__main__":
    main()
    
  
