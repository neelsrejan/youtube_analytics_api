import os
import time
from datetime import date, timedelta, datetime
from YT_ANALYTICS import YT_ANALYTICS
from channel_reports_variables import continents_rev_dict, subcontinents_rev_dict

def main():
    
    # Intro to program
    print("Welcome to the Youtube Analytics API!")
    print("\nThis program is intended for you to be able to get as much raw data from the youtube analytics api, to do so for various kinds of reports I will be asking for responses in order to properly get the information you wish to get.")

    # Get all information to filter on 
    API_KEY = input("Please enter your API KEY: ").strip()
    channel_id = input("Please enter your channel id: ").strip()

    today = datetime.now()
    date_time = today.strftime("%Y-%m-%dT%H-%M-%S")

    YT = YT_ANALYTICS(API_KEY, channel_id, date_time)
    YT.get_filter_info()
    
    # In case you the program crashes and need to restart the program and save files into the same folder add the correct datetime as noted on the folder name
    #YT.date_time = "2021-11-23T09-44-55"
      
    # Create directories for saving data into
    if not os.path.exists(os.path.join(os.getcwd(), f"{YT.channel_name}_data", f"{YT.date_time}")):
        data_categories_video = ["basic_user_activity_statistics", "basic_user_activity_in_US", "user_activity_by_location_over_time", "user_activity_in_US_over_time", "user_activity_by_location", "user_activity_in_US", "user_activity_by_location_over_subscribed_status", "user_activity_in_US_over_subscribed_status", "playback_details_by_location_over_liveOrOnDemand", "playback_details_by_location_over_time", "playback_details_by_country", "playback_details_by_country_averageViewPercentage", "playback_details_in_US", "playback_details_in_US_averageViewPercentage", "video_playback_by_location", "playback_location_details", "traffic_source", "traffic_source_details", "device_type", "operating_system", "operating_system_and_device_type", "viewer_demographics", "engagement_and_content_sharing", "audience_retention", "top_videos_regional", "top_videos_in_US", "top_videos_by_subscriber_type", "top_videos_by_yt_product", "top_videos_by_playback_details"]
        data_categories_playlist = ["basic_stats_playlist", "time_based_playlist", "activity_by_location_playlist", "activity_in_US_playlist", "playback_locations_playlist", "playback_locations_details_playlist", "traffic_sources_playlist", "traffic_sources_details_playlist", "device_type_playlist", "operating_system_playlist", "operating_system_and_device_type_playlist", "viewer_demographics_playlist", "top_playlists"]
        for category in data_categories_video:
            os.makedirs(os.path.join(os.getcwd(), f"{YT.channel_name}_data", f"{YT.date_time}", "raw", "video_reports", "csv", f"{category}"))
            os.makedirs(os.path.join(os.getcwd(), f"{YT.channel_name}_data", f"{YT.date_time}", "raw", "video_reports", "excel", f"{category}"))
            os.makedirs(os.path.join(os.getcwd(), f"{YT.channel_name}_data", f"{YT.date_time}", "clean", "video_reports", "csv", f"{category}"))
            os.makedirs(os.path.join(os.getcwd(), f"{YT.channel_name}_data", f"{YT.date_time}", "clean", "video_reports", "excel", f"{category}"))
        for category in data_categories_playlist:
            os.makedirs(os.path.join(os.getcwd(), f"{YT.channel_name}_data", f"{YT.date_time}", "raw", "playlist_reports", "csv", f"{category}"))
            os.makedirs(os.path.join(os.getcwd(), f"{YT.channel_name}_data", f"{YT.date_time}", "raw", "playlist_reports", "excel", f"{category}"))
            os.makedirs(os.path.join(os.getcwd(), f"{YT.channel_name}_data", f"{YT.date_time}", "clean", "playlist_reports", "csv", f"{category}"))
            os.makedirs(os.path.join(os.getcwd(), f"{YT.channel_name}_data", f"{YT.date_time}", "clean", "playlist_reports", "excel", f"{category}"))    
    
    with open(os.path.join(os.getcwd(), f"{YT.channel_name}_data", f"{YT.date_time}", "parameters.txt"), "w") as f:
        f.write(f"The parameters for the request on {YT.date_time} are:\n\n")
        f.write(f"The start date is: {YT.start_date}\n")
        f.write(f"The end date is: {YT.end_date}\n")
        if YT.has_countries:
            f.write(f"The countries specified are: {YT.countries}\n")
        if YT.has_continents:
            to_write = [continents_rev_dict[continent] for continent in YT.continents]
            f.write(f"The continents specified are: {to_write}\n")
        if YT.has_subcontinents:
            to_write = [subcontinents_rev_dict[subcontinent] for subcontinent in YT.subcontinents]
            f.write(f"The subcontinents specified are: {to_write}\n")
        if YT.has_provinces:
            f.write(f"The provinces specified are: {YT.provinces}\n")
    
    #Get the data
    start_time = time.time()
    curr_time = time.time()
    
    YT.basic_user_activity_statistics()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get basic_user_activity_statistics")
    curr_time = end_time

    YT.basic_user_activity_in_US()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get basic_user_activity_in_US")
    curr_time = end_time
    
    YT.user_activity_by_location_over_time()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get user_activity_by_location_over_time")
    curr_time = end_time
    
    YT.user_activity_in_US_over_time()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get user_activity_in_US_over_time")
    curr_time = end_time

    YT.user_activity_by_location()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get user_activity_by_location")
    curr_time = end_time

    YT.user_activity_in_US()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get user_activity_in_US")
    curr_time = end_time

    YT.user_activity_by_location_over_subscribed_status()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get user_activity_by_location_over_subscribed_status")
    curr_time = end_time

    YT.user_activity_in_US_over_subscribed_status()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get user_activity_in_US_over_subscribed_status")
    curr_time = end_time

    YT.playback_details_by_location_over_liveOrOnDemand()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get playback_details_by_location_over_liveOrOnDemand")    
    curr_time = end_time
    
    YT.playback_details_by_location_over_time()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get playback_details_by_location_over_time")
    curr_time = end_time

    YT.playback_details_by_country()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get playback_details_by_country")
    curr_time = end_time

    YT.playback_details_by_country_averageViewPercentage()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get playback_details_by_country_averageViewPercentage")
    curr_time = end_time

    YT.playback_details_in_US()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get playback_details_in_US")
    curr_time = end_time

    YT.playback_details_in_US_averageViewPercentage()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get playback_details_in_US_averageViewPercentage")
    curr_time = end_time

    YT.video_playback_by_location()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get video_playback_by_location")
    curr_time = end_time

    YT.playback_location_details()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get playback_location_details")
    curr_time = end_time

    YT.traffic_source()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get traffic_source")
    curr_time = end_time
    
    YT.traffic_source_details()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get traffic_source_details")
    curr_time = end_time
    
    YT.device_type()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get device_type")
    curr_time = end_time
    
    YT.operating_system()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get operating_system")
    curr_time = end_time

    YT.operating_system_and_device_type()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get operating_system_and_device_type")
    curr_time = end_time

    YT.viewer_demographics()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get viewer_demographics")
    curr_time = end_time

    YT.engagement_and_content_sharing()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get engagement_and_content_sharing")
    curr_time = end_time
    
    YT.audience_retention()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get audience_retention")
    curr_time = end_time
    
    YT.top_videos_regional()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get top_videos_regional")
    curr_time = end_time

    YT.top_videos_in_US()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get top_videos_in_US")
    curr_time = end_time

    YT.top_videos_by_subscriber_type()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get top_videos_by_subscriber_type")
    curr_time = end_time

    YT.top_videos_by_yt_product()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get top_videos_by_yt_product")
    curr_time = end_time

    YT.top_videos_by_playback_details()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get top_videos_by_playback_details")
    curr_time = end_time
    

    YT.basic_stats_playlist()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get basic_stats_playlist")    
    curr_time = end_time
    
    YT.time_based_playlist()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get time_based_playlist")
    curr_time = end_time
    
    YT.activity_by_location_playlist()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get activity_by_location_playlist")
    curr_time = end_time

    YT.activity_in_US_playlist()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get activity_in_US_playlist")
    curr_time = end_time
    
    YT.playback_locations_playlist()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get playback_locations_playlist")
    curr_time = end_time
    
    YT.playback_locations_details_playlist()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get playback_locations_details_playlist")
    curr_time = end_time
    
    YT.traffic_sources_playlist()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get traffic_sources_playlist")
    curr_time = end_time
    
    YT.traffic_sources_details_playlist()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get traffic_sources_details_playlist")
    curr_time = end_time
     
    YT.device_type_playlist() 
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get device_type_playlist")
    curr_time = end_time
    
    YT.operating_system_playlist()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get operating_system_playlist")
    curr_time = end_time
    
    YT.operating_system_and_device_type_playlist()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get operating_system_and_device_type_playlist")
    curr_time = end_time

    YT.viewer_demographics_playlist()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get viewer_demographics_playlist")
    curr_time = end_time
     
    YT.top_playlists()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to get top_playlists")
    curr_time = end_time

    #Clean the data
    YT.clean_data()
    end_time = time.time()
    print(f"It took {timedelta(seconds=round(end_time - curr_time))} to clean data")
    finish_time = time.time()
    
    print(f"Program finished, your data has been cleaned! It has taken {timedelta(seconds=round(finish_time - start_time))}.")
    
if __name__ == "__main__":
    main()
    
  
