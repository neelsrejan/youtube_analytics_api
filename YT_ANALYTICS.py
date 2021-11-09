import os
from channel_reports_filters import Filters
from datetime import date

from basic_user_activity_statistics import Basic_User_Activity_Statistics
from basic_user_activity_in_US import Basic_User_Activity_In_US
from user_activity_by_location_over_time import User_Activity_By_Location_Over_Time
from user_activity_in_US_over_time import User_Activity_In_US_Over_Time
from user_activity_by_location import User_Activity_By_Location
from user_activity_in_US import User_Activity_In_US
from user_activity_by_location_over_subscribed_status import User_Activity_By_Location_Over_Subscribed_Status
from user_activity_in_US_over_subscribed_status import User_Activity_In_US_Over_Subscribed_Status
from playback_details_by_location_over_liveOrOnDemand import Playback_Details_By_Location_Over_LiveOrOnDemand
from playback_details_by_location_over_time import Playback_Details_By_Location_Over_Time
from playback_details_by_country import Playback_Details_By_Country
from playback_details_by_country_averageViewPercentage import Playback_Details_By_Country_AverageViewPercentage
from playback_details_in_US import Playback_Details_In_US
from playback_details_in_US_averageViewPercentage import Playback_Details_In_US_AverageViewPercentage
from video_playback_by_location import Video_Playback_By_Location
from playback_location_details import Playback_Location_Details
from traffic_source import Traffic_Source
from traffic_source_details import Traffic_Source_Details
from device_type import Device_Type
from operating_system import Operating_System
from operating_system_and_device_type import Operating_System_And_Device_Type
from viewer_demographics import Viewer_Demographics
from engagement_and_content_sharing import Engagement_And_Content_Sharing
from audience_retention import Audience_Retention
from top_videos_regional import Top_Videos_Regional
from top_videos_in_US import Top_Videos_In_US
from top_videos_by_subscriber_type import Top_Videos_By_Subscriber_Type
from top_videos_by_yt_product import Top_Videos_By_YT_Product
from top_videos_by_playback_details import Top_Videos_By_Playback_Details

from basic_stats_playlist import Basic_Stats_Playlist
from time_based_playlist import Time_Based_Playlist
from activity_by_location_playlist import Activity_By_Location_Playlist
from activity_in_US_playlist import Activity_In_US_Playlist
from playback_locations_playlist import Playback_Locations_Playlist
from playback_locations_details_playlist import Playback_Locations_Details_Playlist
from traffic_sources_playlist import Traffic_Sources_Playlist
from traffic_sources_details_playlist import Traffic_Sources_Details_Playlist
from device_type_playlist import Device_Type_Playlist
from operating_system_playlist import Operating_System_Playlist
from operating_system_and_device_type_playlist import Operating_System_And_Device_Type_Playlist
from viewer_demographics_playlist import Viewer_Demographics_Playlist
from top_playlists import Top_Playlists

from clean_data import Clean_Data

class YT_ANALYTICS(Filters, Basic_User_Activity_Statistics, Basic_User_Activity_In_US, User_Activity_By_Location_Over_Time, User_Activity_In_US_Over_Time, User_Activity_By_Location, User_Activity_In_US, User_Activity_By_Location_Over_Subscribed_Status, User_Activity_In_US_Over_Subscribed_Status, Playback_Details_By_Location_Over_LiveOrOnDemand, Playback_Details_By_Location_Over_Time, Playback_Details_By_Country, Playback_Details_By_Country_AverageViewPercentage, Playback_Details_In_US, Playback_Details_In_US_AverageViewPercentage, Video_Playback_By_Location, Playback_Location_Details, Traffic_Source, Traffic_Source_Details, Device_Type, Operating_System, Operating_System_And_Device_Type, Viewer_Demographics, Engagement_And_Content_Sharing, Audience_Retention, Top_Videos_Regional, Top_Videos_In_US, Top_Videos_By_Subscriber_Type, Top_Videos_By_YT_Product, Top_Videos_By_Playback_Details, Basic_Stats_Playlist, Time_Based_Playlist, Activity_By_Location_Playlist, Activity_In_US_Playlist, Playback_Locations_Playlist, Playback_Locations_Details_Playlist, Traffic_Sources_Playlist, Traffic_Sources_Details_Playlist, Device_Type_Playlist, Operating_System_Playlist, Operating_System_And_Device_Type_Playlist, Viewer_Demographics_Playlist, Top_Playlists, Clean_Data):

    def __init__(self, API_KEY, channel_id):
        self.API_KEY = API_KEY
        self.channel_id = channel_id
        self.SCOPES = ["https://www.googleapis.com/auth/youtube",
            "https://www.googleapis.com/auth/youtube.readonly",
            "https://www.googleapis.com/auth/yt-analytics.readonly",
            "https://www.googleapis.com/auth/youtubepartner",
            "https://www.googleapis.com/auth/yt-analytics-monetary.readonly"
            ]
        self.API_SERVICE_NAME = "youtubeAnalytics"
        self.API_VERSION = "v2"
        self.CLIENT_SECRETS_FILE = os.path.join(os.getcwd(), "secrets.json")
        super()
        self.date = str(date.today())
        self.channel_name = None
        self.num_vids = None
        self.vid_ids = []
        self.playlist_ids = []
        self.start_date = None
        self.end_date = None
        self.has_countries = False
        self.countries = []
        self.has_continents = False
        self.continents = []
        self.has_subcontinents = False
        self.subcontinents = []
        self.has_provinces = False
        self.provinces = []
        self.youtubeAnalytics = None
        self.has_groups = False
        self.groups = []

        
    def get_filter_info(self):
        self.get_channel_name()
        self.get_num_vids()
        self.get_vid_ids()
        self.get_playlist_ids()
        self.ask_dates()
        self.ask_countries()
        self.ask_continents()
        self.ask_subcontinents()
        self.ask_provinces()
        self.auth()
        self.get_groups()
