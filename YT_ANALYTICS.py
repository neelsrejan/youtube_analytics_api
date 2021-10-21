import os
from channel_reports_filters import Filters
from basic_user_activity_statistics import Basic_User_Activity_Statistics
from basic_user_activity_US import Basic_User_Activity_US
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

class YT_ANALYTICS(Filters, Basic_User_Activity_Statistics, Basic_User_Activity_US, User_Activity_By_Location_Over_Time, User_Activity_In_US_Over_Time, User_Activity_By_Location, User_Activity_In_US, User_Activity_By_Location_Over_Subscribed_Status, User_Activity_In_US_Over_Subscribed_Status, Playback_Details_By_Location_Over_LiveOrOnDemand, Playback_Details_By_Location_Over_Time, Playback_Details_By_Country, Playback_Details_By_Country_AverageViewPercentage, Playback_Details_In_US, Playback_Details_In_US_AverageViewPercentage, Video_Playback_By_Location, Playback_Location_Details, Traffic_Source, Traffic_Source_Details, Device_Type, Operating_System):

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
        #self.ask_countries()
        #self.ask_continents()
        #self.ask_subcontinents()
        #self.ask_provinces()
        self.has_countries = True
        self.countries = ["US", "GB", "IN"]
        self.has_continents = True
        self.continents = ["019", "142", "150"]
        self.has_subcontinents = True
        self.subcontinents = ["021", "005", "151", "035"]
        self.has_provinces = True
        self.provinces = ["US-CA", "US-TX", "US-NY"]
        self.auth()
        self.get_groups()
