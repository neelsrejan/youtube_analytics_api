import os
from channel_reports import Channel_Reports
from channel_reports_filters import Filters

class YT_ANALYTICS(Channel_Reports, Filters):

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
        self.num_vids = None
        self.vid_ids = []
        self.playlist_ids = []
        self.start_date = None
        self.end_date = None
        self.has_groups = False
        self.groups = []
        self.has_countries = False
        self.countries = []
        self.has_continents = False
        self.continents = []
        self.has_subcontinents = False
        self.subcontinents = []
        self.has_provinces = False
        self.provinces = []
        
    def get_filter_info(self):
        self.get_num_vids()
        self.get_vid_ids()
        self.get_playlist_ids()
        self.ask_dates()
        self.get_groups()
        self.ask_countries()
        self.ask_continents()
        self.ask_subcontinents()
        self.ask_provinces()

