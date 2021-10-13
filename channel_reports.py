import os
import pandas as pd
from datetime import date
import google.oauth2.credentials
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow


class Channel_Reports():

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
    

    def basic_user_activity_statistics(self):
        metrics = ["views", "redViews", "comments", "likes", "dislikes", "videosAddedToPlaylists", "videosRemovedFromPlaylists", "shares", "estimatedMinutesWatched", "estimatedRedMinutesWatched", "averageViewDuration", "averageViewPercentage", "annotationClickThroughRate", "annotationCloseRate", "annotationImpressions", "annotationClickableImpressions", "annotationClosableImpressions", "annotationClicks", "annotationCloses", "cardClickRate", "cardTeaserClickRate", "cardImpressions", "cardTeaserImpressions", "cardClicks", "cardTeaserClicks", "subscribersGained", "subscribersLost"]

        filters = []
        filters_to_remove_1 = []
        filters_to_remove_2 = []
        if not self.has_countries:
            filters_to_remove_1.append("country")
        if not self.has_continents:
            filters_to_remove_1.append("continent")
        if not self.has_subcontinents:
            filters_to_remove_1.append("subContinent")
        if not self.has_groups:
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
                    if filters_1[i] == "country" and filters_2[j] == "video":
                        data = []
                        col_names = None
                        for country in self.countries:
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    dimensions=f"{filters_1[i] + ',' + filters_2[j]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters_1[i]}=={country};{filters_2[j]}=={','.join(self.vid_ids)}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            ) 
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            if len(response["rows"]) != 0:
                                for vid_row in response["rows"]:
                                    data.append(vid_row)
                        response_df = pd.DataFrame(data=data, columns=col_names)
                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "basic_user_activity_statistics", f"filters={filters_1[i] + ',' + filters_2[j]}.csv"), index=False)
                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "basic_user_activity_statistics", f"filters={filters_1[i] + ',' + filters_2[j]}.xlsx"), index=False)
                    elif filters_1[i] == "country" and filters_2[j] == "group":
                        data = []
                        col_names = None
                        for country in self.countries:
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    dimensions=f"{filters_1[i] + ',' + filters_2[j]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters_1[i]}=={country};{filters_2[j]}=={','.join(self.groups)}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            if len(response["rows"]) != 0:
                                for vid_row in response["rows"]:
                                    data.append(vid_row)
                        response_df = pd.DataFrame(data=data, columns=col_names)
                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "basic_user_activity_statistics", f"filters={filters_1[i] + ',' + filters_2[j]}.csv"), index=False)
                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "basic_user_activity_statistics", f"filters={filters_1[i] + ',' + filters_2[j]}.xlsx"), index=False)
                    elif filters_1[i] == "continent" and filters_2[j] == "video":
                        data = []
                        col_names = None
                        for continent in self.continents:
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    dimensions=f"{filters_2[j]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.vid_ids)}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            col_names.insert(1, filters_1[i])
                            if len(response["rows"]) != 0:
                                for vid_row in response["rows"]:
                                    vid_row.insert(1, continent)
                                    data.append(vid_row)
                        response_df = pd.DataFrame(data=data, columns=col_names)
                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "basic_user_activity_statistics", f"filters={filters_1[i] + ',' + filters_2[j]}.csv"), index=False)
                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "basic_user_activity_statistics", f"filters={filters_1[i] + ',' + filters_2[j]}.xlsx"), index=False)
                    elif filters_1[i] == "continent" and filters_2[j] == "group":
                        data = []
                        col_names = None
                        for continent in self.continents:
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    dimensions=f"{filters_2[j]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.groups)}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            col_names.insert(1, filters_1[i])
                            if len(response["rows"]) != 0:
                                for vid_row in response["rows"]:
                                    vid_row.insert(1, continent)
                                    data.append(vid_row)
                        response_df = pd.DataFrame(data=data, columns=col_names)
                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "basic_user_activity_statistics", f"filters={filters_1[i] + ',' + filters_2[j]}.csv"), index=False)
                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "basic_user_activity_statistics", f"filters={filters_1[i] + ',' + filters_2[j]}.xlsx"), index=False)
                    elif filters_1[i] == "subContinent" and filters_2[j] == "video":
                        data = []
                        col_names = None
                        for subcontinent in self.subcontinents:
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    dimensions=f"{filters_2[j]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.vid_ids)}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            col_names.insert(1, filters_1[i])
                            if len(response["rows"]) != 0:
                                for vid_row in response["rows"]:
                                    vid_row.insert(1, subcontinent)
                                    data.append(vid_row)
                        response_df = pd.DataFrame(data=data, columns=col_names)
                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "basic_user_activity_statistics", f"filters={filters_1[i] + ',' + filters_2[j]}.csv"), index=False)
                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "basic_user_activity_statistics", f"filters={filters_1[i] + ',' + filters_2[j]}.xlsx"), index=False)
                    elif filters_1[i] == "subContinent" and filters_2[j] == "group":
                        data = []
                        col_names = None
                        for subcontinent in self.subcontinents:
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    dimensions=f"{filters_2[j]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.groups)}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            col_names.insert(1, filters_1[i])
                            if len(response["rows"]) != 0:
                                for vid_row in response["rows"]:
                                    vid_row.insert(1, subcontinent)
                                    data.append(vid_row)
                        response_df = pd.DataFrame(data=data, columns=col_names)
                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "basic_user_activity_statistics", f"filters={filters_1[i] + ',' + filters_2[j]}.csv"), index=False)
                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "basic_user_activity_statistics", f"filters={filters_1[i] + ',' + filters_2[j]}.xlsx"), index=False)
                elif i == 0 and j != 0:
                    if filters_2[j] == "video":
                        response = self.execute_api_request(
                            self.youtubeAnalytics.reports().query,
                                dimensions=f"{filters_2[j]}",
                                endDate=f"{self.end_date}",
                                filters=f"{filters_2[j]}=={','.join(self.vid_ids)}",
                                ids="channel==MINE",
                                metrics=f"{','.join(metrics)}",
                                startDate=f"{self.start_date}"
                        )
                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                        data = response["rows"]
                        response_df = pd.DataFrame(data=data, columns=col_names)
                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "basic_user_activity_statistics", f"filters={filters_2[j]}.csv"), index=False)
                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "basic_user_activity_statistics", f"filters={filters_2[j]}.xlsx"), index=False)
                    elif filters_2[j] == "group":
                        response = self.execute_api_request(
                             self.youtubeAnalytics.reports().query,
                                dimensions=f"{filters_2[j]}",
                                endDate=f"{self.end_date}",
                                filters=f"{filters_2[j]}=={','.join(self.groups)}",
                                ids="channel==MINE",
                                 metrics=f"{','.join(metrics)}",
                                startDate=f"{start_date}"
                        )
                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                        data = response["rows"]
                        response_df = pd.DataFrame(data=data, columns=col_names)
                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "basic_user_activity_statistics", f"filters={filters_2[j]}.csv"), index=False)
                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "basic_user_activity_statistics", f"filters={filters_2[j]}.xlsx"), index=False)
                elif i != 0 and j == 0:
                    if filters_1[i] == "country":
                        data = []
                        col_names = None
                        for country in self.countries:
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    dimensions=f"{filters_1[i]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters_1[i]}=={country}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            if len(response["rows"]) != 0:
                                data.append(response["rows"][0])
                        response_df = pd.DataFrame(data=data, columns=col_names)
                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "basic_user_activity_statistics", f"filters={filters_1[i]}.csv"), index=False)
                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "basic_user_activity_statistics", f"filters={filters_1[i]}.xlsx"), index=False)
                    elif filters_1[i] == "continent":
                        data = []
                        col_names = None
                        for continent in self.continents:
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters_1[i]}=={continent}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            col_names.insert(1, filters_1[i])
                            if len(response["rows"]) != 0:
                                curr_row = response["rows"][0]
                                curr_row.insert(1, continent)
                                data.append(curr_row)
                        response_df = pd.DataFrame(data=data, columns=col_names)
                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "basic_user_activity_statistics", f"filters={filters_1[i]}.csv"), index=False)
                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "basic_user_activity_statistics", f"filters={filters_1[i]}.xlsx"), index=False)
                    elif filters_1[i] == "subContinent":
                        data = []
                        col_names = None
                        for subcontinent in self.subcontinents:
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters_1[i]}=={subcontinent}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            col_names.insert(1, filters_1[i])
                            if len(response["rows"]) != 0:
                                curr_row = response["rows"][0]
                                curr_row.insert(1, subcontinent)
                                data.append(curr_row)
                        response_df = pd.DataFrame(data=data, columns=col_names)
                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "basic_user_activity_statistics", f"filters={filters_1[i]}.csv"), index=False)
                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "basic_user_activity_statistics", f"filters={filters_1[i]}.xlsx"), index=False)
        return
