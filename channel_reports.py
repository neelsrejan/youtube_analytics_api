import os
import pandas as pd
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
    
    def get_groups(self):
        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
        youtubeAnalytics = self.get_service()
        groups_response = self.execute_api_request(
            youtubeAnalytics.groups().list,
            mine=True
        )
        if len(groups_response["items"]) != 0:
            self.groups.append(groups_response["items"])
            self.has_groups = True
        return
    

    def basic_user_activity_statistics(self):
        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
        youtubeAnalytics = self.get_service()

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
                        for country in self.countries:
                            print(f"dimensions={filters_1[i] + ',' + filters_2[j]}")
                            print(f"endDate={self.end_date}")
                            print(f"filters={filters_1[i]}=={country};{filters_2[j]}=={','.join(self.vid_ids)}")
                            print("ids=channel==MINE")
                            print(f"metrics={','.join(metrics)}")
                            print(f"startDate={self.start_date}")
                            response = self.execute_api_request(
                                youtubeAnalytics.reports().query,
                                    dimensions=f"{filters_1[i] + ',' + filters_2[j]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters_1[i]}=={country};{filters_2[j]}=={','.join(self.vid_ids)}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            ) 
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            data = response["rows"]
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            print(response_df)
                            print("\n")
                    elif filters_1[i] == "country" and filters_2[j] == "group":
                        for country in self.countries:
                            print(f"dimensions={filters_1[i] + ',' + filters_2[j]}")
                            print(f"endDate={self.end_date}")
                            print(f"filters={filters_1[i]}=={country};{filters_2[j]}=={','.join(self.groups)}")
                            print("ids=channel==MINE")
                            print(f"metrics={','.join(metrics)}")
                            print(f"startDate={self.start_date}")
                            response = self.execute_api_request(
                                youtubeAnalytics.reports().query,
                                    dimensions=f"{filters_1[i] + ',' + filters_2[j]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters_1[i]}=={country};{filters_2[j]}=={','.join(self.groups)}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            data = response["rows"]
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            print(response_df)
                            print("\n")
                    elif filters_1[i] == "continent" and filters_2[j] == "video":
                        for continent in self.continents:
                            print(f"dimensions={filters_1[i] + ',' + filters_2[j]}")
                            print(f"endDate={self.end_date}")
                            print(f"filters={filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.vid_ids)}")
                            print("ids=channel==MINE")
                            print(f"metrics={','.join(metrics)}")
                            print(f"startDate={self.start_date}")
                            response = self.execute_api_request(
                                youtubeAnalytics.reports().query,
                                    dimensions=f"{filters_2[j]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.vid_ids)}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            data = response["rows"]
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            print(response_df)
                            print("\n")
                    elif filters_1[i] == "continent" and filters_2[j] == "group":
                        for continent in self.continents:
                            print(f"dimensions={filters_1[i] + ',' + filters_2[j]}")
                            print(f"endDate={self.end_date}")
                            print(f"filters={filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.groups)}")
                            print("ids=channel==MINE")
                            print(f"metrics={','.join(metrics)}")
                            print(f"startDate={self.start_date}")
                            response = self.execute_api_request(
                                youtubeAnalytics.reports().query,
                                    dimensions=f"{filters_2[j]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.groups)}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            data = response["rows"]
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            print(response_df)
                            print("\n")
                    elif filters_1[i] == "subContinent" and filters_2[j] == "video":
                        for subcontinent in self.subcontinents:
                            print(f"dimensions={filters_1[i] + ',' + filters_2[j]}")
                            print(f"endDate={self.end_date}")
                            print(f"filters={filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.vid_ids)}")
                            print("ids=channel==MINE")
                            print(f"metrics={','.join(metrics)}")
                            print(f"startDate={self.start_date}")
                            response = self.execute_api_request(
                                youtubeAnalytics.reports().query,
                                    dimensions=f"{filters_2[j]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.vid_ids)}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            data = response["rows"]
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            print(response_df)
                            print("\n")
                    elif filters_1[i] == "subContinent" and filters_2[j] == "group":
                        for subcontinent in self.subcontinents:
                            print(f"dimensions={filters_1[i] + ',' + filters_2[j]}")
                            print(f"endDate={self.end_date}")
                            print(f"filters={filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.groups)}")
                            print("ids=channel==MINE")
                            print(f"metrics={','.join(metrics)}")
                            print(f"startDate={self.start_date}")
                            response = self.execute_api_request(
                                youtubeAnalytics.reports().query,
                                    dimensions=f"{filters_2[j]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.groups)}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            data = response["rows"]
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            print(response_df)
                            print("\n")
                elif i == 0 and j != 0:
                    if filters_2[j] == "video":
                        print(f"dimensions={filters_2[j]}")
                        print(f"endDate={self.end_date}")
                        print(f"filters={filters_2[j]}=={','.join(self.vid_ids)}")
                        print("ids=channel==MINE")
                        print(f"metrics={','.join(metrics)}")
                        print(f"startDate={self.start_date}")
                        response = self.execute_api_request(
                            youtubeAnalytics.reports().query,
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
                        print(response_df)
                        print("\n")
                    elif filters_2[j] == "group":
                        print(f"dimensions={filters_2[j]}")
                        print(f"endDate={self.end_date}")
                        print(f"filters={filters_2[j]}=={','.join(self.groups)}")
                        print("ids=channel==MINE")
                        print(f"metrics={','.join(metrics)}")
                        print(f"startDate={self.start_date}")
                        response = self.execute_api_request(
                             youtubeAnalytics.reports().query,
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
                        print(response_df)
                        print("\n")
                elif i != 0 and j == 0:
                    #filters.append(filters_1[i])
                    if filters_1[i] == "country":
                        for country in self.countries:
                            print(f"dimensions={filters_1[i]}")
                            print(f"endDate={self.end_date}")
                            print(f"filters={filters_1[i]}=={country}")
                            print("ids=channel==MINE")
                            print(f"metrics={','.join(metrics)}")
                            print(f"startDate={self.start_date}")
                            response = self.execute_api_request(
                                youtubeAnalytics.reports().query,
                                    dimensions=f"{filters_1[i]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters_1[i]}=={country}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            data = response["rows"]
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            print(response_df)
                            print("\n")
                    elif filters_1[i] == "continent":
                        for continent in self.continents:
                            print(f"dimensions={filters_1[i]}")
                            print(f"endDate={self.end_date}")
                            print(f"filters={filters_1[i]}=={continent}")
                            print("ids=channel==MINE")
                            print(f"metrics={','.join(metrics)}")
                            print(f"startDate={self.start_date}")
                            response = self.execute_api_request(
                                youtubeAnalytics.reports().query,
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters_1[i]}=={continent}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            data = response["rows"]
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            print(response_df)
                            print("\n")
                    elif filters_1[i] == "subContinent":
                        for subcontinent in self.subcontinents:
                            print(f"dimensions={filters_1[i]}")
                            print(f"endDate={self.end_date}")
                            print(f"filters={filters_1[i]}=={continent}")
                            print("ids=channel==MINE")
                            print(f"metrics={','.join(metrics)}")
                            print(f"startDate={self.start_date}")
                            response = self.execute_api_request(
                                youtubeAnalytics.reports().query,
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters_1[i]}=={subcontinent}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            data = response["rows"]
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            print(response_df)
                            print("\n")
        return

    # filters, metrics, countries, continents = basic_user_activity_statistics(False)
