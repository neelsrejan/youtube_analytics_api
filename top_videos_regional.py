import os
import pandas as pd
from auth import Auth

class Top_Videos_Regional(Auth):

    def top_videos_regional(self):
        metrics = ["views", "redViews", "comments", "likes", "dislikes", "videosAddedToPlaylists", "videosRemovedFromPlaylists", "shares", "estimatedMinutesWatched", "estimatedRedMinutesWatched", "averageViewDuration", "averageViewPercentage", "annotationClickThroughRate", "annotationCloseRate", "annotationImpressions", "annotationClickableImpressions", "annotationClosableImpressions", "annotationClicks", "annotationCloses", "cardClickRate", "cardTeaserClickRate", "cardImpressions", "cardTeaserImpressions", "cardClicks", "cardTeaserClicks", "subscribersGained", "subscribersLost"]
        sorting_options = ["-views"]
        required_dimension = "video"

        filters_to_remove = []
        if not self.has_countries:
            filters_to_remove.append("country")
        if not self.has_continents:
            filters_to_remove.append("continent")
        if not self.has_subcontinents:
            filters_to_remove.append("subContinent")

        filters = ["", "country", "continent", "subContinent"]

        for fil_to_rem in filters_to_remove:
            filters.remove(fil_to_rem)

        for sort_by in sorting_options:
            for i in range(len(filters)):
                if filters[i] == "country" or filters[i] == "continent" or filters[i] == "subContinent":
                    if filters[i] == "country":
                        data = []
                        col_names = None
                        for country in self.countries:
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    dimensions=f"{required_dimension},{filters[i]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters[i]}=={country}",
                                    ids="channel==MINE",
                                    maxResults=200,
                                    metrics=f"{','.join(metrics)}",
                                    sort=f"{sort_by}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            if len(response["rows"]) != 0:
                                for row in response["rows"]:
                                    data.append(row)
                        response_df = pd.DataFrame(data=data, columns=col_names)
                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_regional", f"{required_dimension},{filters[i]},{sort_by}.csv"), index=False)
                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_regional", f"{required_dimension},{filters[i]},{sort_by}.xlsx"), index=False)
                    elif filters[i] == "continent":
                        data = []
                        col_names = None
                        for continent in self.continents:
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    dimensions=f"{required_dimension}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters[i]}=={continent}",
                                    ids="channel==MINE",
                                    maxResults=200,
                                    metrics=f"{','.join(metrics)}",
                                    sort=f"{sort_by}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            col_names.insert(1, f"{filters[i]}")
                            if len(response["rows"]) != 0:
                                for row in response["rows"]:
                                    row.insert(1, f"{continent}")
                                    data.append(row)
                        response_df = pd.DataFrame(data=data, columns=col_names)
                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_regional", f"{required_dimension},{filters[i]},{sort_by}.csv"), index=False)
                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_regional", f"{required_dimension},{filters[i]},{sort_by}.xlsx"), index=False)
                    elif filters[i] == "subContinent":
                        data = []
                        col_names = None
                        for subcontinent in self.subcontinents:
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    dimensions=f"{required_dimension}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters[i]}=={subcontinent}",
                                    ids="channel==MINE",
                                    maxResults=200,
                                    metrics=f"{','.join(metrics)}",
                                    sort=f"{sort_by}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            col_names.insert(1, f"{filters[i]}")
                            if len(response["rows"]) != 0:
                                for row in response["rows"]:
                                    row.insert(1, f"{subcontinent}")
                                    data.append(row)
                        response_df = pd.DataFrame(data=data, columns=col_names)
                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_regional", f"{required_dimension},{filters[i]},{sort_by}.csv"), index=False)
                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_regional", f"{required_dimension},{filters[i]},{sort_by}.xlsx"), index=False)
                else:
                    data = []
                    col_names = None
                    response = self.execute_api_request(
                        self.youtubeAnalytics.reports().query,
                            dimensions=f"{required_dimension}",
                            endDate=f"{self.end_date}",
                            ids="channel==MINE",
                            maxResults=200,
                            metrics=f"{','.join(metrics)}",
                            sort=f"{sort_by}",
                            startDate=f"{self.start_date}"
                    )
                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                    if len(response["rows"]) != 0:
                        for row in response["rows"]:
                            data.append(row)
                    response_df = pd.DataFrame(data=data, columns=col_names)
                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_regional", f"{required_dimension},{sort_by}.csv"), index=False)
                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_regional", f"{required_dimension},{sort_by}.xlsx"), index=False)
        return
