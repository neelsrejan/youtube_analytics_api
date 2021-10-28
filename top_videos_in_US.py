import os
import pandas as pd
from datetime import date
from auth import Auth

class Top_Videos_In_US(Auth):

    def top_videos_in_US(self):

        if not self.has_provinces:
            return

        metrics = ["views", "redViews", "estimatedMinutesWatched", "estimatedRedMinutesWatched", "averageViewDuration", "averageViewPercentage", "annotationClickThroughRate", "annotationCloseRate", "annotationImpressions", "annotationClickableImpressions", "annotationClosableImpressions", "annotationClicks", "annotationCloses", "cardClickRate", "cardTeaserClickRate", "cardImpressions", "cardTeaserImpressions", "cardClicks", "cardTeaserClicks"]
        sorting_options = ["-views", "-redViews", "-estimatedMinutesWatched", "-estimatedRedMinutesWatched"]
        required_dimension = "video"
        required_filter = "province"
        sub_status = ["SUBSCRIBED", "UNSUBSCRIBED"]

        filters = ["", "subscribedStatus"]

        for sort_by in sorting_options:
            for i in range(len(filters)):
                if filters[i] == "subscribedStatus":
                    data = []
                    col_names = None
                    for province in self.provinces:
                        for sub_type in sub_status:
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    dimensions=f"{required_dimension},{required_filter},{filters[i]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{required_filter}=={province};{filters[i]}=={sub_type}",
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
                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "top_videos_in_US", f"{required_dimension},{required_filter},{filters[i]},{sort_by}.csv"), index=False)
                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "top_videos_in_US", f"{required_dimension},{required_filter},{filters[i]},{sort_by}.xlsx"), index=False)
                else:
                    data = []
                    col_names = None
                    for province in self.provinces:
                        response = self.execute_api_request(
                            self.youtubeAnalytics.reports().query,
                                dimensions=f"{required_dimension},{required_filter}",
                                endDate=f"{self.end_date}",
                                filters=f"{required_filter}=={province}",
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
                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "top_videos_in_US", f"{required_dimension},{required_filter},{sort_by}.csv"), index=False)
                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "top_videos_in_US", f"{required_dimension},{required_filter},{sort_by}.xlsx"), index=False)
        return

        
