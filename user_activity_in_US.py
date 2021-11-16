import os 
import pandas as pd
from auth import Auth

class User_Activity_In_US(Auth):

    def user_activity_in_US(self):
        
        if not self.has_provinces:
            return

        metrics = ["views", "redViews", "estimatedMinutesWatched", "estimatedRedMinutesWatched", "averageViewDuration", "averageViewPercentage", "annotationClickThroughRate", "annotationCloseRate", "annotationImpressions", "annotationClickableImpressions", "annotationClosableImpressions", "annotationClicks", "annotationCloses", "cardClickRate", "cardTeaserClickRate", "cardImpressions", "cardTeaserImpressions", "cardClicks", "cardTeaserClicks"]
        dimensions = ["province"]

        filters_to_remove = []
        if not self.has_groups:
            filters_to_remove.append("group")

        filters_1 = ["country"]
        filters_2 = ["", "video", "group"]
        for fil_to_rem in filters_to_remove:
            filters_2.remove(fil_to_rem)

        for dimension in dimensions:
            for i in range(len(filters_1)):
                for j in range(len(filters_2)):
                    if i == 0 and j != 0:
                        if filters_2[j] == "video":
                            data = []
                            col_names = None
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    dimensions=f"{dimension},{filters_1[i]},{filters_2[j]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters_1[i]}==US;{filters_2[j]}=={','.join(self.vid_ids)}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            if len(response["rows"]) != 0:
                                for row in response["rows"]:
                                    data.append(row)
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "user_activity_in_US", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "user_activity_in_US", f"{dimension},{filters_1[i]},{filters_2[j]}.xlsx"), index=False)
                        elif filters_2[j] == "group":
                            data = []
                            col_names = None
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    dimensions=f"{dimension},{filters_1[i]},{filters_2[j]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters_1[i]}==US;{filters_2[j]}=={','.join(self.groups)}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            if len(response["rows"]) != 0:
                                for row in response["rows"]:
                                    data.append(row)
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "user_activity_in_US", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "user_activity_in_US", f"{dimension},{filters_1[i]},{filters_2[j]}.xlsx"), index=False)
                    elif i == 0 and j == 0:
                        data = []
                        col_names = None
                        response = self.execute_api_request(
                            self.youtubeAnalytics.reports().query,
                                dimensions=f"{dimension},{filters_1[i]}",
                                endDate=f"{self.end_date}",
                                filters=f"{filters_1[i]}==US",
                                ids="channel==MINE",
                                metrics=f"{','.join(metrics)}",
                                startDate=f"{self.start_date}"
                        )
                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                        if len(response["rows"]) != 0:
                            for row in response["rows"]:
                                data.append(row)
                        response_df = pd.DataFrame(data=data, columns=col_names)
                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "user_activity_in_US", f"{dimension},{filters_1[i]}.csv"), index=False)
                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "user_activity_in_US", f"{dimension},{filters_1[i]}.xlsx"), index=False)
        return
