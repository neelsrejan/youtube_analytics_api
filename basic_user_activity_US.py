import os
import pandas as pd
from datetime import date
from auth import Auth

class Basic_User_Activity_US(Auth):

    def basic_user_activity_US(self):
        
        if not self.has_provinces:
            return

        metrics = ["views", "redViews", "estimatedMinutesWatched", "estimatedRedMinutesWatched", "averageViewDuration", "averageViewPercentage", "annotationClickThroughRate", "annotationCloseRate", "annotationImpressions", "annotationClickableImpressions", "annotationClosableImpressions", "annotationClicks", "annotationCloses", "cardClickRate", "cardTeaserClickRate", "cardImpressions", "cardTeaserImpressions", "cardClicks", "cardTeaserClicks"]

        filters_to_remove = []
        if not self.has_groups:
            filters_to_remove.append("group")

        filters_1 = ["province"]
        filters_2 = ["", "video", "group"]
        for fil_to_rem in filters_to_remove:
            filters_2.remove(fil_to_rem)

        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                if i == 0 and j != 0:
                    if filters_1[i] == "province" and filters_2[j] == "video":
                        data = []
                        col_names
                        for province in self.provinces:
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    dimensions=f"{filters_1[i] + ',' + filters_2[j]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters_1[i]}=={province};{filters_2[j]}=={','.join(self.vid_ids)}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            if len(response["rows"]) != 0:
                                for vid_row in response["rows"]:
                                    data.append(vid_row)
                        response_df = pd.DataFrame(data=data, columns=col_names)
                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "basic_user_activity_US", f"filters={filters_1[i] + ',' + filters_2[j]}.csv"), index=False)
                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "basic_user_activity_US", f"filters={filters_1[i] + ',' + filters_2[j]}.xlsx"), index=False)
                    elif filters_1[i] == "province" and filters_2[j] == "group":
                        for province in self.provinces:
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    dimensions=f"{filters_1[i] + ',' + filters_2[j]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters_1[i]}=={province};{filters_2[j]}=={','.join(self.groups)}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            if len(response["rows"]) != 0:
                                for vid_row in response["rows"]:
                                    data.append(vid_row)
                        response_df = pd.DataFrame(data=data, columns=col_names)
                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "basic_user_activity_US", f"filters={filters_1[i] + ',' + filters_2[j]}.csv"), index=False)
                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "basic_user_activity_US", f"filters={filters_1[i] + ',' + filters_2[j]}.xlsx"), index=False)
                elif i == 0 and j == 0:
                    if filters_1[i] == "province":
                        data = []
                        col_names = None
                        for province in self.provinces:
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    dimensions=f"{filters_1[i]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{filters_1[i]}=={province}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            if len(response["rows"]) != 0:
                                data.append(response["rows"][0])
                        response_df = pd.DataFrame(data=data, columns=col_names)
                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "basic_user_activity_US", f"filters={filters_1[i]}.csv"), index=False)
                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "basic_user_activity_US", f"filters={filters_1[i]}.xlsx"), index=False)
        return 
