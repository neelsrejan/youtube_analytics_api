import os
import pandas as pd
from datetime import date
from auth import Auth

class User_Activity_In_US_Over_Subscribed_Status(Auth):

    def user_activity_in_US_over_subscribed_status(self):
        metrics = ["views", "redViews", "estimatedMinutesWatched", "estimatedRedMinutesWatched", "averageViewDuration", "averageViewPercentage", "annotationClickThroughRate", "annotationCloseRate", "annotationImpressions", "annotationClickableImpressions", "annotationClosableImpressions", "annotationClicks", "annotationCloses", "cardClickRate", "cardTeaserClickRate", "cardImpressions", "cardTeaserImpressions", "cardClicks", "cardTeaserClicks"]
        dimensions = ["", "day", "month"]
        sub_status = ["SUBSCRIBED", "UNSUBSCRIBED"]

        filters_to_remove_1 = []
        filters_to_remove_2 = []
        if not self.has_groups:
            filters_to_remove_1.append("group")
        if not self.has_provinces:
            filters_to_remove_2.append("province")

        filters_1 = ["", "video", "group"]
        filters_2 = ["", "province"]
        filters_3 = ["", "subscribedStatus"]

        for fil_to_rem in filters_to_remove_1:
            filters_1.remove(fil_to_rem)
        for fil_to_rem in filters_to_remove_2:
            filters_2.remove(fil_to_rem)

        for dimension in dimensions:
            if dimension == "month":
                start_date = self.start_date[:7] + "-01"
                end_date = self.start_date[:7] + "-01"
            else:
                start_date = self.start_date
                end_date = self.end_date
            for i in range(len(filters_1)):
                for j in range(len(filters_2)):
                    for k in range(len(filters_3)):
                        if dimension == "day" or dimension == "month":
                            if i != 0 and j != 0 and k != 0:
                                if filters_1[i] == "video":
                                    data = []
                                    col_names = None
                                    for province in self.provinces:
                                        for sub_type in sub_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}",
                                                    endDate=f"{end_date}",
                                                    filters=f"{filters_1[i]}=={','.join(self.vid_ids)};{filters_2[j]}=={province};{filters_3[k]}=={sub_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{','.join(metrics)}",
                                                    startDate=f"{start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", "{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                                elif filters_1[i] == "group":
                                    data = []
                                    col_names = None
                                    for province in self.provinces:
                                        for sub_type in sub_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}",
                                                    endDate=f"{end_date}",
                                                    filters=f"{filters_1[i]}=={','.join(self.groups)};{filters_2[j]}=={province};{filters_3[k]}=={sub_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{','.join(metrics)}",
                                                    startDate=f"{start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", "{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                            elif i != 0 and j != 0 and k == 0:
                                if filters_1[i] == "video":
                                    data = []
                                    col_names = None
                                    for province in self.provinces:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_1[i]},{filters_2[j]}",
                                                endDate=f"{end_date}",
                                                filters=f"{filters_1[i]}=={','.join(self.vid_ids)};{filters_2[j]}=={province}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "{dimension},{filters_1[i]},{filters_2[j]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", "{dimension},{filters_1[i]},{filters_2[j]}.xlsx"), index=False)
                                elif filters_1[i] == "group":
                                    data = []
                                    col_names = None
                                    for province in self.provinces:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_1[i]},{filters_2[j]}",
                                                endDate=f"{end_date}",
                                                filters=f"{filters_1[i]}=={','.join(self.groups)};{filters_2[j]}=={province}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "{dimension},{filters_1[i]},{filters_2[j]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", "{dimension},{filters_1[i]},{filters_2[j]}.xlsx"), index=False)
                            elif i != 0 and j == 0 and k != 0:
                                if filters_1[i] == "video":
                                    data = []
                                    col_names = None
                                    for sub_type in sub_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_1[i]},{filters_3[k]}",
                                                endDate=f"{end_date}",
                                                filters=f"{filters_1[i]}=={','.join(self.vid_ids)};{filters_3[k]}=={sub_type}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "{dimension},{filters_1[i]},{filters_3[k]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", "{dimension},{filters_1[i]},{filters_3[k]}.xlsx"), index=False)
                                elif filters_1[i] == "group":
                                    data = []
                                    col_names = None
                                    for sub_type in sub_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_1[i]},{filters_3[k]}",
                                                endDate=f"{end_date}",
                                                filters=f"{filters_1[i]}=={','.join(self.groups)};{filters_3[k]}=={sub_type}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "{dimension},{filters_1[i]},{filters_3[k]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", "{dimension},{filters_1[i]},{filters_3[k]}.xlsx"), index=False)
                            elif i != 0 and j == 0 and k == 0:
                                if filters_1[i] == "video":
                                    data = []
                                    col_names = None
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{dimension},{filters_1[i]}",
                                            endDate=f"{end_date}",
                                            filters=f"{filters_1[i]}=={','.join(self.vid_ids)}",
                                            ids="channel==MINE",
                                            metrics=f"{','.join(metrics)}",
                                            startDate=f"{start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "{dimension},{filters_1[i]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", "{dimension},{filters_1[i]}.xlsx"), index=False)
                                elif filters_1[i] == "group":
                                    data = []
                                    col_names = None
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{dimension},{filters_1[i]}",
                                            endDate=f"{end_date}",
                                            filters=f"{filters_1[i]}=={','.join(self.groups)}",
                                            ids="channel==MINE",
                                            metrics=f"{','.join(metrics)}",
                                            startDate=f"{start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "{dimension},{filters_1[i]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", "{dimension},{filters_1[i]}.xlsx"), index=False)
                            elif i == 0 and j != 0 and k != 0:
                                data = []
                                col_names = None
                                for province in self.provinces:
                                    for sub_type in sub_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_2[j]},{filters_3[k]}",
                                                endDate=f"{end_date}",
                                                filters=f"{filters_2[j]}=={province};{filters_3[k]}=={sub_type}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "{dimension},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", "{dimension},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                            elif i == 0 and j != 0 and k == 0:
                                data = []
                                col_names = None
                                for province in self.provinces:
                                    response = self.execute_api_request(
                                         self.youtubeAnalytics.reports().query,
                                            dimensions=f"{dimension},{filters_2[j]}",
                                            endDate=f"{end_date}",
                                            filters=f"{filters_2[j]}=={province}",
                                            ids="channel==MINE",
                                            metrics=f"{','.join(metrics)}",
                                            startDate=f"{start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "{dimension},{filters_2[j]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", "{dimension},{filters_2[j]}.xlsx"), index=False)
                            elif i == 0 and j == 0 and k != 0:
                                data = []
                                col_names = None
                                for sub_type in sub_status:
                                    response = self.execute_api_request(
                                         self.youtubeAnalytics.reports().query,
                                            dimensions=f"{dimension},{filters_3[k]}",
                                            endDate=f"{end_date}",
                                            filters=f"{filters_3[k]}=={sub_type}",
                                            ids="channel==MINE",
                                            metrics=f"{','.join(metrics)}",
                                            startDate=f"{start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "{dimension},{filters_3[k]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", "{dimension},{filters_3[k]}.xlsx"), index=False)
                            elif i == 0 and j == 0 and k == 0:
                                data = []
                                col_names = None
                                response = self.execute_api_request(
                                     self.youtubeAnalytics.reports().query,
                                        dimensions=f"{dimension}",
                                        endDate=f"{end_date}",
                                        ids="channel==MINE",
                                        metrics=f"{','.join(metrics)}",
                                        startDate=f"{start_date}"
                                )
                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                if len(response["rows"]) != 0:
                                    for row in response["rows"]:
                                        data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", f"{dimension}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", f"{dimension}.xlsx"), index=False)
                        else:
                            if i != 0 and j != 0 and k != 0:
                                if filters_1[i] == "video":
                                    data = []
                                    col_names = None
                                    for province in self.provinces:
                                        for sub_type in sub_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{filters_1[i]},{filters_2[j]},{filters_3[k]}",
                                                    endDate=f"{end_date}",
                                                    filters=f"{filters_1[i]}=={','.join(self.vid_ids)};{filters_2[j]}=={province};{filters_3[k]}=={sub_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{','.join(metrics)}",
                                                    startDate=f"{start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", "{filters_1[i]},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                                elif filters_1[i] == "group":
                                    data = []
                                    col_names = None
                                    for province in self.provinces:
                                        for sub_type in sub_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{filters_1[i]},{filters_2[j]},{filters_3[k]}",
                                                    endDate=f"{end_date}",
                                                    filters=f"{filters_1[i]}=={','.join(self.groups)};{filters_2[j]}=={province};{filters_3[k]}=={sub_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{','.join(metrics)}",
                                                    startDate=f"{start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", "{filters_1[i]},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                            elif i != 0 and j != 0 and k == 0:
                                if filters_1[i] == "video":
                                    data = []
                                    col_names = None
                                    for province in self.provinces:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{filters_1[i]},{filters_2[j]}",
                                                endDate=f"{end_date}",
                                                filters=f"{filters_1[i]}=={','.join(self.vid_ids)};{filters_2[j]}=={province}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "{filters_1[i]},{filters_2[j]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", "{filters_1[i]},{filters_2[j]}.xlsx"), index=False)
                                elif filters_1[i] == "group":
                                    data = []
                                    col_names = None
                                    for province in self.provinces:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{filters_1[i]},{filters_2[j]}",
                                                endDate=f"{end_date}",
                                                filters=f"{filters_1[i]}=={','.join(self.groups)};{filters_2[j]}=={province}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "{filters_1[i]},{filters_2[j]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", "{filters_1[i]},{filters_2[j]}.xlsx"), index=False)
                            elif i != 0 and j == 0 and k != 0:
                                if filters_1[i] == "video":
                                    data = []
                                    col_names = None
                                    for sub_type in sub_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{filters_1[i]},{filters_3[k]}",
                                                endDate=f"{end_date}",
                                                filters=f"{filters_1[i]}=={','.join(self.vid_ids)};{filters_3[k]}=={sub_type}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "{filters_1[i]},{filters_3[k]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", "{filters_1[i]},{filters_3[k]}.xlsx"), index=False)
                                elif filters_1[i] == "group":
                                    data = []
                                    col_names = None
                                    for sub_type in sub_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{filters_1[i]},{filters_3[k]}",
                                                endDate=f"{end_date}",
                                                filters=f"{filters_1[i]}=={','.join(self.groups)};{filters_3[k]}=={sub_type}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "{filters_1[i]},{filters_3[k]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", "{filters_1[i]},{filters_3[k]}.xlsx"), index=False)
                            elif i != 0 and j == 0 and k == 0:
                                if filters_1[i] == "video":
                                    data = []
                                    col_names = None
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{filters_1[i]}",
                                            endDate=f"{end_date}",
                                            filters=f"{filters_1[i]}=={','.join(self.vid_ids)}",
                                            ids="channel==MINE",
                                            metrics=f"{','.join(metrics)}",
                                            startDate=f"{start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "{filters_1[i]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", "{filters_1[i]}.xlsx"), index=False)
                                elif filters_1[i] == "group":
                                    data = []
                                    col_names = None
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{filters_1[i]}",
                                            endDate=f"{end_date}",
                                            filters=f"{filters_1[i]}=={','.join(self.groups)}",
                                            ids="channel==MINE",
                                            metrics=f"{','.join(metrics)}",
                                            startDate=f"{start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "{filters_1[i]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", "{filters_1[i]}.xlsx"), index=False)
                            elif i == 0 and j != 0 and k != 0:
                                data = []
                                col_names = None
                                for province in self.provinces:
                                    for sub_type in sub_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{filters_2[j]},{filters_3[k]}",
                                                endDate=f"{end_date}",
                                                filters=f"{filters_2[j]}=={province};{filters_3[k]}=={sub_type}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", "{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                            elif i == 0 and j != 0 and k == 0:
                                data = []
                                col_names = None
                                for province in self.provinces:
                                    response = self.execute_api_request(
                                         self.youtubeAnalytics.reports().query,
                                            dimensions=f"{filters_2[j]}",
                                            endDate=f"{end_date}",
                                            filters=f"{filters_2[j]}=={province}",
                                            ids="channel==MINE",
                                            metrics=f"{','.join(metrics)}",
                                            startDate=f"{start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "{filters_2[j]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", "{filters_2[j]}.xlsx"), index=False)
                            elif i == 0 and j == 0 and k != 0:
                                data = []
                                col_names = None
                                for sub_type in sub_status:
                                    response = self.execute_api_request(
                                         self.youtubeAnalytics.reports().query,
                                            dimensions=f"{filters_3[k]}",
                                            endDate=f"{end_date}",
                                            filters=f"{filters_3[k]}=={sub_type}",
                                            ids="channel==MINE",
                                            metrics=f"{','.join(metrics)}",
                                            startDate=f"{start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "{filters_3[k]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", "{filters_3[k]}.xlsx"), index=False)
                            elif i == 0 and j == 0 and k == 0:
                                data = []
                                col_names = None
                                response = self.execute_api_request(
                                     self.youtubeAnalytics.reports().query,
                                        endDate=f"{end_date}",
                                        ids="channel==MINE",
                                        metrics=f"{','.join(metrics)}",
                                        startDate=f"{start_date}"
                                )
                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                if len(response["rows"]) != 0:
                                    for row in response["rows"]:
                                        data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "unfiltered.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", "unfiltered.xlsx"), index=False)
        return

