import os
import pandas as pd
from datetime import date
from auth import Auth

class Audience_Retention(Auth):

    def audience_retention(self):
        metrics = ["audienceWatchRatio", "relativeRetentionPerformance"]
        required_dimension = "elapsedVideoTimeRatio"
        required_filter = "video"
        audience_status = ["ORGANIC" ,"AD_INSTREAM", "AD_INDISPLAY"]
        sub_status = ["SUBSCRIBED", "UNSUBSCRIBED"]
        yt_status = ["CORE", "GAMING", "KIDS", "MUSIC"]

        filters_1 = ["", "audienceType"]
        filters_2 = ["", "subscribedStatus"]
        filters_3 = ["", "youtubeProduct"]
       
        for vid_id in self.vid_ids:
            for i in range(len(filters_1)):
                for j in range(len(filters_2)):
                    for k in range(len(filters_3)):
                        if i != 0 and j != 0 and k != 0:
                            data = []
                            col_names = None
                            for audience_type in audience_status:
                                for sub_type in sub_status:
                                    for yt_type in yt_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{required_filter},{required_dimension}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{required_filter}=={vid_id};{filters_1[i]}=={audience_type};{filters_2[j]}=={sub_type};{filters_3[k]}=={yt_type}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        col_names.insert(1, f"{filters_1[i]}")
                                        col_names.insert(2, f"{filters_2[j]}")
                                        col_names.insert(3, f"{filters_3[k]}")
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                row.insert(1, f"{audience_type}")
                                                row.insert(2, f"{sub_type}")
                                                row.insert(3, f"{yt_type}")
                                                data.append(row)
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "audience_retention", f"{vid_id},{filters_1[i]},{filters_2[j]},{filters_3[k]},{required_dimension}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "audience_retention", f"{vid_id},{filters_1[i]},{filters_2[j]},{filters_3[k]},{required_dimension}.xlsx"), index=False)
                        elif i != 0 and j != 0 and k == 0:
                            data = []
                            col_names = None
                            for audience_type in audience_status:
                                for sub_type in sub_status:
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{required_filter},{required_dimension}",
                                            endDate=f"{self.end_date}",
                                            filters=f"{required_filter}=={vid_id};{filters_1[i]}=={audience_type};{filters_2[j]}=={sub_type}",
                                            ids="channel==MINE",
                                            metrics=f"{','.join(metrics)}",
                                            startDate=f"{self.start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    col_names.insert(1, f"{filters_1[i]}")
                                    col_names.insert(2, f"{filters_2[j]}")
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            row.insert(1, f"{audience_type}")
                                            row.insert(2, f"{sub_type}")
                                            data.append(row)
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "audience_retention", f"{vid_id},{filters_1[i]},{filters_2[j]},{required_dimension}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "audience_retention", f"{vid_id},{filters_1[i]},{filters_2[j]},{required_dimension}.xlsx"), index=False)
                        elif i != 0 and j == 0 and k != 0:
                            data = []
                            col_names = None
                            for audience_type in audience_status:
                                for yt_type in yt_status:
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{required_filter},{required_dimension}",
                                            endDate=f"{self.end_date}",
                                            filters=f"{required_filter}=={vid_id};{filters_1[i]}=={audience_type};{filters_3[k]}=={yt_type}",
                                            ids="channel==MINE",
                                            metrics=f"{','.join(metrics)}",
                                            startDate=f"{self.start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    col_names.insert(1, f"{filters_1[i]}")
                                    col_names.insert(2, f"{filters_3[k]}")
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            row.insert(1, f"{audience_type}")
                                            row.insert(2, f"{yt_type}")
                                            data.append(row)
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "audience_retention", f"{vid_id},{filters_1[i]},{filters_3[k]},{required_dimension}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "audience_retention", f"{vid_id},{filters_1[i]},{filters_3[k]},{required_dimension}.xlsx"), index=False)
                        elif i != 0 and j == 0 and k == 0:
                            data = []
                            col_names = None
                            for audience_type in audience_status:
                                response = self.execute_api_request(
                                    self.youtubeAnalytics.reports().query,
                                        dimensions=f"{required_filter},{required_dimension}",
                                        endDate=f"{self.end_date}",
                                        filters=f"{required_filter}=={vid_id};{filters_1[i]}=={audience_type}",
                                        ids="channel==MINE",
                                        metrics=f"{','.join(metrics)}",
                                        startDate=f"{self.start_date}"
                                )
                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                col_names.insert(1, f"{filters_1[i]}")
                                if len(response["rows"]) != 0:
                                    for row in response["rows"]:
                                        row.insert(1, f"{audience_type}")
                                        data.append(row)
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "audience_retention", f"{vid_id},{filters_1[i]},{required_dimension}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "audience_retention", f"{vid_id},{filters_1[i]},{required_dimension}.xlsx"), index=False)
                        elif i == 0 and j != 0 and k != 0:
                            data = []
                            col_names = None
                            for sub_type in sub_status:
                                for yt_type in yt_status:
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{required_filter},{required_dimension}",
                                            endDate=f"{self.end_date}",
                                            filters=f"{required_filter}=={vid_id};{filters_2[j]}=={sub_type};{filters_3[k]}=={yt_type}",
                                            ids="channel==MINE",
                                            metrics=f"{','.join(metrics)}",
                                            startDate=f"{self.start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    col_names.insert(1, f"{filters_2[j]}")
                                    col_names.insert(2, f"{filters_3[k]}")
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            row.insert(1, f"{sub_type}")
                                            row.insert(2, f"{yt_type}")
                                            data.append(row)
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "audience_retention", f"{vid_id},{filters_2[j]},{filters_3[k]},{required_dimension}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "audience_retention", f"{vid_id},{filters_2[j]},{filters_3[k]},{required_dimension}.xlsx"), index=False)
                        elif i == 0 and j != 0 and k == 0:
                            data = []
                            col_names = None
                            for sub_type in sub_status:
                                response = self.execute_api_request(
                                    self.youtubeAnalytics.reports().query,
                                        dimensions=f"{required_filter},{required_dimension}",
                                        endDate=f"{self.end_date}",
                                        filters=f"{required_filter}=={vid_id};{filters_2[j]}=={sub_type}",
                                        ids="channel==MINE",
                                        metrics=f"{','.join(metrics)}",
                                        startDate=f"{self.start_date}"
                                )
                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                col_names.insert(1, f"{filters_2[j]}")
                                if len(response["rows"]) != 0:
                                    for row in response["rows"]:
                                        row.insert(1, f"{sub_type}")
                                        data.append(row)
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "audience_retention", f"{vid_id},{filters_2[j]},{required_dimension}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "audience_retention", f"{vid_id},{filters_2[j]},{required_dimension}.xlsx"), index=False)
                        elif i == 0 and j == 0 and k != 0:
                            data = []
                            col_names = None
                            for yt_type in yt_status:
                                response = self.execute_api_request(
                                    self.youtubeAnalytics.reports().query,
                                        dimensions=f"{required_filter},{required_dimension}",
                                        endDate=f"{self.end_date}",
                                        filters=f"{required_filter}=={vid_id};{filters_3[k]}=={yt_type}",
                                        ids="channel==MINE",
                                        metrics=f"{','.join(metrics)}",
                                        startDate=f"{self.start_date}"
                                )
                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                col_names.insert(1, f"{filters_3[k]}")
                                if len(response["rows"]) != 0:
                                    for row in response["rows"]:
                                        row.insert(1, f"{yt_type}")
                                        data.append(row)
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "audience_retention", f"{vid_id},{filters_3[k]},{required_dimension}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "audience_retention", f"{vid_id},{filters_3[k]},{required_dimension}.xlsx"), index=False)
                        elif i == 0 and j == 0 and k == 0:
                            data = []
                            col_names = None
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    dimensions=f"{required_filter},{required_dimension}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{required_filter}=={vid_id}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            if len(response["rows"]) != 0:
                                for row in response["rows"]:
                                    data.append(row)
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "csv", "audience_retention", f"{vid_id},{required_dimension}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "video_reports", "excel", "audience_retention", f"{vid_id},{required_dimension}.xlsx"), index=False)
        return

