import os
import pandas as pd
from auth import Auth

class Playback_Details_In_US(Auth):

    def playback_details_in_US(self):

        if not self.has_provinces:
            return

        metrics = ["views", "redViews", "estimatedMinutesWatched", "estimatedRedMinutesWatched", "averageViewDuration"]
        dimensions = ["province"]
        live_status = ["LIVE", "ON_DEMAND"]
        sub_status = ["SUBSCRIBED", "UNSUBSCRIBED"]
        yt_status = ["CORE", "GAMING", "KIDS", "MUSIC"]
        default_filter = "country==US"

        filters_to_remove = []
        if not self.has_groups:
            filters_to_remove.append("group")

        filters_1 = ["", "video", "group"]
        filters_2 = ["", "liveOrOnDemand"]
        filters_3 = ["", "subscribedStatus"]
        filters_4 = ["", "youtubeProduct"]

        for fil_to_rem in filters_to_remove:
            filters_1.remove(fil_to_rem)
        
        for dimension in dimensions:
            for i in range(len(filters_1)):
                for j in range(len(filters_2)):
                    for k in range(len(filters_3)):
                        for l in range(len(filters_4)):
                            if i != 0 and j != 0 and k != 0 and l != 0:
                                if filters_1[i] == "video":
                                    data = []
                                    col_names = None
                                    for live_type in live_status:
                                        for sub_type in sub_status:
                                            for yt_type in yt_status:
                                                response = self.execute_api_request(
                                                    self.youtubeAnalytics.reports().query,
                                                        dimensions=f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}",
                                                        endDate=f"{self.end_date}",
                                                        filters=f"{default_filter};{filters_1[i]}=={','.join(self.vid_ids)};{filters_2[j]}=={live_type};{filters_3[k]}=={sub_type};{filters_4[l]}=={yt_type}",
                                                        ids="channel==MINE",
                                                        metrics=f"{','.join(metrics)}",
                                                        startDate=f"{self.start_date}"
                                                )
                                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                if len(response["rows"]) != 0:
                                                    for row in response["rows"]:
                                                        data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.xlsx"), index=False)
                                elif filters_1[i] == "group":
                                    data = []
                                    col_names = None
                                    for live_type in live_status:
                                        for sub_type in sub_status:
                                            for yt_type in yt_status:
                                                response = self.execute_api_request(
                                                    self.youtubeAnalytics.reports().query,
                                                        dimensions=f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}",
                                                        endDate=f"{self.end_date}",
                                                        filters=f"{default_filter};{filters_1[i]}=={','.join(self.groups)};{filters_2[j]}=={live_type};{filters_3[k]}=={sub_type};{filters_4[l]}=={yt_type}",
                                                        ids="channel==MINE",
                                                        metrics=f"{','.join(metrics)}",
                                                        startDate=f"{self.start_date}"
                                                )
                                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                if len(response["rows"]) != 0:
                                                    for row in response["rows"]:
                                                        data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.xlsx"), index=False)
                            elif i != 0 and j != 0 and k != 0 and l == 0:
                                if filters_1[i] == "video":
                                    data = []
                                    col_names = None
                                    for live_type in live_status:
                                        for sub_type in sub_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{default_filter};{filters_1[i]}=={','.join(self.vid_ids)};{filters_2[j]}=={live_type};{filters_3[k]}=={sub_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{','.join(metrics)}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                                elif filters_1[i] == "group":
                                    data = []
                                    col_names = None
                                    for live_type in live_status:
                                        for sub_type in sub_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{default_filter};{filters_1[i]}=={','.join(self.groups)};{filters_2[j]}=={live_type};{filters_3[k]}=={sub_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{','.join(metrics)}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                            elif i != 0 and j != 0 and k == 0 and l != 0:
                                if filters_1[i] == "video":
                                    data = []
                                    col_names = None
                                    for live_type in live_status:
                                        for yt_type in yt_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{default_filter};{filters_1[i]}=={','.join(self.vid_ids)};{filters_2[j]}=={live_type};{filters_4[l]}=={yt_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{','.join(metrics)}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.xlsx"), index=False)
                                elif filters_1[i] == "group":
                                    data = []
                                    col_names = None
                                    for live_type in live_status:
                                        for yt_type in yt_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{default_filter};{filters_1[i]}=={','.join(self.groups)};{filters_2[j]}=={live_type};{filters_4[l]}=={yt_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{','.join(metrics)}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.xlsx"), index=False)
                            elif i != 0 and j != 0 and k == 0 and l == 0:
                                if filters_1[i] == "video":
                                    data = []
                                    col_names = None
                                    for live_type in live_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_1[i]},{filters_2[j]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{default_filter};{filters_1[i]}=={','.join(self.vid_ids)};{filters_2[j]}=={live_type}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_2[j]}.xlsx"), index=False)
                                elif filters_1[i] == "group":
                                    data = []
                                    col_names = None
                                    for live_type in live_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_1[i]},{filters_2[j]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{default_filter};{filters_1[i]}=={','.join(self.groups)};{filters_2[j]}=={live_type}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_2[j]}.xlsx"), index=False)
                            elif i != 0 and j == 0 and k != 0 and l != 0:
                                if filters_1[i] == "video":
                                    data = []
                                    col_names = None
                                    for sub_type in sub_status:
                                        for yt_type in yt_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{default_filter};{filters_1[i]}=={','.join(self.vid_ids)};{filters_3[k]}=={sub_type};{filters_4[l]}=={yt_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{','.join(metrics)}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.xlsx"), index=False)
                                elif filters_1[i] == "group":
                                    data = []
                                    col_names = None
                                    for sub_type in sub_status:
                                        for yt_type in yt_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{default_filter};{filters_1[i]}=={','.join(self.groups)};{filters_3[k]}=={sub_type};{filters_4[l]}=={yt_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{','.join(metrics)}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.xlsx"), index=False)
                            elif i != 0 and j == 0 and k != 0 and l == 0:
                                if filters_1[i] == "video":
                                    data = []
                                    col_names = None
                                    for sub_type in sub_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_1[i]},{filters_3[k]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{default_filter};{filters_1[i]}=={','.join(self.vid_ids)};{filters_3[k]}=={sub_type}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_3[k]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_3[k]}.xlsx"), index=False)
                                elif filters_1[i] == "group":
                                    data = []
                                    col_names = None
                                    for sub_type in sub_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_1[i]},{filters_3[k]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{default_filter};{filters_1[i]}=={','.join(self.groups)};{filters_3[k]}=={sub_type}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_3[k]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_3[k]}.xlsx"), index=False)
                            elif i != 0 and j == 0 and k == 0 and l != 0:
                                if filters_1[i] == "video":
                                    data = []
                                    col_names = None
                                    for yt_type in yt_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_1[i]},{filters_4[l]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{default_filter};{filters_1[i]}=={','.join(self.vid_ids)};{filters_4[l]}=={yt_type}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_4[l]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_4[l]}.xlsx"), index=False)
                                elif filters_1[i] == "group":
                                    data = []
                                    col_names = None
                                    for yt_type in yt_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_1[i]},{filters_4[l]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{default_filter};{filters_1[i]}=={','.join(self.groups)};{filters_4[l]}=={yt_type}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_4[l]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_4[l]}.xlsx"), index=False)
                            elif i != 0 and j == 0 and k == 0 and l == 0:
                                if filters_1[i] == "video":
                                    data = []
                                    col_names = None
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{dimension},{filters_1[i]}",
                                            endDate=f"{self.end_date}",
                                            filters=f"{default_filter};{filters_1[i]}=={','.join(self.vid_ids)}",
                                            ids="channel==MINE",
                                            metrics=f"{','.join(metrics)}",
                                            startDate=f"{self.start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension},{filters_1[i]}.xlsx"), index=False)
                                elif filters_1[i] == "group":
                                    data = []
                                    col_names = None
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{dimension},{filters_1[i]}",
                                            endDate=f"{self.end_date}",
                                            filters=f"{default_filter};{filters_1[i]}=={','.join(self.groups)}",
                                            ids="channel==MINE",
                                            metrics=f"{','.join(metrics)}",
                                            startDate=f"{self.start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension},{filters_1[i]}.xlsx"), index=False)
                            elif i == 0 and j != 0 and k != 0 and l != 0:
                                data = []
                                col_names = None
                                for live_type in live_status:
                                    for sub_type in sub_status:
                                        for yt_type in yt_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{default_filter};{filters_2[j]}=={live_type};{filters_3[k]}=={sub_type};{filters_4[l]}=={yt_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{','.join(metrics)}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.xlsx"), index=False)
                            elif i == 0 and j != 0 and k != 0 and l == 0:
                                data = []
                                col_names = None
                                for live_type in live_status:
                                    for sub_type in sub_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_2[j]},{filters_3[k]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{default_filter};{filters_2[j]}=={live_type};{filters_3[k]}=={sub_type}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                            elif i == 0 and j != 0 and k == 0 and l != 0:
                                data = []
                                col_names = None
                                for live_type in live_status:
                                    for yt_type in yt_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_2[j]},{filters_4[l]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{default_filter};{filters_2[j]}=={live_type};{filters_4[l]}=={yt_type}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_2[j]},{filters_4[l]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension},{filters_2[j]},{filters_4[l]}.xlsx"), index=False)
                            elif i == 0 and j != 0 and k == 0 and l == 0:
                                data = []
                                col_names = None
                                for live_type in live_status:
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{dimension},{filters_2[j]}",
                                            endDate=f"{self.end_date}",
                                            filters=f"{default_filter};{filters_2[j]}=={live_type}",
                                            ids="channel==MINE",
                                            metrics=f"{','.join(metrics)}",
                                            startDate=f"{self.start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_2[j]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension},{filters_2[j]}.xlsx"), index=False)
                            elif i == 0 and j == 0 and k != 0 and l != 0:
                                data = []
                                col_names = None
                                for sub_type in sub_status:
                                    for yt_type in yt_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_3[k]},{filters_4[l]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{default_filter};{filters_3[k]}=={sub_type};{filters_4[l]}=={yt_type}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_3[k]},{filters_4[l]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension},{filters_3[k]},{filters_4[l]}.xlsx"), index=False)
                            elif i == 0 and j == 0 and k != 0 and l == 0:
                                data = []
                                col_names = None
                                for sub_type in sub_status:
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{dimension},{filters_3[k]}",
                                            endDate=f"{self.end_date}",
                                            filters=f"{default_filter};{filters_3[k]}=={sub_type}",
                                            ids="channel==MINE",
                                            metrics=f"{','.join(metrics)}",
                                            startDate=f"{self.start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_3[k]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension},{filters_3[k]}.xlsx"), index=False)
                            elif i == 0 and j == 0 and k == 0 and l != 0:
                                data = []
                                col_names = None
                                for yt_type in yt_status:
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{dimension},{filters_4[l]}",
                                            endDate=f"{self.end_date}",
                                            filters=f"{default_filter};{filters_4[l]}=={yt_type}",
                                            ids="channel==MINE",
                                            metrics=f"{','.join(metrics)}",
                                            startDate=f"{self.start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_4[l]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension},{filters_4[l]}.xlsx"), index=False)
                            elif i == 0 and j == 0 and k == 0 and l == 0:
                                data = []
                                col_names = None
                                response = self.execute_api_request(
                                    self.youtubeAnalytics.reports().query,
                                        dimensions=f"{dimension}",
                                        endDate=f"{self.end_date}",
                                        filters=f"{default_filter}",
                                        ids="channel==MINE",
                                        metrics=f"{','.join(metrics)}",
                                        startDate=f"{self.start_date}"
                                )
                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                if len(response["rows"]) != 0:
                                    for row in response["rows"]:
                                        data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "playback_details_in_US", f"{dimension}.xlsx"), index=False)
        return
