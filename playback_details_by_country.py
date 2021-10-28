import os
import pandas as pd
from datetime import date
from auth import Auth

class Playback_Details_By_Country(Auth):

    def playback_details_by_country(self):
        metrics = ["views", "redViews", "estimatedMinutesWatched", "estimatedRedMinutesWatched", "averageViewDuration"]
        dimensions = ["country"]
        live_status = ["LIVE", "ON_DEMAND"]
        sub_status = ["SUBSCRIBED", "UNSUBSCRIBED"]
        yt_status = ["CORE", "GAMING", "KIDS", "MUSIC"]

        filters_to_remove_1 = []
        filters_to_remove_2 = []
        if not self.has_continents:
            filters_to_remove_1.append("continent")
        if not self.has_subcontinents:
            filters_to_remove_1.append("subContinent")
        if not self.has_groups:
            filters_to_remove_2.append("group")

        filters_1 = ["", "continent", "subContinent"]
        filters_2 = ["", "video", "group"]
        filters_3 = ["", "liveOrOnDemand"]
        filters_4 = ["", "subscribedStatus"]
        filters_5 = ["", "youtubeProduct"]

        for fil_to_rem in filters_to_remove_1:
            filters_1.remove(fil_to_rem)
        for fil_to_rem in filters_to_remove_2:
            filters_2.remove(fil_to_rem)
        
        for dimension in dimensions:
            for i in range(len(filters_1)):
                for j in range(len(filters_2)):
                    for k in range(len(filters_3)):
                        for l in range(len(filters_4)):
                            for m in range(len(filters_5)):
                                if i != 0 and j != 0 and k != 0 and l != 0 and m != 0:
                                    if filters_1[i] == "continent":
                                        if filters_2[j] == "video":
                                            data = []
                                            col_names = None
                                            for continent in self.continents:
                                                for live_type in live_status:
                                                    for sub_type in sub_status:
                                                        for yt_type in yt_status:
                                                            response = self.execute_api_request(
                                                                self.youtubeAnalytics.reports().query,
                                                                    dimensions=f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}",
                                                                    endDate=f"{self.end_date}",
                                                                    filters=f"{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.vid_ids)};{filters_3[k]}=={live_type};{filters_4[l]}=={sub_type};{filters_5[m]}=={yt_type}",
                                                                    ids="channel==MINE",
                                                                    metrics=f"{','.join(metrics)}",
                                                                    startDate=f"{self.start_date}"
                                                            )
                                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                            col_names.insert(1, f"{filters_1[i]}")
                                                            if len(response["rows"]) != 0:
                                                                for row in response["rows"]:
                                                                    row.insert(1, f"{continent}")
                                                                    data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.xlsx"), index=False)
                                        elif filters_2[j] == "group":
                                            data = []
                                            col_names = None
                                            for continent in self.continents:
                                                for live_type in live_status:
                                                    for sub_type in sub_status:
                                                        for yt_type in yt_status:
                                                            response = self.execute_api_request(
                                                                self.youtubeAnalytics.reports().query,
                                                                    dimensions=f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}",
                                                                    endDate=f"{self.end_date}",
                                                                    filters=f"{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.groups)};{filters_3[k]}=={live_type};{filters_4[l]}=={sub_type};{filters_5[m]}=={yt_type}",
                                                                    ids="channel==MINE",
                                                                    metrics=f"{','.join(metrics)}",
                                                                    startDate=f"{self.start_date}"
                                                            )
                                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                            col_names.insert(1, f"{filters_1[i]}")
                                                            if len(response["rows"]) != 0:
                                                                for row in response["rows"]:
                                                                    row.insert(1, f"{continent}")
                                                                    data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.xlsx"), index=False)
                                    elif filters_1[i] == "subContinent":
                                        if filters_2[j] == "video":
                                            data = []
                                            col_names = None
                                            for subcontinent in self.subcontinents:
                                                for live_type in live_status:
                                                    for sub_type in sub_status:
                                                        for yt_type in yt_status:
                                                            response = self.execute_api_request(
                                                                self.youtubeAnalytics.reports().query,
                                                                    dimensions=f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}",
                                                                    endDate=f"{self.end_date}",
                                                                    filters=f"{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.vid_ids)};{filters_3[k]}=={live_type};{filters_4[l]}=={sub_type};{filters_5[m]}=={yt_type}",
                                                                    ids="channel==MINE",
                                                                    metrics=f"{','.join(metrics)}",
                                                                    startDate=f"{self.start_date}"
                                                            )
                                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                            col_names.insert(1, f"{filters_1[i]}")
                                                            if len(response["rows"]) != 0:
                                                                for row in response["rows"]:
                                                                    row.insert(1, f"{subcontinent}")
                                                                    data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.xlsx"), index=False)
                                        elif filters_2[j] == "group":
                                            data = []
                                            col_names = None
                                            for subcontinent in self.subcontinents:
                                                for live_type in live_status:
                                                    for sub_type in sub_status:
                                                        for yt_type in yt_status:
                                                            response = self.execute_api_request(
                                                                self.youtubeAnalytics.reports().query,
                                                                    dimensions=f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}",
                                                                    endDate=f"{self.end_date}",
                                                                    filters=f"{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.groups)};{filters_3[k]}=={live_type};{filters_4[l]}=={sub_type};{filters_5[m]}=={yt_type}",
                                                                    ids="channel==MINE",
                                                                    metrics=f"{','.join(metrics)}",
                                                                    startDate=f"{self.start_date}"
                                                            )
                                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                            col_names.insert(1, f"{filters_1[i]}")
                                                            if len(response["rows"]) != 0:
                                                                for row in response["rows"]:
                                                                    row.insert(1, f"{subcontinent}")
                                                                    data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.xlsx"), index=False)
                                elif i != 0 and j != 0 and k != 0 and l != 0 and m == 0:
                                    if filters_1[i] == "continent":
                                        if filters_2[j] == "video":
                                            data = []
                                            col_names = None
                                            for continent in self.continents:
                                                for live_type in live_status:
                                                    for sub_type in sub_status:
                                                        response = self.execute_api_request(
                                                            self.youtubeAnalytics.reports().query,
                                                                dimensions=f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}",
                                                                endDate=f"{self.end_date}",
                                                                filters=f"{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.vid_ids)};{filters_3[k]}=={live_type};{filters_4[l]}=={sub_type}",
                                                                ids="channel==MINE",
                                                                metrics=f"{','.join(metrics)}",
                                                                startDate=f"{self.start_date}"
                                                        )
                                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                        col_names.insert(1, f"{filters_1[i]}")
                                                        if len(response["rows"]) != 0:
                                                            for row in response["rows"]:
                                                                row.insert(1, f"{continent}")
                                                                data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.xlsx"), index=False)
                                        elif filters_2[j] == "group":
                                            data = []
                                            col_names = None
                                            for continent in self.continents:
                                                for live_type in live_status:
                                                    for sub_type in sub_status:
                                                        response = self.execute_api_request(
                                                            self.youtubeAnalytics.reports().query,
                                                                dimensions=f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}",
                                                                endDate=f"{self.end_date}",
                                                                filters=f"{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.groups)};{filters_3[k]}=={live_type};{filters_4[l]}=={sub_type}",
                                                                ids="channel==MINE",
                                                                metrics=f"{','.join(metrics)}",
                                                                startDate=f"{self.start_date}"
                                                        )
                                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                        col_names.insert(1, f"{filters_1[i]}")
                                                        if len(response["rows"]) != 0:
                                                            for row in response["rows"]:
                                                                row.insert(1, f"{continent}")
                                                                data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.xlsx"), index=False)
                                    elif filters_1[i] == "subContinent":
                                        if filters_2[j] == "video":
                                            data = []
                                            col_names = None
                                            for subcontinent in self.subcontinents:
                                                for live_type in live_status:
                                                    for sub_type in sub_status:
                                                        response = self.execute_api_request(
                                                            self.youtubeAnalytics.reports().query,
                                                                dimensions=f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}",
                                                                endDate=f"{self.end_date}",
                                                                filters=f"{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.vid_ids)};{filters_3[k]}=={live_type};{filters_4[l]}=={sub_type}",
                                                                ids="channel==MINE",
                                                                metrics=f"{','.join(metrics)}",
                                                                startDate=f"{self.start_date}"
                                                        )
                                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                        col_names.insert(1, f"{filters_1[i]}")
                                                        if len(response["rows"]) != 0:
                                                            for row in response["rows"]:
                                                                row.insert(1, f"{subcontinent}")
                                                                data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.xlsx"), index=False)
                                        elif filters_2[j] == "group":
                                            data = []
                                            col_names = None
                                            for subcontinent in self.subcontinents:
                                                for live_type in live_status:
                                                    for sub_type in sub_status:
                                                        response = self.execute_api_request(
                                                            self.youtubeAnalytics.reports().query,
                                                                dimensions=f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}",
                                                                endDate=f"{self.end_date}",
                                                                filters=f"{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.groups)};{filters_3[k]}=={live_type};{filters_4[l]}=={sub_type}",
                                                                ids="channel==MINE",
                                                                metrics=f"{','.join(metrics)}",
                                                                startDate=f"{self.start_date}"
                                                        )
                                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                        col_names.insert(1, f"{filters_1[i]}")
                                                        if len(response["rows"]) != 0:
                                                            for row in response["rows"]:
                                                                row.insert(1, f"{subcontinent}")
                                                                data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.xlsx"), index=False)
                                elif i != 0 and j != 0 and k != 0 and l == 0 and m != 0:
                                    if filters_1[i] == "continent":
                                        if filters_2[j] == "video":
                                            data = []
                                            col_names = None
                                            for continent in self.continents:
                                                for live_type in live_status:
                                                    for yt_type in yt_status:
                                                        response = self.execute_api_request(
                                                            self.youtubeAnalytics.reports().query,
                                                                dimensions=f"{dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]}",
                                                                endDate=f"{self.end_date}",
                                                                filters=f"{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.vid_ids)};{filters_3[k]}=={live_type};{filters_5[m]}=={yt_type}",
                                                                ids="channel==MINE",
                                                                metrics=f"{','.join(metrics)}",
                                                                startDate=f"{self.start_date}"
                                                        )
                                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                        col_names.insert(1, f"{filters_1[i]}")
                                                        if len(response["rows"]) != 0:
                                                            for row in response["rows"]:
                                                                row.insert(1, f"{continent}")
                                                                data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_5[m]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_5[m]}.xlsx"), index=False)
                                        elif filters_2[j] == "group":
                                            data = []
                                            col_names = None
                                            for continent in self.continents:
                                                for live_type in live_status:
                                                    for yt_type in yt_status:
                                                        response = self.execute_api_request(
                                                            self.youtubeAnalytics.reports().query,
                                                                dimensions=f"{dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]}",
                                                                endDate=f"{self.end_date}",
                                                                filters=f"{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.groups)};{filters_3[k]}=={live_type};{filters_5[m]}=={yt_type}",
                                                                ids="channel==MINE",
                                                                metrics=f"{','.join(metrics)}",
                                                                startDate=f"{self.start_date}"
                                                        )
                                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                        col_names.insert(1, f"{filters_1[i]}")
                                                        if len(response["rows"]) != 0:
                                                            for row in response["rows"]:
                                                                row.insert(1, f"{continent}")
                                                                data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_5[m]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_5[m]}.xlsx"), index=False)
                                    elif filters_1[i] == "subContinent":
                                        if filters_2[j] == "video":
                                            data = []
                                            col_names = None
                                            for subcontinent in self.subcontinents:
                                                for live_type in live_status:
                                                    for yt_type in yt_status:
                                                        response = self.execute_api_request(
                                                            self.youtubeAnalytics.reports().query,
                                                                dimensions=f"{dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]}",
                                                                endDate=f"{self.end_date}",
                                                                filters=f"{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.vid_ids)};{filters_3[k]}=={live_type};{filters_5[m]}=={yt_type}",
                                                                ids="channel==MINE",
                                                                metrics=f"{','.join(metrics)}",
                                                                startDate=f"{self.start_date}"
                                                        )
                                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                        col_names.insert(1, f"{filters_1[i]}")
                                                        if len(response["rows"]) != 0:
                                                            for row in response["rows"]:
                                                                row.insert(1, f"{subcontinent}")
                                                                data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_5[m]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_5[m]}.xlsx"), index=False)
                                        elif filters_2[j] == "group":
                                            data = []
                                            col_names = None
                                            for subcontinent in self.subcontinents:
                                                for live_type in live_status:
                                                    for yt_type in yt_status:
                                                        response = self.execute_api_request(
                                                            self.youtubeAnalytics.reports().query,
                                                                dimensions=f"{dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]}",
                                                                endDate=f"{self.end_date}",
                                                                filters=f"{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.groups)};{filters_3[k]}=={live_type};{filters_5[m]}=={yt_type}",
                                                                ids="channel==MINE",
                                                                metrics=f"{','.join(metrics)}",
                                                                startDate=f"{self.start_date}"
                                                        )
                                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                        col_names.insert(1, f"{filters_1[i]}")
                                                        if len(response["rows"]) != 0:
                                                            for row in response["rows"]:
                                                                row.insert(1, f"{subcontinent}")
                                                                data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_5[m]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_5[m]}.xlsx"), index=False)
                                elif i != 0 and j != 0 and k != 0 and l == 0 and m == 0:
                                    if filters_1[i] == "continent":
                                        if filters_2[j] == "video":
                                            data = []
                                            col_names = None
                                            for continent in self.continents:
                                                for live_type in live_status:
                                                    response = self.execute_api_request(
                                                        self.youtubeAnalytics.reports().query,
                                                            dimensions=f"{dimension},{filters_2[j]},{filters_3[k]}",
                                                            endDate=f"{self.end_date}",
                                                            filters=f"{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.vid_ids)};{filters_3[k]}=={live_type}",
                                                            ids="channel==MINE",
                                                            metrics=f"{','.join(metrics)}",
                                                            startDate=f"{self.start_date}"
                                                    )
                                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                    col_names.insert(1, f"{filters_1[i]}")
                                                    if len(response["rows"]) != 0:
                                                        for row in response["rows"]:
                                                            row.insert(1, f"{continent}")
                                                            data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                                        elif filters_2[j] == "group":
                                            data = []
                                            col_names = None
                                            for continent in self.continents:
                                                for live_type in live_status:
                                                    response = self.execute_api_request(
                                                        self.youtubeAnalytics.reports().query,
                                                            dimensions=f"{dimension},{filters_2[j]},{filters_3[k]}",
                                                            endDate=f"{self.end_date}",
                                                            filters=f"{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.groups)};{filters_3[k]}=={live_type}",
                                                            ids="channel==MINE",
                                                            metrics=f"{','.join(metrics)}",
                                                            startDate=f"{self.start_date}"
                                                    )
                                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                    col_names.insert(1, f"{filters_1[i]}")
                                                    if len(response["rows"]) != 0:
                                                        for row in response["rows"]:
                                                            row.insert(1, f"{continent}")
                                                            data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                                    elif filters_1[i] == "subContinent":
                                        if filters_2[j] == "video":
                                            data = []
                                            col_names = None
                                            for subcontinent in self.subcontinents:
                                                for live_type in live_status:
                                                    response = self.execute_api_request(
                                                        self.youtubeAnalytics.reports().query,
                                                            dimensions=f"{dimension},{filters_2[j]},{filters_3[k]}",
                                                            endDate=f"{self.end_date}",
                                                            filters=f"{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.vid_ids)};{filters_3[k]}=={live_type}",
                                                            ids="channel==MINE",
                                                            metrics=f"{','.join(metrics)}",
                                                            startDate=f"{self.start_date}"
                                                    )
                                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                    col_names.insert(1, f"{filters_1[i]}")
                                                    if len(response["rows"]) != 0:
                                                        for row in response["rows"]:
                                                            row.insert(1, f"{subcontinent}")
                                                            data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                                        elif filters_2[j] == "group":
                                            data = []
                                            col_names = None
                                            for subcontinent in self.subcontinents:
                                                for live_type in live_status:
                                                    response = self.execute_api_request(
                                                        self.youtubeAnalytics.reports().query,
                                                            dimensions=f"{dimension},{filters_2[j]},{filters_3[k]}",
                                                            endDate=f"{self.end_date}",
                                                            filters=f"{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.groups)};{filters_3[k]}=={live_type}",
                                                            ids="channel==MINE",
                                                            metrics=f"{','.join(metrics)}",
                                                            startDate=f"{self.start_date}"
                                                    )
                                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                    col_names.insert(1, f"{filters_1[i]}")
                                                    if len(response["rows"]) != 0:
                                                        for row in response["rows"]:
                                                            row.insert(1, f"{subcontinent}")
                                                            data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                                elif i != 0 and j != 0 and k == 0 and l != 0 and m != 0:
                                    if filters_1[i] == "continent":
                                        if filters_2[j] == "video":
                                            data = []
                                            col_names = None
                                            for continent in self.continents:
                                                for sub_type in sub_status:
                                                    for yt_type in yt_status:
                                                        response = self.execute_api_request(
                                                            self.youtubeAnalytics.reports().query,
                                                                dimensions=f"{dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]}",
                                                                endDate=f"{self.end_date}",
                                                                filters=f"{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.vid_ids)};{filters_4[l]}=={sub_type};{filters_5[m]}=={yt_type}",
                                                                ids="channel==MINE",
                                                                metrics=f"{','.join(metrics)}",
                                                                startDate=f"{self.start_date}"
                                                        )
                                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                        col_names.insert(1, f"{filters_1[i]}")
                                                        if len(response["rows"]) != 0:
                                                            for row in response["rows"]:
                                                                row.insert(1, f"{continent}")
                                                                data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{filters_5[m]}.xlsx"), index=False)
                                        elif filters_2[j] == "group":
                                            data = []
                                            col_names = None
                                            for continent in self.continents:
                                                for sub_type in sub_status:
                                                    for yt_type in yt_status:
                                                        response = self.execute_api_request(
                                                            self.youtubeAnalytics.reports().query,
                                                                dimensions=f"{dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]}",
                                                                endDate=f"{self.end_date}",
                                                                filters=f"{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.groups)};{filters_4[l]}=={sub_type};{filters_5[m]}=={yt_type}",
                                                                ids="channel==MINE",
                                                                metrics=f"{','.join(metrics)}",
                                                                startDate=f"{self.start_date}"
                                                        )
                                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                        col_names.insert(1, f"{filters_1[i]}")
                                                        if len(response["rows"]) != 0:
                                                            for row in response["rows"]:
                                                                row.insert(1, f"{continent}")
                                                                data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{filters_5[m]}.xlsx"), index=False)
                                    elif filters_1[i] == "subContinent":
                                        if filters_2[j] == "video":
                                            data = []
                                            col_names = None
                                            for subcontinent in self.subcontinents:
                                                for sub_type in sub_status:
                                                    for yt_type in yt_status:
                                                        response = self.execute_api_request(
                                                            self.youtubeAnalytics.reports().query,
                                                                dimensions=f"{dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]}",
                                                                endDate=f"{self.end_date}",
                                                                filters=f"{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.vid_ids)};{filters_4[l]}=={sub_type};{filters_5[m]}=={yt_type}",
                                                                ids="channel==MINE",
                                                                metrics=f"{','.join(metrics)}",
                                                                startDate=f"{self.start_date}"
                                                        )
                                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                        col_names.insert(1, f"{filters_1[i]}")
                                                        if len(response["rows"]) != 0:
                                                            for row in response["rows"]:
                                                                row.insert(1, f"{subcontinent}")
                                                                data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{filters_5[m]}.xlsx"), index=False)
                                        elif filters_2[j] == "group":
                                            data = []
                                            col_names = None
                                            for subcontinent in self.subcontinents:
                                                for sub_type in sub_status:
                                                    for yt_type in yt_status:
                                                        response = self.execute_api_request(
                                                            self.youtubeAnalytics.reports().query,
                                                                dimensions=f"{dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]}",
                                                                endDate=f"{self.end_date}",
                                                                filters=f"{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.groups)};{filters_4[l]}=={sub_type};{filters_5[m]}=={yt_type}",
                                                                ids="channel==MINE",
                                                                metrics=f"{','.join(metrics)}",
                                                                startDate=f"{self.start_date}"
                                                        )
                                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                        col_names.insert(1, f"{filters_1[i]}")
                                                        if len(response["rows"]) != 0:
                                                            for row in response["rows"]:
                                                                row.insert(1, f"{subcontinent}")
                                                                data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{filters_5[m]}.xlsx"), index=False)
                                elif i != 0 and j != 0 and k == 0 and l != 0 and m == 0:
                                    if filters_1[i] == "continent":
                                        if filters_2[j] == "video":
                                            data = []
                                            col_names = None
                                            for continent in self.continents:
                                                for sub_type in sub_status:
                                                    response = self.execute_api_request(
                                                        self.youtubeAnalytics.reports().query,
                                                            dimensions=f"{dimension},{filters_2[j]},{filters_4[l]}",
                                                            endDate=f"{self.end_date}",
                                                            filters=f"{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.vid_ids)};{filters_4[l]}=={sub_type}",
                                                            ids="channel==MINE",
                                                            metrics=f"{','.join(metrics)}",
                                                            startDate=f"{self.start_date}"
                                                    )
                                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                    col_names.insert(1, f"{filters_1[i]}")
                                                    if len(response["rows"]) != 0:
                                                        for row in response["rows"]:
                                                            row.insert(1, f"{continent}")
                                                            data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.xlsx"), index=False)
                                        elif filters_2[j] == "group":
                                            data = []
                                            col_names = None
                                            for continent in self.continents:
                                                for sub_type in sub_status:
                                                    response = self.execute_api_request(
                                                        self.youtubeAnalytics.reports().query,
                                                            dimensions=f"{dimension},{filters_2[j]},{filters_4[l]}",
                                                            endDate=f"{self.end_date}",
                                                            filters=f"{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.groups)};{filters_4[l]}=={sub_type}",
                                                            ids="channel==MINE",
                                                            metrics=f"{','.join(metrics)}",
                                                            startDate=f"{self.start_date}"
                                                    )
                                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                    col_names.insert(1, f"{filters_1[i]}")
                                                    if len(response["rows"]) != 0:
                                                        for row in response["rows"]:
                                                            row.insert(1, f"{continent}")
                                                            data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.xlsx"), index=False)
                                    elif filters_1[i] == "subContinent":
                                        if filters_2[j] == "video":
                                            data = []
                                            col_names = None
                                            for subcontinent in self.subcontinents:
                                                for sub_type in sub_status:
                                                    response = self.execute_api_request(
                                                        self.youtubeAnalytics.reports().query,
                                                            dimensions=f"{dimension},{filters_2[j]},{filters_4[l]}",
                                                            endDate=f"{self.end_date}",
                                                            filters=f"{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.vid_ids)};{filters_4[l]}=={sub_type}",
                                                            ids="channel==MINE",
                                                            metrics=f"{','.join(metrics)}",
                                                            startDate=f"{self.start_date}"
                                                    )
                                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                    col_names.insert(1, f"{filters_1[i]}")
                                                    if len(response["rows"]) != 0:
                                                        for row in response["rows"]:
                                                            row.insert(1, f"{subcontinent}")
                                                            data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.xlsx"), index=False)
                                        elif filters_2[j] == "group":
                                            data = []
                                            col_names = None
                                            for subcontinent in self.subcontinents:
                                                for sub_type in sub_status:
                                                    response = self.execute_api_request(
                                                        self.youtubeAnalytics.reports().query,
                                                            dimensions=f"{dimension},{filters_2[j]},{filters_4[l]}",
                                                            endDate=f"{self.end_date}",
                                                            filters=f"{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.groups)};{filters_4[l]}=={sub_type}",
                                                            ids="channel==MINE",
                                                            metrics=f"{','.join(metrics)}",
                                                            startDate=f"{self.start_date}"
                                                    )
                                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                    col_names.insert(1, f"{filters_1[i]}")
                                                    if len(response["rows"]) != 0:
                                                        for row in response["rows"]:
                                                            row.insert(1, f"{subcontinent}")
                                                            data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.xlsx"), index=False)
                                elif i != 0 and j != 0 and k == 0 and l == 0 and m != 0:
                                    if filters_1[i] == "continent":
                                        if filters_2[j] == "video":
                                            data = []
                                            col_names = None
                                            for continent in self.continents:
                                                for yt_type in yt_status:
                                                    response = self.execute_api_request(
                                                        self.youtubeAnalytics.reports().query,
                                                            dimensions=f"{dimension},{filters_2[j]},{filters_5[m]}",
                                                            endDate=f"{self.end_date}",
                                                            filters=f"{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.vid_ids)};{filters_5[m]}=={yt_type}",
                                                            ids="channel==MINE",
                                                            metrics=f"{','.join(metrics)}",
                                                            startDate=f"{self.start_date}"
                                                    )
                                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                    col_names.insert(1, f"{filters_1[i]}")
                                                    if len(response["rows"]) != 0:
                                                        for row in response["rows"]:
                                                            row.insert(1, f"{continent}")
                                                            data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_5[m]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_5[m]}.xlsx"), index=False)
                                        elif filters_2[j] == "group":
                                            data = []
                                            col_names = None
                                            for continent in self.continents:
                                                for yt_type in yt_status:
                                                    response = self.execute_api_request(
                                                        self.youtubeAnalytics.reports().query,
                                                            dimensions=f"{dimension},{filters_2[j]},{filters_5[m]}",
                                                            endDate=f"{self.end_date}",
                                                            filters=f"{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.groups)};{filters_5[m]}=={yt_type}",
                                                            ids="channel==MINE",
                                                            metrics=f"{','.join(metrics)}",
                                                            startDate=f"{self.start_date}"
                                                    )
                                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                    col_names.insert(1, f"{filters_1[i]}")
                                                    if len(response["rows"]) != 0:
                                                        for row in response["rows"]:
                                                            row.insert(1, f"{continent}")
                                                            data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_5[m]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_5[m]}.xlsx"), index=False)
                                    elif filters_1[i] == "subContinent":
                                        if filters_2[j] == "video":
                                            data = []
                                            col_names = None
                                            for subcontinent in self.subcontinents:
                                                for yt_type in yt_status:
                                                    response = self.execute_api_request(
                                                        self.youtubeAnalytics.reports().query,
                                                            dimensions=f"{dimension},{filters_2[j]},{filters_5[m]}",
                                                            endDate=f"{self.end_date}",
                                                            filters=f"{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.vid_ids)};{filters_5[m]}=={yt_type}",
                                                            ids="channel==MINE",
                                                            metrics=f"{','.join(metrics)}",
                                                            startDate=f"{self.start_date}"
                                                    )
                                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                    col_names.insert(1, f"{filters_1[i]}")
                                                    if len(response["rows"]) != 0:
                                                        for row in response["rows"]:
                                                            row.insert(1, f"{subcontinent}")
                                                            data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_5[m]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_5[m]}.xlsx"), index=False)
                                        elif filters_2[j] == "group":
                                            data = []
                                            col_names = None
                                            for subcontinent in self.subcontinents:
                                                for yt_type in yt_status:
                                                    response = self.execute_api_request(
                                                        self.youtubeAnalytics.reports().query,
                                                            dimensions=f"{dimension},{filters_2[j]},{filters_5[m]}",
                                                            endDate=f"{self.end_date}",
                                                            filters=f"{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.groups)};{filters_5[m]}=={yt_type}",
                                                            ids="channel==MINE",
                                                            metrics=f"{','.join(metrics)}",
                                                            startDate=f"{self.start_date}"
                                                    )
                                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                    col_names.insert(1, f"{filters_1[i]}")
                                                    if len(response["rows"]) != 0:
                                                        for row in response["rows"]:
                                                            row.insert(1, f"{subcontinent}")
                                                            data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_5[m]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_5[m]}.xlsx"), index=False)
                                elif i != 0 and j != 0 and k == 0 and l == 0 and m == 0:
                                    if filters_1[i] == "continent":
                                        if filters_2[j] == "video":
                                            data = []
                                            col_names = None
                                            for continent in self.continents:
                                                response = self.execute_api_request(
                                                    self.youtubeAnalytics.reports().query,
                                                        dimensions=f"{dimension},{filters_2[j]}",
                                                        endDate=f"{self.end_date}",
                                                        filters=f"{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.vid_ids)}",
                                                        ids="channel==MINE",
                                                        metrics=f"{','.join(metrics)}",
                                                        startDate=f"{self.start_date}"
                                                )
                                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                col_names.insert(1, f"{filters_1[i]}")
                                                if len(response["rows"]) != 0:
                                                    for row in response["rows"]:
                                                        row.insert(1, f"{continent}")
                                                        data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]}.xlsx"), index=False)
                                        elif filters_2[j] == "group":
                                            data = []
                                            col_names = None
                                            for continent in self.continents:
                                                response = self.execute_api_request(
                                                    self.youtubeAnalytics.reports().query,
                                                        dimensions=f"{dimension},{filters_2[j]}",
                                                        endDate=f"{self.end_date}",
                                                        filters=f"{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.groups)}",
                                                        ids="channel==MINE",
                                                        metrics=f"{','.join(metrics)}",
                                                        startDate=f"{self.start_date}"
                                                )
                                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                col_names.insert(1, f"{filters_1[i]}")
                                                if len(response["rows"]) != 0:
                                                    for row in response["rows"]:
                                                        row.insert(1, f"{continent}")
                                                        data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]}.xlsx"), index=False)
                                    elif filters_1[i] == "subContinent":
                                        if filters_2[j] == "video":
                                            data = []
                                            col_names = None
                                            for subcontinent in self.subcontinents:
                                                response = self.execute_api_request(
                                                    self.youtubeAnalytics.reports().query,
                                                        dimensions=f"{dimension},{filters_2[j]}",
                                                        endDate=f"{self.end_date}",
                                                        filters=f"{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.vid_ids)}",
                                                        ids="channel==MINE",
                                                        metrics=f"{','.join(metrics)}",
                                                        startDate=f"{self.start_date}"
                                                )
                                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                col_names.insert(1, f"{filters_1[i]}")
                                                if len(response["rows"]) != 0:
                                                    for row in response["rows"]:
                                                        row.insert(1, f"{subcontinent}")
                                                        data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]}.xlsx"), index=False)
                                        elif filters_2[j] == "group":
                                            data = []
                                            col_names = None
                                            for subcontinent in self.subcontinents:
                                                response = self.execute_api_request(
                                                    self.youtubeAnalytics.reports().query,
                                                        dimensions=f"{dimension},{filters_2[j]}",
                                                        endDate=f"{self.end_date}",
                                                        filters=f"{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.groups)}",
                                                        ids="channel==MINE",
                                                        metrics=f"{','.join(metrics)}",
                                                        startDate=f"{self.start_date}"
                                                )
                                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                col_names.insert(1, f"{filters_1[i]}")
                                                if len(response["rows"]) != 0:
                                                    for row in response["rows"]:
                                                        row.insert(1, f"{subcontinent}")
                                                        data.append(row)
                                            response_df = pd.DataFrame(data=data, columns=col_names)
                                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"), index=False)
                                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]}.xlsx"), index=False)
                                elif i != 0 and j == 0 and k != 0 and l != 0 and m != 0:
                                    if filters_1[i] == "continent":
                                        data = []
                                        col_names = None
                                        for continent in self.continents:
                                            for live_type in live_status:
                                                for sub_type in sub_status:
                                                    for yt_type in yt_status:
                                                        response = self.execute_api_request(
                                                            self.youtubeAnalytics.reports().query,
                                                                dimensions=f"{dimension},{filters_3[k]},{filters_4[l]},{filters_5[m]}",
                                                                endDate=f"{self.end_date}",
                                                                filters=f"{filters_1[i]}=={continent};{filters_3[k]}=={live_type};{filters_4[l]}=={sub_type};{filters_5[m]}=={yt_type}",
                                                                ids="channel==MINE",
                                                                metrics=f"{','.join(metrics)}",
                                                                startDate=f"{self.start_date}"
                                                        )
                                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                        col_names.insert(1, f"{filters_1[i]}")
                                                        if len(response["rows"]) != 0:
                                                            for row in response["rows"]:
                                                                row.insert(1, f"{continent}")
                                                                data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.xlsx"), index=False)
                                    elif filters_1[i] == "subContinent":
                                        data = []
                                        col_names = None
                                        for subcontinent in self.subcontinents:
                                            for live_type in live_status:
                                                for sub_type in sub_status:
                                                    for yt_type in yt_status:
                                                        response = self.execute_api_request(
                                                            self.youtubeAnalytics.reports().query,
                                                                dimensions=f"{dimension},{filters_3[k]},{filters_4[l]},{filters_5[m]}",
                                                                endDate=f"{self.end_date}",
                                                                filters=f"{filters_1[i]}=={subcontinent};{filters_3[k]}=={live_type};{filters_4[l]}=={sub_type};{filters_5[m]}=={yt_type}",
                                                                ids="channel==MINE",
                                                                metrics=f"{','.join(metrics)}",
                                                                startDate=f"{self.start_date}"
                                                        )
                                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                        col_names.insert(1, f"{filters_1[i]}")
                                                        if len(response["rows"]) != 0:
                                                            for row in response["rows"]:
                                                                row.insert(1, f"{subcontinent}")
                                                                data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.xlsx"), index=False)
                                elif i != 0 and j == 0 and k != 0 and l != 0 and m == 0:
                                    if filters_1[i] == "continent":
                                        data = []
                                        col_names = None
                                        for continent in self.continents:
                                            for live_type in live_status:
                                                for sub_type in sub_status:
                                                    response = self.execute_api_request(
                                                        self.youtubeAnalytics.reports().query,
                                                            dimensions=f"{dimension},{filters_3[k]},{filters_4[l]}",
                                                            endDate=f"{self.end_date}",
                                                            filters=f"{filters_1[i]}=={continent};{filters_3[k]}=={live_type};{filters_4[l]}=={sub_type}",
                                                            ids="channel==MINE",
                                                            metrics=f"{','.join(metrics)}",
                                                            startDate=f"{self.start_date}"
                                                    )
                                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                    col_names.insert(1, f"{filters_1[i]}")
                                                    if len(response["rows"]) != 0:
                                                        for row in response["rows"]:
                                                            row.insert(1, f"{continent}")
                                                            data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.xlsx"), index=False)
                                    elif filters_1[i] == "subContinent":
                                        data = []
                                        col_names = None
                                        for subcontinent in self.subcontinents:
                                            for live_type in live_status:
                                                for sub_type in sub_status:
                                                    response = self.execute_api_request(
                                                        self.youtubeAnalytics.reports().query,
                                                            dimensions=f"{dimension},{filters_3[k]},{filters_4[l]}",
                                                            endDate=f"{self.end_date}",
                                                            filters=f"{filters_1[i]}=={subcontinent};{filters_3[k]}=={live_type};{filters_4[l]}=={sub_type}",
                                                            ids="channel==MINE",
                                                            metrics=f"{','.join(metrics)}",
                                                            startDate=f"{self.start_date}"
                                                    )
                                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                    col_names.insert(1, f"{filters_1[i]}")
                                                    if len(response["rows"]) != 0:
                                                        for row in response["rows"]:
                                                            row.insert(1, f"{subcontinent}")
                                                            data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.xlsx"), index=False)
                                elif i != 0 and j == 0 and k != 0 and l == 0 and m != 0:
                                    if filters_1[i] == "continent":
                                        data = []
                                        col_names = None
                                        for continent in self.continents:
                                            for live_type in live_status:
                                                for yt_type in yt_status:
                                                    response = self.execute_api_request(
                                                        self.youtubeAnalytics.reports().query,
                                                            dimensions=f"{dimension},{filters_3[k]},{filters_5[m]}",
                                                            endDate=f"{self.end_date}",
                                                            filters=f"{filters_1[i]}=={continent};{filters_3[k]}=={live_type};{filters_5[m]}=={yt_type}",
                                                            ids="channel==MINE",
                                                            metrics=f"{','.join(metrics)}",
                                                            startDate=f"{self.start_date}"
                                                    )
                                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                    col_names.insert(1, f"{filters_1[i]}")
                                                    if len(response["rows"]) != 0:
                                                        for row in response["rows"]:
                                                            row.insert(1, f"{continent}")
                                                            data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_5[m]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_5[m]}.xlsx"), index=False)
                                    elif filters_1[i] == "subContinent":
                                        data = []
                                        col_names = None
                                        for subcontinent in self.subcontinents:
                                            for live_type in live_status:
                                                for yt_type in yt_status:
                                                    response = self.execute_api_request(
                                                        self.youtubeAnalytics.reports().query,
                                                            dimensions=f"{dimension},{filters_3[k]},{filters_5[m]}",
                                                            endDate=f"{self.end_date}",
                                                            filters=f"{filters_1[i]}=={subcontinent};{filters_3[k]}=={live_type};{filters_5[m]}=={yt_type}",
                                                            ids="channel==MINE",
                                                            metrics=f"{','.join(metrics)}",
                                                            startDate=f"{self.start_date}"
                                                    )
                                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                    col_names.insert(1, f"{filters_1[i]}")
                                                    if len(response["rows"]) != 0:
                                                        for row in response["rows"]:
                                                            row.insert(1, f"{subcontinent}")
                                                            data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_5[m]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_5[m]}.xlsx"), index=False)
                                elif i != 0 and j == 0 and k != 0 and l == 0 and m == 0:
                                    if filters_1[i] == "continent":
                                        data = []
                                        col_names = None
                                        for continent in self.continents:
                                            for live_type in live_status:
                                                response = self.execute_api_request(
                                                    self.youtubeAnalytics.reports().query,
                                                        dimensions=f"{dimension},{filters_3[k]}",
                                                        endDate=f"{self.end_date}",
                                                        filters=f"{filters_1[i]}=={continent};{filters_3[k]}=={live_type}",
                                                        ids="channel==MINE",
                                                        metrics=f"{','.join(metrics)}",
                                                        startDate=f"{self.start_date}"
                                                )
                                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                col_names.insert(1, f"{filters_1[i]}")
                                                if len(response["rows"]) != 0:
                                                    for row in response["rows"]:
                                                        row.insert(1, f"{continent}")
                                                        data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_3[k]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_3[k]}.xlsx"), index=False)
                                    elif filters_1[i] == "subContinent":
                                        data = []
                                        col_names = None
                                        for subcontinent in self.subcontinents:
                                            for live_type in live_status:
                                                response = self.execute_api_request(
                                                    self.youtubeAnalytics.reports().query,
                                                        dimensions=f"{dimension},{filters_3[k]}",
                                                        endDate=f"{self.end_date}",
                                                        filters=f"{filters_1[i]}=={subcontinent};{filters_3[k]}=={live_type}",
                                                        ids="channel==MINE",
                                                        metrics=f"{','.join(metrics)}",
                                                        startDate=f"{self.start_date}"
                                                )
                                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                col_names.insert(1, f"{filters_1[i]}")
                                                if len(response["rows"]) != 0:
                                                    for row in response["rows"]:
                                                        row.insert(1, f"{subcontinent}")
                                                        data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_3[k]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_3[k]}.xlsx"), index=False)
                                elif i != 0 and j == 0 and k == 0 and l != 0 and m != 0:
                                    if filters_1[i] == "continent":
                                        data = []
                                        col_names = None
                                        for continent in self.continents:
                                            for sub_type in sub_status:
                                                for yt_type in yt_status:
                                                    response = self.execute_api_request(
                                                        self.youtubeAnalytics.reports().query,
                                                            dimensions=f"{dimension},{filters_4[l]},{filters_5[m]}",
                                                            endDate=f"{self.end_date}",
                                                            filters=f"{filters_1[i]}=={continent};{filters_4[l]}=={sub_type};{filters_5[m]}=={yt_type}",
                                                            ids="channel==MINE",
                                                            metrics=f"{','.join(metrics)}",
                                                            startDate=f"{self.start_date}"
                                                    )
                                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                    col_names.insert(1, f"{filters_1[i]}")
                                                    if len(response["rows"]) != 0:
                                                        for row in response["rows"]:
                                                            row.insert(1, f"{continent}")
                                                            data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_4[l]},{filters_5[m]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_4[l]},{filters_5[m]}.xlsx"), index=False)
                                    elif filters_1[i] == "subContinent":
                                        data = []
                                        col_names = None
                                        for subcontinent in self.subcontinents:
                                            for sub_type in sub_status:
                                                for yt_type in yt_status:
                                                    response = self.execute_api_request(
                                                        self.youtubeAnalytics.reports().query,
                                                            dimensions=f"{dimension},{filters_4[l]},{filters_5[m]}",
                                                            endDate=f"{self.end_date}",
                                                            filters=f"{filters_1[i]}=={subcontinent};{filters_4[l]}=={sub_type};{filters_5[m]}=={yt_type}",
                                                            ids="channel==MINE",
                                                            metrics=f"{','.join(metrics)}",
                                                            startDate=f"{self.start_date}"
                                                    )
                                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                    col_names.insert(1, f"{filters_1[i]}")
                                                    if len(response["rows"]) != 0:
                                                        for row in response["rows"]:
                                                            row.insert(1, f"{subcontinent}")
                                                            data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_4[l]},{filters_5[m]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_4[l]},{filters_5[m]}.xlsx"), index=False)
                                elif i != 0 and j == 0 and k == 0 and l != 0 and m == 0:
                                    if filters_1[i] == "continent":
                                        data = []
                                        col_names = None
                                        for continent in self.continents:
                                            for sub_type in sub_status:
                                                response = self.execute_api_request(
                                                    self.youtubeAnalytics.reports().query,
                                                        dimensions=f"{dimension},{filters_4[l]}",
                                                        endDate=f"{self.end_date}",
                                                        filters=f"{filters_1[i]}=={continent};{filters_4[l]}=={sub_type}",
                                                        ids="channel==MINE",
                                                        metrics=f"{','.join(metrics)}",
                                                        startDate=f"{self.start_date}"
                                                )
                                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                col_names.insert(1, f"{filters_1[i]}")
                                                if len(response["rows"]) != 0:
                                                    for row in response["rows"]:
                                                        row.insert(1, f"{continent}")
                                                        data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_4[l]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_4[l]}.xlsx"), index=False)
                                    elif filters_1[i] == "subContinent":
                                        data = []
                                        col_names = None
                                        for subcontinent in self.subcontinents:
                                            for sub_type in sub_status:
                                                response = self.execute_api_request(
                                                    self.youtubeAnalytics.reports().query,
                                                        dimensions=f"{dimension},{filters_4[l]}",
                                                        endDate=f"{self.end_date}",
                                                        filters=f"{filters_1[i]}=={subcontinent};{filters_4[l]}=={sub_type}",
                                                        ids="channel==MINE",
                                                        metrics=f"{','.join(metrics)}",
                                                        startDate=f"{self.start_date}"
                                                )
                                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                col_names.insert(1, f"{filters_1[i]}")
                                                if len(response["rows"]) != 0:
                                                    for row in response["rows"]:
                                                        row.insert(1, f"{subcontinent}")
                                                        data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_4[l]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_4[l]}.xlsx"), index=False)
                                elif i != 0 and j == 0 and k == 0 and l == 0 and m != 0:
                                    if filters_1[i] == "continent":
                                        data = []
                                        col_names = None
                                        for continent in self.continents:
                                            for yt_type in yt_status:
                                                response = self.execute_api_request(
                                                    self.youtubeAnalytics.reports().query,
                                                        dimensions=f"{dimension},{filters_5[m]}",
                                                        endDate=f"{self.end_date}",
                                                        filters=f"{filters_1[i]}=={continent};{filters_5[m]}=={yt_type}",
                                                        ids="channel==MINE",
                                                        metrics=f"{','.join(metrics)}",
                                                        startDate=f"{self.start_date}"
                                                )
                                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                col_names.insert(1, f"{filters_1[i]}")
                                                if len(response["rows"]) != 0:
                                                    for row in response["rows"]:
                                                        row.insert(1, f"{continent}")
                                                        data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_5[m]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_5[m]}.xlsx"), index=False)
                                    elif filters_1[i] == "subContinent":
                                        data = []
                                        col_names = None
                                        for subcontinent in self.subcontinents:
                                            for yt_type in yt_status:
                                                response = self.execute_api_request(
                                                    self.youtubeAnalytics.reports().query,
                                                        dimensions=f"{dimension},{filters_5[m]}",
                                                        endDate=f"{self.end_date}",
                                                        filters=f"{filters_1[i]}=={subcontinent};{filters_5[m]}=={yt_type}",
                                                        ids="channel==MINE",
                                                        metrics=f"{','.join(metrics)}",
                                                        startDate=f"{self.start_date}"
                                                )
                                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                col_names.insert(1, f"{filters_1[i]}")
                                                if len(response["rows"]) != 0:
                                                    for row in response["rows"]:
                                                        row.insert(1, f"{subcontinent}")
                                                        data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_5[m]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_5[m]}.xlsx"), index=False)
                                elif i != 0 and j == 0 and k == 0 and l == 0 and m == 0:
                                    if filters_1[i] == "continent":
                                        data = []
                                        col_names = None
                                        for continent in self.continents:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{filters_1[i]}=={continent}",
                                                    ids="channel==MINE",
                                                    metrics=f"{','.join(metrics)}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            col_names.insert(1, f"{filters_1[i]}")
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    row.insert(1, f"{continent}")
                                                    data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]}.xlsx"), index=False)
                                    elif filters_1[i] == "subContinent":
                                        data = []
                                        col_names = None
                                        for subcontinent in self.subcontinents:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{filters_1[i]}=={subcontinent}",
                                                    ids="channel==MINE",
                                                    metrics=f"{','.join(metrics)}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            col_names.insert(1, f"{filters_1[i]}")
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    row.insert(1, f"{subcontinent}")
                                                    data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_1[i]}.xlsx"), index=False)
                                elif i == 0 and j != 0 and k != 0 and l != 0 and m != 0:
                                    if filters_2[j] == "video":
                                        data = []
                                        col_names = None
                                        for live_type in live_status:
                                            for sub_type in sub_status:
                                                for yt_type in yt_status:
                                                    response = self.execute_api_request(
                                                        self.youtubeAnalytics.reports().query,
                                                            dimensions=f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}",
                                                            endDate=f"{self.end_date}",
                                                            filters=f"{filters_2[j]}=={','.join(self.vid_ids)};{filters_3[k]}=={live_type};{filters_4[l]}=={sub_type};{filters_5[m]}=={yt_type}",
                                                            ids="channel==MINE",
                                                            metrics=f"{','.join(metrics)}",
                                                            startDate=f"{self.start_date}"
                                                    )
                                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                    if len(response["rows"]) != 0:
                                                        for row in response["rows"]:
                                                            data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.xlsx"), index=False)
                                    elif filters_2[j] == "group":
                                        data = []
                                        col_names = None
                                        for live_type in live_status:
                                            for sub_type in sub_status:
                                                for yt_type in yt_status:
                                                    response = self.execute_api_request(
                                                        self.youtubeAnalytics.reports().query,
                                                            dimensions=f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}",
                                                            endDate=f"{self.end_date}",
                                                            filters=f"{filters_2[j]}=={','.join(self.groups)};{filters_3[k]}=={live_type};{filters_4[l]}=={sub_type};{filters_5[m]}=={yt_type}",
                                                            ids="channel==MINE",
                                                            metrics=f"{','.join(metrics)}",
                                                            startDate=f"{self.start_date}"
                                                    )
                                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                    if len(response["rows"]) != 0:
                                                        for row in response["rows"]:
                                                            data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.xlsx"), index=False)
                                elif i == 0 and j != 0 and k != 0 and l != 0 and m == 0:
                                    if filters_2[j] == "video":
                                        data = []
                                        col_names = None
                                        for live_type in live_status:
                                            for sub_type in sub_status:
                                                response = self.execute_api_request(
                                                    self.youtubeAnalytics.reports().query,
                                                        dimensions=f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}",
                                                        endDate=f"{self.end_date}",
                                                        filters=f"{filters_2[j]}=={','.join(self.vid_ids)};{filters_3[k]}=={live_type};{filters_4[l]}=={sub_type}",
                                                        ids="channel==MINE",
                                                        metrics=f"{','.join(metrics)}",
                                                        startDate=f"{self.start_date}"
                                                )
                                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                if len(response["rows"]) != 0:
                                                    for row in response["rows"]:
                                                        data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.xlsx"), index=False)
                                    elif filters_2[j] == "group":
                                        data = []
                                        col_names = None
                                        for live_type in live_status:
                                            for sub_type in sub_status:
                                                response = self.execute_api_request(
                                                    self.youtubeAnalytics.reports().query,
                                                        dimensions=f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}",
                                                        endDate=f"{self.end_date}",
                                                        filters=f"{filters_2[j]}=={','.join(self.groups)};{filters_3[k]}=={live_type};{filters_4[l]}=={sub_type}",
                                                        ids="channel==MINE",
                                                        metrics=f"{','.join(metrics)}",
                                                        startDate=f"{self.start_date}"
                                                )
                                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                if len(response["rows"]) != 0:
                                                    for row in response["rows"]:
                                                        data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.xlsx"), index=False)
                                elif i == 0 and j != 0 and k != 0 and l == 0 and m != 0:
                                    if filters_2[j] == "video":
                                        data = []
                                        col_names = None
                                        for live_type in live_status:
                                            for yt_type in yt_status:
                                                response = self.execute_api_request(
                                                    self.youtubeAnalytics.reports().query,
                                                        dimensions=f"{dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]}",
                                                        endDate=f"{self.end_date}",
                                                        filters=f"{filters_2[j]}=={','.join(self.vid_ids)};{filters_3[k]}=={live_type};{filters_5[m]}=={yt_type}",
                                                        ids="channel==MINE",
                                                        metrics=f"{','.join(metrics)}",
                                                        startDate=f"{self.start_date}"
                                                )
                                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                if len(response["rows"]) != 0:
                                                    for row in response["rows"]:
                                                        data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]}.xlsx"), index=False)
                                    elif filters_2[j] == "group":
                                        data = []
                                        col_names = None
                                        for live_type in live_status:
                                            for yt_type in yt_status:
                                                response = self.execute_api_request(
                                                    self.youtubeAnalytics.reports().query,
                                                        dimensions=f"{dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]}",
                                                        endDate=f"{self.end_date}",
                                                        filters=f"{filters_2[j]}=={','.join(self.groups)};{filters_3[k]}=={live_type};{filters_5[m]}=={yt_type}",
                                                        ids="channel==MINE",
                                                        metrics=f"{','.join(metrics)}",
                                                        startDate=f"{self.start_date}"
                                                )
                                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                if len(response["rows"]) != 0:
                                                    for row in response["rows"]:
                                                        data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]}.xlsx"), index=False)
                                elif i == 0 and j != 0 and k != 0 and l == 0 and m == 0:
                                    if filters_2[j] == "video":
                                        data = []
                                        col_names = None
                                        for live_type in live_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_2[j]},{filters_3[k]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{filters_2[j]}=={','.join(self.vid_ids)};{filters_3[k]}=={live_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{','.join(metrics)}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                                    elif filters_2[j] == "group":
                                        data = []
                                        col_names = None
                                        for live_type in live_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_2[j]},{filters_3[k]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{filters_2[j]}=={','.join(self.groups)};{filters_3[k]}=={live_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{','.join(metrics)}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                                elif i == 0 and j != 0 and k == 0 and l != 0 and m != 0:
                                    if filters_2[j] == "video":
                                        data = []
                                        col_names = None
                                        for sub_type in sub_status:
                                            for yt_type in yt_status:
                                                response = self.execute_api_request(
                                                    self.youtubeAnalytics.reports().query,
                                                        dimensions=f"{dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]}",
                                                        endDate=f"{self.end_date}",
                                                        filters=f"{filters_2[j]}=={','.join(self.vid_ids)};{filters_4[l]}=={sub_type};{filters_5[m]}=={yt_type}",
                                                        ids="channel==MINE",
                                                        metrics=f"{','.join(metrics)}",
                                                        startDate=f"{self.start_date}"
                                                )
                                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                if len(response["rows"]) != 0:
                                                    for row in response["rows"]:
                                                        data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]}.xlsx"), index=False)
                                    elif filters_2[j] == "group":
                                        data = []
                                        col_names = None
                                        for sub_type in sub_status:
                                            for yt_type in yt_status:
                                                response = self.execute_api_request(
                                                    self.youtubeAnalytics.reports().query,
                                                        dimensions=f"{dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]}",
                                                        endDate=f"{self.end_date}",
                                                        filters=f"{filters_2[j]}=={','.join(self.groups)};{filters_4[l]}=={sub_type};{filters_5[m]}=={yt_type}",
                                                        ids="channel==MINE",
                                                        metrics=f"{','.join(metrics)}",
                                                        startDate=f"{self.start_date}"
                                                )
                                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                if len(response["rows"]) != 0:
                                                    for row in response["rows"]:
                                                        data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]}.xlsx"), index=False)
                                elif i == 0 and j != 0 and k == 0 and l != 0 and m == 0:
                                    if filters_2[j] == "video":
                                        data = []
                                        col_names = None
                                        for sub_type in sub_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_2[j]},{filters_4[l]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{filters_2[j]}=={','.join(self.vid_ids)};{filters_4[l]}=={sub_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{','.join(metrics)}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_4[l]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_4[l]}.xlsx"), index=False)
                                    elif filters_2[j] == "group":
                                        data = []
                                        col_names = None
                                        for sub_type in sub_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_2[j]},{filters_4[l]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{filters_2[j]}=={','.join(self.groups)};{filters_4[l]}=={sub_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{','.join(metrics)}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_4[l]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_4[l]}.xlsx"), index=False)
                                elif i == 0 and j != 0 and k == 0 and l == 0 and m != 0:
                                    if filters_2[j] == "video":
                                        data = []
                                        col_names = None
                                        for yt_type in yt_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_2[j]},{filters_5[m]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{filters_2[j]}=={','.join(self.vid_ids)};{filters_5[m]}=={yt_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{','.join(metrics)}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_5[m]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_5[m]}.xlsx"), index=False)
                                    elif filters_2[j] == "group":
                                        data = []
                                        col_names = None
                                        for yt_type in yt_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_2[j]},{filters_5[m]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{filters_2[j]}=={','.join(self.groups)};{filters_5[m]}=={yt_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{','.join(metrics)}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_5[m]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_5[m]}.xlsx"), index=False)
                                elif i == 0 and j != 0 and k == 0 and l == 0 and m == 0:
                                    if filters_2[j] == "video":
                                        data = []
                                        col_names = None
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_2[j]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{filters_2[j]}=={','.join(self.vid_ids)}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_2[j]}.xlsx"), index=False)
                                    elif filters_2[j] == "group":
                                        data = []
                                        col_names = None
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_2[j]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{filters_2[j]}=={','.join(self.groups)}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                        response_df = pd.DataFrame(data=data, columns=col_names)
                                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]}.csv"), index=False)
                                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_2[j]}.xlsx"), index=False)
                                elif i == 0 and j == 0 and k != 0 and l != 0 and m != 0:
                                    data = []
                                    col_names = None
                                    for live_type in live_status:
                                        for sub_type in sub_status:
                                            for yt_type in yt_status:
                                                response = self.execute_api_request(
                                                    self.youtubeAnalytics.reports().query,
                                                        dimensions=f"{dimension},{filters_3[k]},{filters_4[l]},{filters_5[m]}",
                                                        endDate=f"{self.end_date}",
                                                        filters=f"{filters_3[k]}=={live_type};{filters_4[l]}=={sub_type};{filters_5[m]}=={yt_type}",
                                                        ids="channel==MINE",
                                                        metrics=f"{','.join(metrics)}",
                                                        startDate=f"{self.start_date}"
                                                )
                                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                                if len(response["rows"]) != 0:
                                                    for row in response["rows"]:
                                                        data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_3[k]},{filters_4[l]},{filters_5[m]}.xlsx"), index=False)
                                elif i == 0 and j == 0 and k != 0 and l != 0 and m == 0:
                                    data = []
                                    col_names = None
                                    for live_type in live_status:
                                        for sub_type in sub_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_3[k]},{filters_4[l]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{filters_3[k]}=={live_type};{filters_4[l]}=={sub_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{','.join(metrics)}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_3[k]},{filters_4[l]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_3[k]},{filters_4[l]}.xlsx"), index=False)
                                elif i == 0 and j == 0 and k != 0 and l == 0 and m != 0:
                                    data = []
                                    col_names = None
                                    for live_type in live_status:
                                        for yt_type in yt_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_3[k]},{filters_5[m]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{filters_3[k]}=={live_type};{filters_5[m]}=={yt_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{','.join(metrics)}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_3[k]},{filters_5[m]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_3[k]},{filters_5[m]}.xlsx"), index=False)
                                elif i == 0 and j == 0 and k != 0 and l == 0 and m == 0:
                                    data = []
                                    col_names = None
                                    for live_type in live_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_3[k]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{filters_3[k]}=={live_type}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_3[k]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_3[k]}.xlsx"), index=False)
                                elif i == 0 and j == 0 and k == 0 and l != 0 and m != 0:
                                    data = []
                                    col_names = None
                                    for sub_type in sub_status:
                                        for yt_type in yt_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_4[l]},{filters_5[m]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{filters_4[l]}=={sub_type};{filters_5[m]}=={yt_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{','.join(metrics)}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_4[l]},{filters_5[m]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_4[l]},{filters_5[m]}.xlsx"), index=False)
                                elif i == 0 and j == 0 and k == 0 and l != 0 and m == 0:
                                    data = []
                                    col_names = None
                                    for sub_type in sub_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_4[l]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{filters_4[l]}=={sub_type}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_4[l]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_4[l]}.xlsx"), index=False)
                                elif i == 0 and j == 0 and k == 0 and l == 0 and m != 0:
                                    data = []
                                    col_names = None
                                    for yt_type in yt_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_5[m]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{filters_5[m]}=={yt_type}",
                                                ids="channel==MINE",
                                                metrics=f"{','.join(metrics)}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_5[m]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension},{filters_5[m]}.xlsx"), index=False)
                                elif i == 0 and j == 0 and k == 0 and l == 0 and m == 0:
                                    data = []
                                    col_names = None
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{dimension}",
                                            endDate=f"{self.end_date}",
                                            ids="channel==MINE",
                                            metrics=f"{','.join(metrics)}",
                                            startDate=f"{self.start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{date.today()}", "raw", "video_reports", "excel", "playback_details_by_country", f"{dimension}.xlsx"), index=False)
        return
