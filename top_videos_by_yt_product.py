import os
import pandas as pd
from auth import Auth

class Top_Videos_By_YT_Product(Auth):

    def top_videos_by_yt_product(self):
        metrics = ["views", "redViews", "estimatedMinutesWatched", "estimatedRedMinutesWatched", "averageViewDuration", "averageViewPercentage"]
        sorting_options = ["-views"]
        required_dimension = "video" 
        sub_status = ["SUBSCRIBED", "UNSUBSCRIBED"]
        yt_status = ["CORE", "GAMING", "KIDS", "MUSIC"]

        filters_to_remove = []
        if not self.has_countries:
            filters_to_remove.append("country")
        if not self.has_provinces:
            filters_to_remove.append("province")
        if not self.has_continents:
            filters_to_remove.append("continent")
        if not self.has_subcontinents:
            filters_to_remove.append("subContinent")

        filters_1 = ["", "country", "province", "continent", "subContinent"]
        filters_2 = ["", "subscribedStatus"]
        filters_3 = ["", "youtubeProduct"]

        for fil_to_rem in filters_to_remove:
            filters_1.remove(fil_to_rem)

        for sort_by in sorting_options:
            for i in range(len(filters_1)):
                for j in range(len(filters_2)):
                    for k in range(len(filters_3)):
                        if i != 0 and j != 0 and k != 0:
                            if filters_1[i] == "country":
                                data = []
                                col_names = None
                                for country in self.countries:
                                    for sub_type in sub_status:
                                        for yt_type in yt_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{filters_1[i]}=={country};{filters_2[j]}=={sub_type};{filters_3[k]}=={yt_type}",
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
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{sort_by}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{sort_by}.xlsx"), index=False)
                            elif filters_1[i] == "province":
                                data = []
                                col_names = None
                                for province in self.provinces:
                                    for sub_type in sub_status:
                                        for yt_type in yt_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{filters_1[i]}=={province};{filters_2[j]}=={sub_type};{filters_3[k]}=={yt_type}",
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
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{sort_by}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{sort_by}.xlsx"), index=False)
                            elif filters_1[i] == "continent":
                                data = []
                                col_names = None
                                for continent in self.continents:
                                    for sub_type in sub_status:
                                        for yt_type in yt_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{required_dimension},{filters_2[j]},{filters_3[k]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{filters_1[i]}=={continent};{filters_2[j]}=={sub_type};{filters_3[k]}=={yt_type}",
                                                    ids="channel==MINE",
                                                    maxResults=200,
                                                    metrics=f"{','.join(metrics)}",
                                                    sort=f"{sort_by}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            col_names.insert(1, f"{filters_1[i]}")
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    row.insert(1, f"{continent}")
                                                    data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{sort_by}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{sort_by}.xlsx"), index=False)
                            elif filters_1[i] == "subContinent":
                                data = []
                                col_names = None
                                for subcontinent in self.subcontinents:
                                    for sub_type in sub_status:
                                        for yt_type in yt_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{required_dimension},{filters_2[j]},{filters_3[k]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{filters_1[i]}=={subcontinent};{filters_2[j]}=={sub_type};{filters_3[k]}=={yt_type}",
                                                    ids="channel==MINE",
                                                    maxResults=200,
                                                    metrics=f"{','.join(metrics)}",
                                                    sort=f"{sort_by}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            col_names.insert(1, f"{filters_1[i]}")
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    row.insert(1, f"{subcontinent}")
                                                    data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{sort_by}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{sort_by}.xlsx"), index=False)
                        elif i != 0 and j != 0 and k == 0:
                            if filters_1[i] == "country":
                                data = []
                                col_names = None
                                for country in self.countries:
                                    for sub_type in sub_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{required_dimension},{filters_1[i]},{filters_2[j]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{filters_1[i]}=={country};{filters_2[j]}=={sub_type}",
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
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_2[j]},{sort_by}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_2[j]},{sort_by}.xlsx"), index=False)
                            elif filters_1[i] == "province":
                                data = []
                                col_names = None
                                for province in self.provinces:
                                    for sub_type in sub_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{required_dimension},{filters_1[i]},{filters_2[j]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{filters_1[i]}=={province};{filters_2[j]}=={sub_type}",
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
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_2[j]},{sort_by}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_2[j]},{sort_by}.xlsx"), index=False)
                            elif filters_1[i] == "continent":
                                data = []
                                col_names = None
                                for continent in self.continents:
                                    for sub_type in sub_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{required_dimension},{filters_2[j]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{filters_1[i]}=={continent};{filters_2[j]}=={sub_type}",
                                                ids="channel==MINE",
                                                maxResults=200,
                                                metrics=f"{','.join(metrics)}",
                                                sort=f"{sort_by}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        col_names.insert(1, f"{filters_1[i]}")
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                row.insert(1, f"{continent}")
                                                data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_2[j]},{sort_by}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_2[j]},{sort_by}.xlsx"), index=False)
                            elif filters_1[i] == "subContinent":
                                data = []
                                col_names = None
                                for subcontinent in self.subcontinents:
                                    for sub_type in sub_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{required_dimension},{filters_2[j]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{filters_1[i]}=={subcontinent};{filters_2[j]}=={sub_type}",
                                                ids="channel==MINE",
                                                maxResults=200,
                                                metrics=f"{','.join(metrics)}",
                                                sort=f"{sort_by}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        col_names.insert(1, f"{filters_1[i]}")
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                row.insert(1, f"{subcontinent}")
                                                data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_2[j]},{sort_by}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_2[j]},{sort_by}.xlsx"), index=False)
                        elif i != 0 and j == 0 and k != 0:
                            if filters_1[i] == "country":
                                data = []
                                col_names = None
                                for country in self.countries:
                                    for yt_type in yt_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{required_dimension},{filters_1[i]},{filters_3[k]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{filters_1[i]}=={country};{filters_3[k]}=={yt_type}",
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
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_3[k]},{sort_by}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_3[k]},{sort_by}.xlsx"), index=False)
                            elif filters_1[i] == "province":
                                data = []
                                col_names = None
                                for province in self.provinces:
                                    for yt_type in yt_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{required_dimension},{filters_1[i]},{filters_3[k]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{filters_1[i]}=={province};{filters_3[k]}=={yt_type}",
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
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_3[k]},{sort_by}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_3[k]},{sort_by}.xlsx"), index=False)
                            elif filters_1[i] == "continent":
                                data = []
                                col_names = None
                                for continent in self.continents:
                                    for yt_type in yt_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{required_dimension},{filters_3[k]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{filters_1[i]}=={continent};{filters_3[k]}=={yt_type}",
                                                ids="channel==MINE",
                                                maxResults=200,
                                                metrics=f"{','.join(metrics)}",
                                                sort=f"{sort_by}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        col_names.insert(1, f"{filters_1[i]}")
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                row.insert(1, f"{continent}")
                                                data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_3[k]},{sort_by}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_3[k]},{sort_by}.xlsx"), index=False)
                            elif filters_1[i] == "subContinent":
                                data = []
                                col_names = None
                                for subcontinent in self.subcontinents:
                                    for yt_type in yt_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{required_dimension},{filters_3[k]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{filters_1[i]}=={subcontinent};{filters_3[k]}=={yt_type}",
                                                ids="channel==MINE",
                                                maxResults=200,
                                                metrics=f"{','.join(metrics)}",
                                                sort=f"{sort_by}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        col_names.insert(1, f"{filters_1[i]}")
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                row.insert(1, f"{subcontinent}")
                                                data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_3[k]},{sort_by}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_3[k]},{sort_by}.xlsx"), index=False)
                        elif i != 0 and j == 0 and k == 0:
                            if filters_1[i] == "country":
                                data = []
                                col_names = None
                                for country in self.countries:
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{required_dimension},{filters_1[i]}",
                                            endDate=f"{self.end_date}",
                                            filters=f"{filters_1[i]}=={country}",
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
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{sort_by}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{sort_by}.xlsx"), index=False)
                            elif filters_1[i] == "province":
                                data = []
                                col_names = None
                                for province in self.provinces:
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{required_dimension},{filters_1[i]}",
                                            endDate=f"{self.end_date}",
                                            filters=f"{filters_1[i]}=={province}",
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
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{sort_by}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{sort_by}.xlsx"), index=False)
                            elif filters_1[i] == "continent":
                                data = []
                                col_names = None
                                for continent in self.continents:
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{required_dimension}",
                                            endDate=f"{self.end_date}",
                                            filters=f"{filters_1[i]}=={continent}",
                                            ids="channel==MINE",
                                            maxResults=200,
                                            metrics=f"{','.join(metrics)}",
                                            sort=f"{sort_by}",
                                            startDate=f"{self.start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    col_names.insert(1, f"{filters_1[i]}")
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            row.insert(1, f"{continent}")
                                            data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{sort_by}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{sort_by}.xlsx"), index=False)
                            elif filters_1[i] == "subContinent":
                                data = []
                                col_names = None
                                for subcontinent in self.subcontinents:
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{required_dimension}",
                                            endDate=f"{self.end_date}",
                                            filters=f"{filters_1[i]}=={subcontinent}",
                                            ids="channel==MINE",
                                            maxResults=200,
                                            metrics=f"{','.join(metrics)}",
                                            sort=f"{sort_by}",
                                            startDate=f"{self.start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    col_names.insert(1, f"{filters_1[i]}")
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            row.insert(1, f"{subcontinent}")
                                            data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{sort_by}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{sort_by}.xlsx"), index=False)
                        elif i == 0 and j != 0 and k != 0:
                            data = []
                            col_names = None
                            for sub_type in sub_status:
                                for yt_type in yt_status:
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{required_dimension},{filters_2[j]},{filters_3[k]}",
                                            endDate=f"{self.end_date}",
                                            filters=f"{filters_2[j]}=={sub_type};{filters_3[k]}=={yt_type}",
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
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_2[j]},{filters_3[k]},{sort_by}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_by_yt_product", f"{required_dimension},{filters_2[j]},{filters_3[k]},{sort_by}.xlsx"), index=False)
                        elif i == 0 and j != 0 and k == 0:
                            data = []
                            col_names = None
                            for sub_type in sub_status:
                                response = self.execute_api_request(
                                    self.youtubeAnalytics.reports().query,
                                        dimensions=f"{required_dimension},{filters_2[j]}",
                                        endDate=f"{self.end_date}",
                                        filters=f"{filters_2[j]}=={sub_type}",
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
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_2[j]},{sort_by}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_by_yt_product", f"{required_dimension},{filters_2[j]},{sort_by}.xlsx"), index=False)
                        elif i == 0 and j == 0 and k != 0:
                            data = []
                            col_names = None
                            for yt_type in yt_status:
                                response = self.execute_api_request(
                                    self.youtubeAnalytics.reports().query,
                                        dimensions=f"{required_dimension},{filters_3[k]}",
                                        endDate=f"{self.end_date}",
                                        filters=f"{filters_3[k]}=={yt_type}",
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
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_3[k]},{sort_by}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_by_yt_product", f"{required_dimension},{filters_3[k]},{sort_by}.xlsx"), index=False)
                        elif i == 0 and j == 0 and k == 0:
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
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{sort_by}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "video_reports", "excel", "top_videos_by_yt_product", f"{required_dimension},{sort_by}.xlsx"), index=False)
        return
