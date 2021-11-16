import os
import pandas as pd
from auth import Auth

class Activity_In_US_Playlist(Auth):

    def activity_in_US_playlist(self):
        metrics = ["views", "redViews", "estimatedMinutesWatched", "estimatedRedMinutesWatched", "averageViewDuration", "playlistStarts", "viewsPerPlaylistStart", "averageTimeInPlaylist"]
        required_dimension = "province"
        required_filter = "isCurated==1;country==US"
        sub_status = ["SUBSCRIBED", "UNSUBSCRIBED"]
        yt_status = ["CORE", "GAMING", "KIDS", "MUSIC"]

        filters_to_remove = []
        if not self.has_groups:
            filters_to_remove.append("group")

        filters_1 = ["", "playlist", "group"]
        filters_2 = ["", "subscribedStatus"]
        filters_3 = ["", "youtubeProduct"]

        for fil_to_rem in filters_to_remove:
            filters_1.remove(fil_to_rem)

        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    if i != 0 and j != 0 and k != 0:
                        if filters_1[i] == "playlist":
                            data = []
                            col_names = None
                            for sub_type in sub_status:
                                for yt_type in yt_status:
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}",
                                            endDate=f"{self.end_date}",
                                            filters=f"{required_filter};{filters_1[i]}=={','.join(self.playlist_ids)};{filters_2[j]}=={sub_type};{filters_3[k]}=={yt_type}",
                                            ids="channel==MINE",
                                            metrics=f"{','.join(metrics)}",
                                            startDate=f"{self.start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            data.append(row)
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "activity_in_US_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "activity_in_US_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                        elif filters_1[i] == "group":
                            data = []
                            col_names = None
                            for sub_type in sub_status:
                                for yt_type in yt_status:
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}",
                                            endDate=f"{self.end_date}",
                                            filters=f"{required_filter};{filters_1[i]}=={','.join(self.groups)};{filters_2[j]}=={sub_type};{filters_3[k]}=={yt_type}",
                                            ids="channel==MINE",
                                            metrics=f"{','.join(metrics)}",
                                            startDate=f"{self.start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            data.append(row)
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "activity_in_US_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "activity_in_US_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                    elif i != 0 and j != 0 and k == 0:
                        if filters_1[i] == "playlist":
                            data = []
                            col_names = None
                            for sub_type in sub_status:
                                response = self.execute_api_request(
                                    self.youtubeAnalytics.reports().query,
                                        dimensions=f"{required_dimension},{filters_1[i]},{filters_2[j]}",
                                        endDate=f"{self.end_date}",
                                        filters=f"{required_filter};{filters_1[i]}=={','.join(self.playlist_ids)};{filters_2[j]}=={sub_type}",
                                        ids="channel==MINE",
                                        metrics=f"{','.join(metrics)}",
                                        startDate=f"{self.start_date}"
                                )
                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                if len(response["rows"]) != 0:
                                    for row in response["rows"]:
                                        data.append(row)
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "activity_in_US_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "activity_in_US_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]}.xlsx"), index=False)
                        elif filters_1[i] == "group":
                            data = []
                            col_names = None
                            for sub_type in sub_status:
                                response = self.execute_api_request(
                                    self.youtubeAnalytics.reports().query,
                                        dimensions=f"{required_dimension},{filters_1[i]},{filters_2[j]}",
                                        endDate=f"{self.end_date}",
                                        filters=f"{required_filter};{filters_1[i]}=={','.join(self.groups)};{filters_2[j]}=={sub_type}",
                                        ids="channel==MINE",
                                        metrics=f"{','.join(metrics)}",
                                        startDate=f"{self.start_date}"
                                )
                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                if len(response["rows"]) != 0:
                                    for row in response["rows"]:
                                        data.append(row)
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "activity_in_US_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "activity_in_US_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]}.xlsx"), index=False)
                    elif i != 0 and j == 0 and k != 0:
                        if filters_1[i] == "playlist":
                            data = []
                            col_names = None
                            for yt_type in yt_status:
                                response = self.execute_api_request(
                                    self.youtubeAnalytics.reports().query,
                                        dimensions=f"{required_dimension},{filters_1[i]},{filters_3[k]}",
                                        endDate=f"{self.end_date}",
                                        filters=f"{required_filter};{filters_1[i]}=={','.join(self.playlist_ids)};{filters_3[k]}=={yt_type}",
                                        ids="channel==MINE",
                                        metrics=f"{','.join(metrics)}",
                                        startDate=f"{self.start_date}"
                                )
                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                if len(response["rows"]) != 0:
                                    for row in response["rows"]:
                                        data.append(row)
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "activity_in_US_playlist", f"{required_dimension},{filters_1[i]},{filters_3[k]}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "activity_in_US_playlist", f"{required_dimension},{filters_1[i]},{filters_3[k]}.xlsx"), index=False)
                        elif filters_1[i] == "group":
                            data = []
                            col_names = None
                            for yt_type in yt_status:
                                response = self.execute_api_request(
                                    self.youtubeAnalytics.reports().query,
                                        dimensions=f"{required_dimension},{filters_1[i]},{filters_3[k]}",
                                        endDate=f"{self.end_date}",
                                        filters=f"{required_filter};{filters_1[i]}=={','.join(self.groups)};{filters_3[k]}=={yt_type}",
                                        ids="channel==MINE",
                                        metrics=f"{','.join(metrics)}",
                                        startDate=f"{self.start_date}"
                                )
                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                if len(response["rows"]) != 0:
                                    for row in response["rows"]:
                                        data.append(row)
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "activity_in_US_playlist", f"{required_dimension},{filters_1[i]},{filters_3[k]}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "activity_in_US_playlist", f"{required_dimension},{filters_1[i]},{filters_3[k]}.xlsx"), index=False)
                    elif i != 0 and j == 0 and k == 0:
                        if filters_1[i] == "playlist":
                            data = []
                            col_names = None
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    dimensions=f"{required_dimension},{filters_1[i]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{required_filter};{filters_1[i]}=={','.join(self.playlist_ids)}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            if len(response["rows"]) != 0:
                                for row in response["rows"]:
                                    data.append(row)
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "activity_in_US_playlist", f"{required_dimension},{filters_1[i]}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "activity_in_US_playlist", f"{required_dimension},{filters_1[i]}.xlsx"), index=False)
                        elif filters_1[i] == "group":
                            data = []
                            col_names = None
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    dimensions=f"{required_dimension},{filters_1[i]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{required_filter};{filters_1[i]}=={','.join(self.groups)}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            if len(response["rows"]) != 0:
                                for row in response["rows"]:
                                    data.append(row)
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "activity_in_US_playlist", f"{required_dimension},{filters_1[i]}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "activity_in_US_playlist", f"{required_dimension},{filters_1[i]}.xlsx"), index=False)
                    elif i == 0 and j != 0 and k != 0:
                        data = []
                        col_names = None
                        for sub_type in sub_status:
                            for yt_type in yt_status:
                                response = self.execute_api_request(
                                    self.youtubeAnalytics.reports().query,
                                        dimensions=f"{required_dimension},{filters_2[j]},{filters_3[k]}",
                                        endDate=f"{self.end_date}",
                                        filters=f"{required_filter};{filters_2[j]}=={sub_type};{filters_3[k]}=={yt_type}",
                                        ids="channel==MINE",
                                        metrics=f"{','.join(metrics)}",
                                        startDate=f"{self.start_date}"
                                )
                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                if len(response["rows"]) != 0:
                                    for row in response["rows"]:
                                        data.append(row)
                        response_df = pd.DataFrame(data=data, columns=col_names)
                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "activity_in_US_playlist", f"{required_dimension},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "activity_in_US_playlist", f"{required_dimension},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                    elif i == 0 and j != 0 and k == 0:
                        data = []
                        col_names = None
                        for sub_type in sub_status:
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    dimensions=f"{required_dimension},{filters_2[j]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{required_filter};{filters_2[j]}=={sub_type}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            if len(response["rows"]) != 0:
                                for row in response["rows"]:
                                    data.append(row)
                        response_df = pd.DataFrame(data=data, columns=col_names)
                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "activity_in_US_playlist", f"{required_dimension},{filters_2[j]}.csv"), index=False)
                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "activity_in_US_playlist", f"{required_dimension},{filters_2[j]}.xlsx"), index=False)
                    elif i == 0 and j == 0 and k != 0:
                        data = []
                        col_names = None
                        for yt_type in yt_status:
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    dimensions=f"{required_dimension},{filters_3[k]}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{required_filter};{filters_3[k]}=={yt_type}",
                                    ids="channel==MINE",
                                    metrics=f"{','.join(metrics)}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            if len(response["rows"]) != 0:
                                for row in response["rows"]:
                                    data.append(row)
                        response_df = pd.DataFrame(data=data, columns=col_names)
                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "activity_in_US_playlist", f"{required_dimension},{filters_3[k]}.csv"), index=False)
                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "activity_in_US_playlist", f"{required_dimension},{filters_3[k]}.xlsx"), index=False)
                    elif i == 0 and j == 0 and k == 0:
                        data = []
                        col_names = None
                        response = self.execute_api_request(
                            self.youtubeAnalytics.reports().query,
                                dimensions=f"{required_dimension}",
                                endDate=f"{self.end_date}",
                                filters=f"{required_filter}",
                                ids="channel==MINE",
                                metrics=f"{','.join(metrics)}",
                                startDate=f"{self.start_date}"
                        )
                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                        if len(response["rows"]) != 0:
                            for row in response["rows"]:
                                data.append(row)
                        response_df = pd.DataFrame(data=data, columns=col_names)
                        response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "activity_in_US_playlist", f"{required_dimension}.csv"), index=False)
                        response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "activity_in_US_playlist", f"{required_dimension}.xlsx"), index=False)
        return
