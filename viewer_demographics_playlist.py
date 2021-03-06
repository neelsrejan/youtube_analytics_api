import os
import pandas as pd
from auth import Auth

class Viewer_Demographics_Playlist(Auth):

    def viewer_demographics_playlist(self):
        metric = "viewerPercentage"
        required_filter = "isCurated==1"
        dimensions = ["ageGroup", "gender", "ageGroup,gender"]
        sub_status = ["SUBSCRIBED", "UNSUBSCRIBED"]

        filters_to_remove_1 = []
        filters_to_remove_2 = []
        if not self.has_countries:
            filters_to_remove_1.append("country")
        if not self.has_provinces:
            filters_to_remove_1.append("province")
        if not self.has_continents:
            filters_to_remove_1.append("continent")
        if not self.has_subcontinents:
            filters_to_remove_1.append("subContinent")
        if not self.has_groups:
            filters_to_remove_2.append("group")

        filters_1 = ["", "country", "province", "continent", "subContinent"]
        filters_2 = ["", "playlist", "group"]
        filters_3 = ["", "subscribedStatus"]

        for fil_to_rem in filters_to_remove_1:
            filters_1.remove(fil_to_rem)
        for fil_to_rem in filters_to_remove_2:
            filters_2.remove(fil_to_rem)
        
        for dimension in dimensions:
            for i in range(len(filters_1)):
                for j in range(len(filters_2)):
                    for k in range(len(filters_3)):
                        if i != 0 and j != 0 and k != 0:
                            if filters_1[i] == "country":
                                if filters_2[j] == "playlist":
                                    data = []
                                    col_names = None
                                    for country in self.countries:
                                        for sub_type in sub_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{required_filter};{filters_1[i]}=={country};{filters_2[j]}=={','.join(self.playlist_ids)};{filters_3[k]}=={sub_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{metric}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                                elif filters_2[j] == "group":
                                    data = []
                                    col_names = None
                                    for country in self.countries:
                                        for sub_type in sub_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{required_filter};{filters_1[i]}=={country};{filters_2[j]}=={','.join(self.groups)};{filters_3[k]}=={sub_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{metric}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                            elif filters_1[i] == "province":
                                if filters_2[j] == "playlist":
                                    data = []
                                    col_names = None
                                    for province in self.provinces:
                                        for sub_type in sub_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{required_filter};{filters_1[i]}=={province};{filters_2[j]}=={','.join(self.playlist_ids)};{filters_3[k]}=={sub_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{metric}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                                elif filters_2[j] == "group":
                                    data = []
                                    col_names = None
                                    for province in self.provinces:
                                        for sub_type in sub_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{required_filter};{filters_1[i]}=={province};{filters_2[j]}=={','.join(self.groups)};{filters_3[k]}=={sub_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{metric}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                            elif filters_1[i] == "continent":
                                if filters_2[j] == "playlist":
                                    data = []
                                    col_names = None
                                    for continent in self.continents:
                                        for sub_type in sub_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_2[j]},{filters_3[k]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{required_filter};{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.playlist_ids)};{filters_3[k]}=={sub_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{metric}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if dimension != "ageGroup,gender":
                                                col_names.insert(1, f"{filters_1[i]}")
                                            else:
                                                col_names.insert(2, f"{filters_1[i]}")
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    if dimension != "ageGroup,gender":
                                                        row.insert(1, f"{continent}")
                                                    else:
                                                        row.insert(2, f"{continent}")
                                                    data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                                elif filters_2[j] == "group":
                                    data = []
                                    col_names = None
                                    for continent in self.continents:
                                        for sub_type in sub_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_2[j]},{filters_3[k]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{required_filter};{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.groups)};{filters_3[k]}=={sub_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{metric}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if dimension != "ageGroup,gender":
                                                col_names.insert(1, f"{filters_1[i]}")
                                            else:
                                                col_names.insert(2, f"{filters_1[i]}")
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    if dimension != "ageGroup,gender":
                                                        row.insert(1, f"{continent}")
                                                    else:
                                                        row.insert(2, f"{continent}")
                                                    data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                            elif filters_1[i] == "subContinent":
                                if filters_2[j] == "playlist":
                                    data = []
                                    col_names = None
                                    for subcontinent in self.subcontinents:
                                        for sub_type in sub_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_2[j]},{filters_3[k]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{required_filter};{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.playlist_ids)};{filters_3[k]}=={sub_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{metric}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if dimension != "ageGroup,gender":
                                                col_names.insert(1, f"{filters_1[i]}")
                                            else:
                                                col_names.insert(2, f"{filters_1[i]}")
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    if dimension != "ageGroup,gender":
                                                        row.insert(1, f"{subcontinent}")
                                                    else:
                                                        row.insert(2, f"{subcontinent}")
                                                    data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                                elif filters_2[j] == "group":
                                    data = []
                                    col_names = None
                                    for subcontinent in self.subcontinents:
                                        for sub_type in sub_status:
                                            response = self.execute_api_request(
                                                self.youtubeAnalytics.reports().query,
                                                    dimensions=f"{dimension},{filters_2[j]},{filters_3[k]}",
                                                    endDate=f"{self.end_date}",
                                                    filters=f"{required_filter};{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.groups)};{filters_3[k]}=={sub_type}",
                                                    ids="channel==MINE",
                                                    metrics=f"{metric}",
                                                    startDate=f"{self.start_date}"
                                            )
                                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                            if dimension != "ageGroup,gender":
                                                col_names.insert(1, f"{filters_1[i]}")
                                            else:
                                                col_names.insert(2, f"{filters_1[i]}")
                                            if len(response["rows"]) != 0:
                                                for row in response["rows"]:
                                                    if dimension != "ageGroup,gender":
                                                        row.insert(1, f"{subcontinent}")
                                                    else:
                                                        row.insert(2, f"{subcontinent}")
                                                    data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                        elif i != 0 and j != 0 and k == 0:
                            if filters_1[i] == "country":
                                if filters_2[j] == "playlist":
                                    data = []
                                    col_names = None
                                    for country in self.countries:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_1[i]},{filters_2[j]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{required_filter};{filters_1[i]}=={country};{filters_2[j]}=={','.join(self.playlist_ids)}",
                                                ids="channel==MINE",
                                                metrics=f"{metric}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]}.xlsx"), index=False)
                                elif filters_2[j] == "group":
                                    data = []
                                    col_names = None
                                    for country in self.countries:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_1[i]},{filters_2[j]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{required_filter};{filters_1[i]}=={country};{filters_2[j]}=={','.join(self.groups)}",
                                                ids="channel==MINE",
                                                metrics=f"{metric}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]}.xlsx"), index=False)
                            elif filters_1[i] == "province":
                                if filters_2[j] == "playlist":
                                    data = []
                                    col_names = None
                                    for province in self.provinces:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_1[i]},{filters_2[j]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{required_filter};{filters_1[i]}=={province};{filters_2[j]}=={','.join(self.playlist_ids)}",
                                                ids="channel==MINE",
                                                metrics=f"{metric}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]}.xlsx"), index=False)
                                elif filters_2[j] == "group":
                                    data = []
                                    col_names = None
                                    for province in self.provinces:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_1[i]},{filters_2[j]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{required_filter};{filters_1[i]}=={province};{filters_2[j]}=={','.join(self.groups)}",
                                                ids="channel==MINE",
                                                metrics=f"{metric}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]}.xlsx"), index=False)
                            elif filters_1[i] == "continent":
                                if filters_2[j] == "playlist":
                                    data = []
                                    col_names = None
                                    for continent in self.continents:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_2[j]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{required_filter};{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.playlist_ids)}",
                                                ids="channel==MINE",
                                                metrics=f"{metric}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if dimension != "ageGroup,gender":
                                            col_names.insert(1, f"{filters_1[i]}")
                                        else:
                                            col_names.insert(2, f"{filters_1[i]}")
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                if dimension != "ageGroup,gender":
                                                    row.insert(1, f"{continent}")
                                                else:
                                                    row.insert(2, f"{continent}")
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]}.xlsx"), index=False)
                                elif filters_2[j] == "group":
                                    data = []
                                    col_names = None
                                    for continent in self.continents:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_2[j]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{required_filter};{filters_1[i]}=={continent};{filters_2[j]}=={','.join(self.groups)}",
                                                ids="channel==MINE",
                                                metrics=f"{metric}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if dimension != "ageGroup,gender":
                                            col_names.insert(1, f"{filters_1[i]}")
                                        else:
                                            col_names.insert(2, f"{filters_1[i]}")
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                if dimension != "ageGroup,gender":
                                                    row.insert(1, f"{continent}")
                                                else:
                                                    row.insert(2, f"{continent}")
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]}.xlsx"), index=False)
                            elif filters_1[i] == "subContinent":
                                if filters_2[j] == "playlist":
                                    data = []
                                    col_names = None
                                    for subcontinent in self.subcontinents:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_2[j]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{required_filter};{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.playlist_ids)}",
                                                ids="channel==MINE",
                                                metrics=f"{metric}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if dimension != "ageGroup,gender":
                                            col_names.insert(1, f"{filters_1[i]}")
                                        else:
                                            col_names.insert(2, f"{filters_1[i]}")
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                if dimension != "ageGroup,gender":
                                                    row.insert(1, f"{subcontinent}")
                                                else:
                                                    row.insert(2, f"{subcontinent}")
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]}.xlsx"), index=False)
                                elif filters_2[j] == "group":
                                    data = []
                                    col_names = None
                                    for subcontinent in self.subcontinents:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_2[j]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{required_filter};{filters_1[i]}=={subcontinent};{filters_2[j]}=={','.join(self.groups)}",
                                                ids="channel==MINE",
                                                metrics=f"{metric}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if dimension != "ageGroup,gender":
                                            col_names.insert(1, f"{filters_1[i]}")
                                        else:
                                            col_names.insert(2, f"{filters_1[i]}")
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                if dimension != "ageGroup,gender":
                                                    row.insert(1, f"{subcontinent}")
                                                else:
                                                    row.insert(2, f"{subcontinent}")
                                                data.append(row)
                                    response_df = pd.DataFrame(data=data, columns=col_names)
                                    response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"), index=False)
                                    response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]}.xlsx"), index=False)
                        elif i != 0 and j == 0 and k != 0:
                            if filters_1[i] == "country":
                                data = []
                                col_names = None
                                for country in self.countries:
                                    for sub_type in sub_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_1[i]},{filters_3[k]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{required_filter};{filters_1[i]}=={country};{filters_3[k]}=={sub_type}",
                                                ids="channel==MINE",
                                                metrics=f"{metric}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_3[k]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_3[k]}.xlsx"), index=False)
                            elif filters_1[i] == "province":
                                data = []
                                col_names = None
                                for province in self.provinces:
                                    for sub_type in sub_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_1[i]},{filters_3[k]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{required_filter};{filters_1[i]}=={province};{filters_3[k]}=={sub_type}",
                                                ids="channel==MINE",
                                                metrics=f"{metric}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_3[k]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_3[k]}.xlsx"), index=False)
                            elif filters_1[i] == "continent":
                                data = []
                                col_names = None
                                for continent in self.continents:
                                    for sub_type in sub_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_3[k]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{required_filter};{filters_1[i]}=={continent};{filters_3[k]}=={sub_type}",
                                                ids="channel==MINE",
                                                metrics=f"{metric}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if dimension != "ageGroup,gender":
                                            col_names.insert(1, f"{filters_1[i]}")
                                        else:
                                            col_names.insert(2, f"{filters_1[i]}")
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                if dimension != "ageGroup,gender":
                                                    row.insert(1, f"{continent}")
                                                else:
                                                    row.insert(2, f"{continent}")
                                                data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_3[k]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_3[k]}.xlsx"), index=False)
                            elif filters_1[i] == "subContinent":
                                data = []
                                col_names = None
                                for subcontinent in self.subcontinents:
                                    for sub_type in sub_status:
                                        response = self.execute_api_request(
                                            self.youtubeAnalytics.reports().query,
                                                dimensions=f"{dimension},{filters_3[k]}",
                                                endDate=f"{self.end_date}",
                                                filters=f"{required_filter};{filters_1[i]}=={subcontinent};{filters_3[k]}=={sub_type}",
                                                ids="channel==MINE",
                                                metrics=f"{metric}",
                                                startDate=f"{self.start_date}"
                                        )
                                        col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                        if dimension != "ageGroup,gender":
                                            col_names.insert(1, f"{filters_1[i]}")
                                        else:
                                            col_names.insert(2, f"{filters_1[i]}")
                                        if len(response["rows"]) != 0:
                                            for row in response["rows"]:
                                                if dimension != "ageGroup,gender":
                                                    row.insert(1, f"{subcontinent}")
                                                else:
                                                    row.insert(2, f"{subcontinent}")
                                                data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_3[k]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_3[k]}.xlsx"), index=False)
                        elif i != 0 and j == 0 and k == 0:
                            if filters_1[i] == "country":
                                data = []
                                col_names = None
                                for country in self.countries:
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{dimension},{filters_1[i]}",
                                            endDate=f"{self.end_date}",
                                            filters=f"{required_filter};{filters_1[i]}=={country}",
                                            ids="channel==MINE",
                                            metrics=f"{metric}",
                                            startDate=f"{self.start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]}.xlsx"), index=False)
                            elif filters_1[i] == "province":
                                data = []
                                col_names = None
                                for province in self.provinces:
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{dimension},{filters_1[i]}",
                                            endDate=f"{self.end_date}",
                                            filters=f"{required_filter};{filters_1[i]}=={province}",
                                            ids="channel==MINE",
                                            metrics=f"{metric}",
                                            startDate=f"{self.start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]}.xlsx"), index=False)
                            elif filters_1[i] == "continent":
                                data = []
                                col_names = None
                                for continent in self.continents:
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{dimension}",
                                            endDate=f"{self.end_date}",
                                            filters=f"{required_filter};{filters_1[i]}=={continent}",
                                            ids="channel==MINE",
                                            metrics=f"{metric}",
                                            startDate=f"{self.start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    if dimension != "ageGroup,gender":
                                        col_names.insert(1, f"{filters_1[i]}")
                                    else:
                                        col_names.insert(2, f"{filters_1[i]}")
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            if dimension != "ageGroup,gender":
                                                row.insert(1, f"{continent}")
                                            else:
                                                row.insert(2, f"{continent}")
                                            data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]}.xlsx"), index=False)
                            elif filters_1[i] == "subContinent":
                                data = []
                                col_names = None
                                for subcontinent in self.subcontinents:
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{dimension}",
                                            endDate=f"{self.end_date}",
                                            filters=f"{required_filter};{filters_1[i]}=={subcontinent}",
                                            ids="channel==MINE",
                                            metrics=f"{metric}",
                                            startDate=f"{self.start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    if dimension != "ageGroup,gender":
                                        col_names.insert(1, f"{filters_1[i]}")
                                    else:
                                        col_names.insert(2, f"{filters_1[i]}")
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            if dimension != "ageGroup,gender":
                                                row.insert(1, f"{subcontinent}")
                                            else:
                                                row.insert(2, f"{subcontinent}")
                                            data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_1[i]}.xlsx"), index=False)
                        elif i == 0 and j != 0 and k != 0:
                            if filters_2[j] == "playlist":
                                data = []
                                col_names = None
                                for sub_type in sub_status:
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{dimension},{filters_2[j]},{filters_3[k]}",
                                            endDate=f"{self.end_date}",
                                            filters=f"{required_filter};{filters_2[j]}=={','.join(self.playlist_ids)};{filters_3[k]}=={sub_type}",
                                            ids="channel==MINE",
                                            metrics=f"{metric}",
                                            startDate=f"{self.start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                            elif filters_2[j] == "group":
                                data = []
                                col_names = None
                                for sub_type in sub_status:
                                    response = self.execute_api_request(
                                        self.youtubeAnalytics.reports().query,
                                            dimensions=f"{dimension},{filters_2[j]},{filters_3[k]}",
                                            endDate=f"{self.end_date}",
                                            filters=f"{required_filter};{filters_2[j]}=={','.join(self.groups)};{filters_3[k]}=={sub_type}",
                                            ids="channel==MINE",
                                            metrics=f"{metric}",
                                            startDate=f"{self.start_date}"
                                    )
                                    col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                    if len(response["rows"]) != 0:
                                        for row in response["rows"]:
                                            data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_2[j]},{filters_3[k]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_2[j]},{filters_3[k]}.xlsx"), index=False)
                        elif i == 0 and j != 0 and k == 0:
                            if filters_2[j] == "playlist":
                                data = []
                                col_names = None
                                response = self.execute_api_request(
                                    self.youtubeAnalytics.reports().query,
                                        dimensions=f"{dimension},{filters_2[j]}",
                                        endDate=f"{self.end_date}",
                                        filters=f"{required_filter};{filters_2[j]}=={','.join(self.playlist_ids)}",
                                        ids="channel==MINE",
                                        metrics=f"{metric}",
                                        startDate=f"{self.start_date}"
                                )
                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                if len(response["rows"]) != 0:
                                    for row in response["rows"]:
                                        data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_2[j]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_2[j]}.xlsx"), index=False)
                            elif filters_2[j] == "group":
                                data = []
                                col_names = None
                                response = self.execute_api_request(
                                    self.youtubeAnalytics.reports().query,
                                        dimensions=f"{dimension},{filters_2[j]}",
                                        endDate=f"{self.end_date}",
                                        filters=f"{required_filter};{filters_2[j]}=={','.join(self.groups)}",
                                        ids="channel==MINE",
                                        metrics=f"{metric}",
                                        startDate=f"{self.start_date}"
                                )
                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                if len(response["rows"]) != 0:
                                    for row in response["rows"]:
                                        data.append(row)
                                response_df = pd.DataFrame(data=data, columns=col_names)
                                response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_2[j]}.csv"), index=False)
                                response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_2[j]}.xlsx"), index=False)
                        elif i == 0 and j == 0 and k != 0:
                            data = []
                            col_names = None
                            for sub_type in sub_status:
                                response = self.execute_api_request(
                                    self.youtubeAnalytics.reports().query,
                                        dimensions=f"{dimension},{filters_3[k]}",
                                        endDate=f"{self.end_date}",
                                        filters=f"{required_filter};{filters_3[k]}=={sub_type}",
                                        ids="channel==MINE",
                                        metrics=f"{metric}",
                                        startDate=f"{self.start_date}"
                                )
                                col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                                if len(response["rows"]) != 0:
                                    for row in response["rows"]:
                                        data.append(row)
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_3[k]}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{filters_3[k]}.xlsx"), index=False)
                        elif i == 0 and j == 0 and k == 0:
                            data = []
                            col_names = None
                            response = self.execute_api_request(
                                self.youtubeAnalytics.reports().query,
                                    dimensions=f"{dimension}",
                                    endDate=f"{self.end_date}",
                                    filters=f"{required_filter}",
                                    ids="channel==MINE",
                                    metrics=f"{metric}",
                                    startDate=f"{self.start_date}"
                            )
                            col_names = [col_header["name"] for col_header in response["columnHeaders"]]
                            if len(response["rows"]) != 0:
                                for row in response["rows"]:
                                    data.append(row)
                            response_df = pd.DataFrame(data=data, columns=col_names)
                            response_df.to_csv(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension}.csv"), index=False)
                            response_df.to_excel(os.path.join(os.getcwd(), f"{self.channel_name}_data", f"{self.date_time}", "raw", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension}.xlsx"), index=False)
        return
