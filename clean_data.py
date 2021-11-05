import os
import pandas as pd
import warnings
from datetime import date
from channel_reports_variables import continents_rev_dict, subcontinents_rev_dict

def clean(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents):
    warnings.simplefilter("ignore")
    #clean_basic_user_activity_statistics(channel_name, date, has_groups, has_countries, has_continents, has_subcontinents)
    #clean_basic_user_activity_in_US(channel_name, date, has_groups, has_provinces)
    #clean_user_activity_by_location_over_time(channel_name, date, has_groups, has_countries, has_continents, has_subcontinents)
    #clean_user_activity_in_US_over_time(channel_name, date, has_groups, has_provinces)
    #clean_user_activity_by_location(channel_name, date, has_groups, has_countries, has_continents, has_subcontinents)
    #clean_user_activity_in_US(channel_name, date, has_groups, has_provinces)
    #clean_user_activity_by_location_over_subscribed_status(channel_name, date, has_groups, has_countries, has_continents, has_subcontinents)    
    #clean_user_activity_in_US_over_subscribed_status(channel_name, date, has_groups, has_provinces)
    #clean_playback_details_by_location_over_liveOrOnDemand(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents)
    #clean_playback_details_by_location_over_time(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents)
    #clean_playback_details_by_country(channel_name, date, has_groups, has_countries, has_continents, has_subcontinents)
    #clean_playback_details_by_country_averageViewPercentage(channel_name, date, has_groups, has_countries, has_continents, has_subcontinents)
    #clean_playback_details_in_US(channel_name, date, has_groups, has_provinces)
    #clean_playback_details_in_US_averageViewPercentage(channel_name, date, has_groups, has_provinces)
    #clean_video_playback_by_location(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents)
    #clean_playback_location_details(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents)
    #clean_traffic_source(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents)
    #clean_traffic_source_details(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents)
    #clean_device_type(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents)
    #clean_operating_system(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents)
    #clean_operating_system_and_device_type(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents)
    #clean_viewer_demographics(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents)
    #clean_engagement_and_content_sharing(channel_name, date, has_groups, has_countries, has_continents, has_subcontinents)
    #clean_audience_retention(channel_name, date)
    #clean_top_videos_regional(channel_name, date, has_countries, has_continents, has_subcontinents)
    #clean_top_videos_in_US(channel_name, date, has_provinces)
    #clean_top_videos_by_subscriber_type(channel_name, date, has_countries, has_continents, has_subcontinents)
    #clean_top_videos_by_yt_product(channel_name, date, has_countries, has_provinces, has_continents, has_subcontinents)
    #clean_top_videos_by_playback_details(channel_name, date, has_countries, has_provinces, has_continents, has_subcontinents)

    #clean_basic_stats_playlist(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents)
    #clean_time_based_playlist(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents)
    #clean_activity_by_location_playlist(channel_name, date, has_groups, has_countries, has_continents, has_subcontinents)
    #clean_activity_in_US_playlist(channel_name, date, has_groups, has_provinces)
    #clean_playback_locations_playlist(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents)
    #clean_playback_locations_details_playlist(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents)
    #clean_traffic_sources_playlist(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents)
    #clean_traffic_sources_details_playlist(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents)
    #clean_device_type_playlist(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents)
    #clean_operating_system_playlist(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents)
    #clean_operating_system_and_device_type_playlist(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents)
    #clean_viewer_demographics_playlist(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents)
    clean_top_playlists(channel_name, date, has_countries, has_provinces, has_continents, has_subcontinents)

def clean_basic_user_activity_statistics(channel_name, date, has_groups, has_countries, has_continents, has_subcontinents):

    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    filters_1 = ["", "country", "continent", "subContinent"]
    filters_2 = ["", "video", "group"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)

    df = None
    to_merge = None
    for i in range(len(filters_1)):
        for j in range(len(filters_2)):
            if i != 0 and j != 0:
                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "basic_user_activity_statistics", f"{filters_1[i]},{filters_2[j]}.csv"))
                merge_on = list(to_merge.columns)
                df = df.merge(to_merge, how="outer", on=merge_on)
            elif i != 0 and j == 0:
                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "basic_user_activity_statistics", f"{filters_1[i]}.csv"))
                merge_on = list(to_merge.columns)[1:]
                df = df.merge(to_merge, how="outer", on=merge_on)
            elif i == 0 and j != 0:
                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "basic_user_activity_statistics", f"{filters_2[j]}.csv"))
                merge_on = list(to_merge.columns)[1:]
                df = df.merge(to_merge, how="outer", on=merge_on)
            elif i == 0 and j == 0:
                df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "basic_user_activity_statistics", "unfiltered.csv"))
    
    cols_to_swap = filters_2[1:] + filters_1[1:]
    for i in range(len(cols_to_swap)):
        if cols_to_swap[i] == "continent":
            col = df[f"{cols_to_swap[i]}"]
            new_col = []
            for row in col:
                if row == row:
                    if row < 10:
                        new_col.append(continents_rev_dict["00" + str(int(row))])
                    elif row < 100:
                        new_col.append(continents_rev_dict["0" + str(int(row))])
                    else:
                        new_col.append(continents_rev_dict[str(int(row))])
                else:
                    new_col.append(row)
            df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
            df.insert(i, f"{cols_to_swap[i]}", new_col)
        elif cols_to_swap[i] == "subContinent":
            col = df[f"{cols_to_swap[i]}"]
            new_col = []
            for row in col:
                if row == row:
                    if row < 10:
                        new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                    elif row < 100:
                        new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                    else:
                        new_col.append(subcontinents_rev_dict[str(int(row))])
                else:
                    new_col.append(row)
            df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
            df.insert(i, f"{cols_to_swap[i].lower()}", new_col)
        else:
            col = df[f"{cols_to_swap[i]}"]
            df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
            df.insert(i, f"{cols_to_swap[i]}", col)
    df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "basic_user_activity_statistics", f"{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
    df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "basic_user_activity_statistics", f"{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_basic_user_activity_in_US(channel_name, date, has_groups, has_provinces):

    if not has_provinces:
        return

    filters_to_remove = []
    if not has_groups:
        filters_to_remove.append("group")

    filters_1 = ["province"]
    filters_2 = ["", "video", "group"]
    for fil_to_rem in filters_to_remove:
        filters_2.remove(fil_to_rem)

    df = None
    to_merge = None
    for i in range(len(filters_1)):
        for j in range(len(filters_2)):
            if i == 0 and j != 0:
                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "basic_user_activity_in_US", f"{filters_1[i]},{filters_2[j]}.csv"))
                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                df = df.merge(to_merge, how="outer", on=merge_on)
            elif i == 0 and j == 0:
                df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "basic_user_activity_in_US", f"{filters_1[i]}.csv"))

    cols_to_swap = filters_2[1:]
    for i in range(len(cols_to_swap)):
        col = df[f"{cols_to_swap[i]}"]
        df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
        df.insert(i+1, f"{cols_to_swap[i]}", col)
    df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "basic_user_activity_in_US", f"{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
    df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "basic_user_activity_in_US", f"{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_user_activity_by_location_over_time(channel_name, date, has_groups, has_countries, has_continents, has_subcontinents):

    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    dimensions= ["day", "month"]
    filters_1 = ["", "country", "continent", "subContinent"]
    filters_2 = ["", "video", "group"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)

    df = None
    to_merge = None
    for dimension in dimensions:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                if i != 0 and j != 0:
                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location_over_time", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                    merge_on = list(to_merge.columns)
                    df = df.merge(to_merge, how="outer", on=merge_on)
                elif i != 0 and j == 0:
                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location_over_time", f"{dimension},{filters_1[i]}.csv"))
                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                    df = df.merge(to_merge, how="outer", on=merge_on)
                elif i == 0 and j != 0:
                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location_over_time", f"{dimension},{filters_2[j]}.csv"))
                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                    df = df.merge(to_merge, how="outer", on=merge_on)
                elif i == 0 and j == 0:
                    df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location_over_time", f"{dimension}.csv"))
        
        cols_to_swap = filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if cols_to_swap[i] == "continent":
                col = df[f"{cols_to_swap[i]}"]
                new_col = []
                for row in col:
                    if row == row:
                        if row < 10:
                            new_col.append(continents_rev_dict["00" + str(int(row))])
                        elif row < 100:
                            new_col.append(continents_rev_dict["0" + str(int(row))])
                        else:
                            new_col.append(continents_rev_dict[str(int(row))])
                    else:
                        new_col.append(row)
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i, f"{cols_to_swap[i]}", new_col)
            elif cols_to_swap[i] == "subContinent":
                col = df[f"{cols_to_swap[i]}"]
                new_col = []
                for row in col:
                    if row == row:
                        if row < 10:
                            new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                        elif row < 100:
                            new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                        else:
                            new_col.append(subcontinents_rev_dict[str(int(row))])
                    else:
                        new_col.append(row)
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i, f"{cols_to_swap[i].lower()}", new_col)
            else:
                col = df[f"{cols_to_swap[i]}"]
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i, f"{cols_to_swap[i]}", col)
        df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "user_activity_by_location_over_time", f"{dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
        df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "user_activity_by_location_over_time", f"{dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_user_activity_in_US_over_time(channel_name, date, has_groups, has_provinces):

    if not has_provinces:
        return

    filters_to_remove = []
    if not has_groups:
        filters_to_remove.append("group")

    dimensions = ["day", "month"]
    filters_1 = ["province"]
    filters_2 = ["", "video", "group"]
    for fil_to_rem in filters_to_remove:
        filters_2.remove(fil_to_rem)

    df = None
    to_merge = None
    for dimension in dimensions:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                if i == 0 and j != 0:
                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_in_US_over_time", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                    merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                    df = df.merge(to_merge, how="outer", on=merge_on)
                elif i == 0 and j == 0:
                    df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_in_US_over_time", f"{dimension},{filters_1[i]}.csv"))

        cols_to_swap = filters_2[1:]
        for i in range(len(cols_to_swap)):
            col = df[f"{cols_to_swap[i]}"]
            df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
            df.insert(i+2, f"{cols_to_swap[i]}", col)
        df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "user_activity_in_US_over_time", f"{dimension},{filters_1[i]},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
        df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "user_activity_in_US_over_time", f"{dimension},{filters_1[i]},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_user_activity_by_location(channel_name, date, has_groups, has_countries, has_continents, has_subcontinents):

    if not has_countries:
        return

    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")
    
    dimensions = ["country"]
    filters_1 = ["", "continent", "subContinent"]
    filters_2 = ["", "video", "group"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)

    df = None
    to_merge = None
    for dimension in dimensions:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                if i != 0 and j != 0:
                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                    merge_on = list(to_merge.columns)
                    df = df.merge(to_merge, how="outer", on=merge_on)
                elif i != 0 and j == 0:
                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location", f"{dimension},{filters_1[i]}.csv"))
                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                    df = df.merge(to_merge, how="outer", on=merge_on)
                elif i == 0 and j != 0:
                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location", f"{dimension},{filters_2[j]}.csv"))
                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                    df = df.merge(to_merge, how="outer", on=merge_on)
                elif i == 0 and j == 0:
                    df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location", f"{dimension}.csv"))
    
    cols_to_swap = filters_2[1:] + filters_1[1:]
    for i in range(len(cols_to_swap)):
        if cols_to_swap[i] == "continent":
            col = df[f"{cols_to_swap[i]}"]
            new_col = []
            for row in col:
                if row == row:
                    if row < 10:
                        new_col.append(continents_rev_dict["00" + str(int(row))])
                    elif row < 100:
                        new_col.append(continents_rev_dict["0" + str(int(row))])
                    else:
                        new_col.append(continents_rev_dict[str(int(row))])
                else:
                    new_col.append(row)
            df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
            df.insert(i, f"{cols_to_swap[i]}", new_col)
        elif cols_to_swap[i] == "subContinent":
            col = df[f"{cols_to_swap[i]}"]
            new_col = []
            for row in col:
                if row == row:
                    if row < 10:
                        new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                    elif row < 100:
                        new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                    else:
                        new_col.append(subcontinents_rev_dict[str(int(row))])
                else:
                    new_col.append(row)
            df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
            df.insert(i, f"{cols_to_swap[i].lower()}", new_col)
        else:
            col = df[f"{cols_to_swap[i]}"]
            df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
            df.insert(i, f"{cols_to_swap[i]}", col)
    df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "user_activity_by_location", f"{dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
    df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "user_activity_by_location", f"{dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_user_activity_in_US(channel_name, date, has_groups, has_provinces):

    if not has_provinces:
        return

    filters_to_remove = []
    if not has_groups:
        filters_to_remove.append("group")

    dimensions = ["province"]
    filters_1 = ["country"]
    filters_2 = ["", "video", "group"]

    for fil_to_rem in filters_to_remove:
        filters_2.remove(fil_to_rem)

    df = None
    to_merge = None
    for dimension in dimensions:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                if i == 0 and j != 0:
                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_in_US", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                    merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                    df = df.merge(to_merge, how="outer", on=merge_on)
                elif i == 0 and j == 0:
                    df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_in_US", f"{dimension},{filters_1[i]}.csv"))

    cols_to_swap = filters_2[1:]
    for i in range(len(cols_to_swap)):
        col = df[f"{cols_to_swap[i]}"]
        df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
        df.insert(i+2, f"{cols_to_swap[i]}", col)
    df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "user_activity_in_US", f"{dimension},{filters_1[i]},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
    df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "user_activity_in_US", f"{dimension},{filters_1[i]},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_user_activity_by_location_over_subscribed_status(channel_name, date, has_groups, has_countries, has_continents, has_subcontinents):

    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    dimensions = ["", "day", "month"]
    filters_1 = ["", "country", "continent", "subContinent"]
    filters_2 = ["", "video", "group"]
    filters_3 = ["", "subscribedStatus"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)

    df = None
    to_merge = None
    for dimension in dimensions:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    if i != 0 and j != 0 and k != 0:
                        if dimension == "day" or dimension == "month":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location_over_subscribed_status", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location_over_subscribed_status", f"{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j != 0 and k == 0:
                        if dimension == "day" or dimension == "month":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location_over_subscribed_status", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location_over_subscribed_status", f"{filters_1[i]},{filters_2[j]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k != 0:
                        if dimension == "day" or dimension == "month":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location_over_subscribed_status", f"{dimension},{filters_1[i]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location_over_subscribed_status", f"{filters_1[i]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k == 0:
                        if dimension == "day" or dimension == "month":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location_over_subscribed_status", f"{dimension},{filters_1[i]}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location_over_subscribed_status", f"{filters_1[i]}.csv"))
                            merge_on = list(to_merge.columns)[1:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k != 0:
                        if dimension == "day" or dimension == "month":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location_over_subscribed_status", f"{dimension},{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location_over_subscribed_status", f"{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k == 0:
                        if dimension == "day" or dimension == "month":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location_over_subscribed_status", f"{dimension},{filters_2[j]}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location_over_subscribed_status", f"{filters_2[j]}.csv"))
                            merge_on = list(to_merge.columns)[1:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k != 0:
                        if dimension == "day" or dimension == "month":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location_over_subscribed_status", f"{dimension},{filters_3[k]}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location_over_subscribed_status", f"{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)[1:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k == 0:
                        if dimension == "day" or dimension == "month":
                            df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location_over_subscribed_status", f"{dimension}.csv"))
                        else:
                            df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_by_location_over_subscribed_status", "unfiltered.csv"))


        cols_to_swap = filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if dimension == "day" or dimension == "month":
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", col)
            else:
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i, f"{cols_to_swap[i]}", col)
        if dimension == "day" or dimension == "month":
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "user_activity_by_location_over_subscribed_status", f"{dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "user_activity_by_location_over_subscribed_status", f"{dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")
        else:
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "user_activity_by_location_over_subscribed_status", f"{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "user_activity_by_location_over_subscribed_status", f"{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_user_activity_in_US_over_subscribed_status(channel_name, date, has_groups, has_provinces):

    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_groups:
        filters_to_remove_1.append("group")
    if not has_provinces:
        filters_to_remove_2.append("province")

    dimensions = ["", "day", "month"]
    filters_1 = ["", "video", "group"]
    filters_2 = ["", "province"]
    filters_3 = ["", "subscribedStatus"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)

    df = None
    to_merge = None
    for dimension in dimensions:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    if i != 0 and j != 0 and k != 0:
                        if dimension == "day" or dimension == "month":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", f"{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j != 0 and k == 0:
                        if dimension == "day" or dimension == "month":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", f"{filters_1[i]},{filters_2[j]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k != 0:
                        if dimension == "day" or dimension == "month":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", f"{dimension},{filters_1[i]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", f"{filters_1[i]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k == 0:
                        if dimension == "day" or dimension == "month":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", f"{dimension},{filters_1[i]}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", f"{filters_1[i]}.csv"))
                            merge_on = list(to_merge.columns)[1:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k != 0:
                        if dimension == "day" or dimension == "month":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", f"{dimension},{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", f"{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k == 0:
                        if dimension == "day" or dimension == "month":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", f"{dimension},{filters_2[j]}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", f"{filters_2[j]}.csv"))
                            merge_on = list(to_merge.columns)[1:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k != 0:
                        if dimension == "day" or dimension == "month":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", f"{dimension},{filters_3[k]}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", f"{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)[1:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k == 0:
                        if dimension == "day" or dimension == "month":
                            df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", f"{dimension}.csv"))
                        else:
                            df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", "unfiltered.csv"))

        cols_to_swap = filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if dimension == "day" or dimension == "month":
                col = df[f"{cols_to_swap[i]}"]
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i]}", col)
            else:
                col = df[f"{cols_to_swap[i]}"]
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i, f"{cols_to_swap[i]}", col)
        if dimension == "day" or dimension == "month":
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", f"{dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", f"{dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")
        else:
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "user_activity_in_US_over_subscribed_status", f"{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "user_activity_in_US_over_subscribed_status", f"{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_playback_details_by_location_over_liveOrOnDemand(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents):

    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_provinces:
        filters_to_remove_1.append("province")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    dimensions = ["", "day", "month"]
    filters_1 = ["", "country", "province", "continent", "subContinent"]
    filters_2 = ["", "video", "group"]
    filters_3 = ["", "liveOrOnDemand"]
    filters_4 = ["", "subscribedStatus"]
    filters_5 = ["", "youtubeProduct"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)
    
    df = None
    to_merge = None
    for dimension in dimensions:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    for l in range(len(filters_4)):
                        for m in range(len(filters_5)):
                            if i != 0 and j != 0 and k != 0 and l != 0 and m != 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k != 0 and l != 0 and m == 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k != 0 and l == 0 and m != 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k != 0 and l == 0 and m == 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k == 0 and l != 0 and m != 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_1[i]},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k == 0 and l != 0 and m == 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k == 0 and l == 0 and m != 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_1[i]},{filters_2[j]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k == 0 and l == 0 and m == 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_1[i]},{filters_2[j]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k != 0 and l != 0 and m != 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_1[i]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k != 0 and l != 0 and m == 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k != 0 and l == 0 and m != 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_1[i]},{filters_3[k]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k != 0 and l == 0 and m == 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_1[i]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_1[i]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k == 0 and l != 0 and m != 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_1[i]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_1[i]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k == 0 and l != 0 and m == 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_1[i]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_1[i]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k == 0 and l == 0 and m != 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_1[i]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_1[i]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k == 0 and l == 0 and m == 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_1[i]}.csv"))
                                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_1[i]}.csv"))
                                    merge_on = list(to_merge.columns)[1:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k != 0 and l != 0 and m != 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k != 0 and l != 0 and m == 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k != 0 and l == 0 and m != 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_2[j]},{filters_3[k]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k != 0 and l == 0 and m == 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_2[j]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_2[j]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k == 0 and l != 0 and m != 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k == 0 and l != 0 and m == 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_2[j]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_2[j]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k == 0 and l == 0 and m != 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_2[j]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_2[j]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k == 0 and l == 0 and m == 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_2[j]}.csv"))
                                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_2[j]}.csv"))
                                    merge_on = list(to_merge.columns)[1:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k != 0 and l != 0 and m != 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k != 0 and l != 0 and m == 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k != 0 and l == 0 and m != 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_3[k]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_3[k]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k != 0 and l == 0 and m == 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_3[k]}.csv"))
                                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)[1:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k == 0 and l != 0 and m != 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k == 0 and l != 0 and m == 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_4[l]}.csv"))
                                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)[1:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k == 0 and l == 0 and m != 0:
                                if dimension == "day" or dimension == "month":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{filters_5[m]}.csv"))
                                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)[1:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k == 0 and l == 0 and m == 0:
                                if dimension == "day" or dimension == "month":
                                    df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension}.csv"))
                                else:
                                    df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", "unfiltered.csv"))

        cols_to_swap = filters_5[1:] + filters_4[1:] + filters_3[1:] + filters_2[1:] + filters_1[i:]
        for i in range(len(cols_to_swap)):
            if dimension == "day" or dimension == "month":
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", col)
            else:
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i, f"{cols_to_swap[i]}", col)
        if dimension == "day" or dimension == "month":
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "playback_details_by_location_over_liveOrOnDemand", f"{dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")
        else:
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "playback_details_by_location_over_liveOrOnDemand", f"{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "playback_details_by_location_over_liveOrOnDemand", f"{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_playback_details_by_location_over_time(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents):
    
    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_provinces:
        filters_to_remove_1.append("province")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    dimensions = ["", "day", "month"]
    filters_1 = ["", "country", "province", "continent", "subContinent"]
    filters_2 = ["", "video", "group"]
    filters_3 = ["", "subscribedStatus"]
    filters_4 = ["", "youtubeProduct"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)

    df = None
    to_merge = None
    for dimension in dimensions:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    for l in range(len(filters_4)):
                        if i != 0 and j != 0 and k != 0 and l != 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k != 0 and l == 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k == 0 and l != 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k == 0 and l == 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{filters_1[i]},{filters_2[j]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k != 0 and l != 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k != 0 and l == 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{dimension},{filters_1[i]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{filters_1[i]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k == 0 and l != 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{dimension},{filters_1[i]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{filters_1[i]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k == 0 and l == 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{dimension},{filters_1[i]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{filters_1[i]}.csv"))
                                merge_on = list(to_merge.columns)[1:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k != 0 and l != 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k != 0 and l == 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{dimension},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k == 0 and l != 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{dimension},{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k == 0 and l == 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{dimension},{filters_2[j]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{filters_2[j]}.csv"))
                                merge_on = list(to_merge.columns)[1:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k != 0 and l != 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{dimension},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k != 0 and l == 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{dimension},{filters_3[k]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)[1:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k == 0 and l != 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{dimension},{filters_4[l]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)[1:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k == 0 and l == 0:
                            if dimension == "day" or dimension == "month":
                                df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", f"{dimension}.csv"))
                            else:
                                df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_location_over_time", "unfiltered.csv"))

        cols_to_swap = filters_4[1:] + filters_3[1:] + filters_2[1:] + filters_1[i:]
        for i in range(len(cols_to_swap)):
            if dimension == "day" or dimension == "month":
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", col)
            else:
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i, f"{cols_to_swap[i]}", col)
        if dimension == "day" or dimension == "month":
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "playback_details_by_location_over_time", f"{dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "playback_details_by_location_over_time", f"{dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")
        else:
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "playback_details_by_location_over_time", f"{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "playback_details_by_location_over_time", f"{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_playback_details_by_country(channel_name, date, has_groups, has_countries, has_continents, has_subcontinents):

    if not has_countries:
        return

    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    dimensions = ["country"]
    filters_1 = ["", "continent", "subContinent"]
    filters_2 = ["", "video", "group"]
    filters_3 = ["", "liveOrOnDemand"]
    filters_4 = ["", "subscribedStatus"]
    filters_5 = ["", "youtubeProduct"]
    

    df = None
    to_merge = None
    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)
    
    df = None
    to_merge = None
    for dimension in dimensions:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    for l in range(len(filters_4)):
                        for m in range(len(filters_5)):
                            if i != 0 and j != 0 and k != 0 and l != 0 and m != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k != 0 and l != 0 and m == 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k != 0 and l == 0 and m != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_5[m]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k != 0 and l == 0 and m == 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k == 0 and l != 0 and m != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k == 0 and l != 0 and m == 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k == 0 and l == 0 and m != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_5[m]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k == 0 and l == 0 and m == 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k != 0 and l != 0 and m != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k != 0 and l != 0 and m == 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k != 0 and l == 0 and m != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_5[m]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k != 0 and l == 0 and m == 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k == 0 and l != 0 and m != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_4[l]},{filters_5[m]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k == 0 and l != 0 and m == 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k == 0 and l == 0 and m != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]},{filters_5[m]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k == 0 and l == 0 and m == 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_1[i]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k != 0 and l != 0 and m != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k != 0 and l != 0 and m == 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k != 0 and l == 0 and m != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k != 0 and l == 0 and m == 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k == 0 and l != 0 and m != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k == 0 and l != 0 and m == 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k == 0 and l == 0 and m != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]},{filters_5[m]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k == 0 and l == 0 and m == 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_2[j]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k != 0 and l != 0 and m != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k != 0 and l != 0 and m == 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k != 0 and l == 0 and m != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_3[k]},{filters_5[m]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k != 0 and l == 0 and m == 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_3[k]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k == 0 and l != 0 and m != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_4[l]},{filters_5[m]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k == 0 and l != 0 and m == 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_4[l]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k == 0 and l == 0 and m != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension},{filters_5[m]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k == 0 and l == 0 and m == 0:
                                df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country", f"{dimension}.csv"))

        cols_to_swap = filters_5[1:] + filters_4[1:] + filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if cols_to_swap[i] == "continent":
                col = df[f"{cols_to_swap[i]}"]
                new_col = []
                for row in col:
                    if row == row:
                        if row < 10:
                            new_col.append(continents_rev_dict["00" + str(int(row))])
                        elif row < 100:
                            new_col.append(continents_rev_dict["0" + str(int(row))])
                        else:
                            new_col.append(continents_rev_dict[str(int(row))])
                    else:
                        new_col.append(row)
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i]}", new_col)
            elif cols_to_swap[i] == "subContinent":
                col = df[f"{cols_to_swap[i]}"]
                new_col = []
                for row in col:
                    if row == row:
                        if row < 10:
                            new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                        elif row < 100:
                            new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                        else:
                            new_col.append(subcontinents_rev_dict[str(int(row))])
                    else:
                        new_col.append(row)
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
            else:
                col = df[f"{cols_to_swap[i]}"]
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i]}", col)
        df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "playback_details_by_country", f"{dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
        df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "playback_details_by_country", f"{dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")
        
def clean_playback_details_by_country_averageViewPercentage(channel_name, date, has_groups, has_countries, has_continents, has_subcontinents):

    if not has_countries:
        return

    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    dimensions = ["country"]
    filters_1 = ["", "continent", "subContinent"]
    filters_2 = ["", "video", "group"]
    filters_3 = ["", "subscribedStatus"]
    filters_4 = ["", "youtubeProduct"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)
    
    df = None
    to_merge = None
    for dimension in dimensions:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    for l in range(len(filters_4)):
                        if i != 0 and j != 0 and k != 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country_averageViewPercentage", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k != 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country_averageViewPercentage", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k == 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country_averageViewPercentage", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k == 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country_averageViewPercentage", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k != 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country_averageViewPercentage", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k != 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country_averageViewPercentage", f"{dimension},{filters_1[i]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k == 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country_averageViewPercentage", f"{dimension},{filters_1[i]},{filters_4[l]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k == 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country_averageViewPercentage", f"{dimension},{filters_1[i]}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k != 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country_averageViewPercentage", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k != 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country_averageViewPercentage", f"{dimension},{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k == 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country_averageViewPercentage", f"{dimension},{filters_2[j]},{filters_4[l]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k == 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country_averageViewPercentage", f"{dimension},{filters_2[j]}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k != 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country_averageViewPercentage", f"{dimension},{filters_3[k]},{filters_4[l]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k != 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country_averageViewPercentage", f"{dimension},{filters_3[k]}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k == 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country_averageViewPercentage", f"{dimension},{filters_4[l]}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k == 0 and l == 0:
                            df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_by_country_averageViewPercentage", f"{dimension}.csv"))

        cols_to_swap = filters_4[1:] + filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if cols_to_swap[i] == "continent":
                col = df[f"{cols_to_swap[i]}"]
                new_col = []
                for row in col:
                    if row == row:
                        if row < 10:
                            new_col.append(continents_rev_dict["00" + str(int(row))])
                        elif row < 100:
                            new_col.append(continents_rev_dict["0" + str(int(row))])
                        else:
                            new_col.append(continents_rev_dict[str(int(row))])
                    else:
                        new_col.append(row)
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i]}", new_col)
            elif cols_to_swap[i] == "subContinent":
                col = df[f"{cols_to_swap[i]}"]
                new_col = []
                for row in col:
                    if row == row:
                        if row < 10:
                            new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                        elif row < 100:
                            new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                        else:
                            new_col.append(subcontinents_rev_dict[str(int(row))])
                    else:
                        new_col.append(row)
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
            else:
                col = df[f"{cols_to_swap[i]}"]
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i]}", col)
        df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "playback_details_by_country_averageViewPercentage", f"{dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
        df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "playback_details_by_country_averageViewPercentage", f"{dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_playback_details_in_US(channel_name, date, has_groups, has_provinces):

    if not has_provinces:
        return

    filters_to_remove = []
    if not has_groups:
        filters_to_remove.append("group")

    dimensions = ["province"]
    filters_1 = ["", "video", "group"]
    filters_2 = ["", "liveOrOnDemand"]
    filters_3 = ["", "subscribedStatus"]
    filters_4 = ["", "youtubeProduct"]

    for fil_to_rem in filters_to_remove:
        filters_1.remove(fil_to_rem)
    
    df = None
    to_merge = None
    for dimension in dimensions:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    for l in range(len(filters_4)):
                        if i != 0 and j != 0 and k != 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k != 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k == 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k == 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k != 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k != 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k == 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]},{filters_4[l]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k == 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_1[i]}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k != 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k != 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k == 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_2[j]},{filters_4[l]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k == 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_2[j]}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k != 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_3[k]},{filters_4[l]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k != 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_3[k]}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k == 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension},{filters_4[l]}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k == 0 and l == 0:
                            df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US", f"{dimension}.csv"))

    cols_to_swap = filters_4[1:] + filters_3[1:] + filters_2[1:] + filters_1[1:]
    for i in range(len(cols_to_swap)):
        if cols_to_swap[i] == "continent":
            col = df[f"{cols_to_swap[i]}"]
            new_col = []
            for row in col:
                if row == row:
                    if row < 10:
                        new_col.append(continents_rev_dict["00" + str(int(row))])
                    elif row < 100:
                        new_col.append(continents_rev_dict["0" + str(int(row))])
                    else:
                        new_col.append(continents_rev_dict[str(int(row))])
                else:
                    new_col.append(row)
            df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
            df.insert(i+1, f"{cols_to_swap[i]}", new_col)
        elif cols_to_swap[i] == "subContinent":
            col = df[f"{cols_to_swap[i]}"]
            new_col = []
            for row in col:
                if row == row:
                    if row < 10:
                        new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                    elif row < 100:
                        new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                    else:
                        new_col.append(subcontinents_rev_dict[str(int(row))])
                else:
                    new_col.append(row)
            df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
            df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
        else:
            col = df[f"{cols_to_swap[i]}"]
            df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
            df.insert(i+1, f"{cols_to_swap[i]}", col)
    df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "playback_details_in_US", f"{dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
    df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "playback_details_in_US", f"{dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")


def clean_playback_details_in_US_averageViewPercentage(channel_name, date, has_groups, has_provinces):
    
    if not has_provinces:
        return

    filters_to_remove = []
    if not has_groups:
        filters_to_remove.append("group")

    dimensions = ["province"]
    filters_1 = ["", "video", "group"]
    filters_2 = ["", "subscribedStatus"]
    filters_3 = ["", "youtubeProduct"]

    for fil_to_rem in filters_to_remove:
        filters_1.remove(fil_to_rem)
    
    df = None
    to_merge = None
    for dimension in dimensions:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    if i != 0 and j != 0 and k != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US_averageViewPercentage", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j != 0 and k == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US_averageViewPercentage", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US_averageViewPercentage", f"{dimension},{filters_1[i]},{filters_3[k]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US_averageViewPercentage", f"{dimension},{filters_1[i]}.csv"))
                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US_averageViewPercentage", f"{dimension},{filters_2[j]},{filters_3[k]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US_averageViewPercentage", f"{dimension},{filters_2[j]}.csv"))
                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US_averageViewPercentage", f"{dimension},{filters_3[k]}.csv"))
                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k == 0:
                        df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_details_in_US_averageViewPercentage", f"{dimension}.csv"))

        cols_to_swap = filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if cols_to_swap[i] == "continent":
                col = df[f"{cols_to_swap[i]}"]
                new_col = []
                for row in col:
                    if row == row:
                        if row < 10:
                            new_col.append(continents_rev_dict["00" + str(int(row))])
                        elif row < 100:
                            new_col.append(continents_rev_dict["0" + str(int(row))])
                        else:
                            new_col.append(continents_rev_dict[str(int(row))])
                    else:
                        new_col.append(row)
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i]}", new_col)
            elif cols_to_swap[i] == "subContinent":
                col = df[f"{cols_to_swap[i]}"]
                new_col = []
                for row in col:
                    if row == row:
                        if row < 10:
                            new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                        elif row < 100:
                            new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                        else:
                            new_col.append(subcontinents_rev_dict[str(int(row))])
                    else:
                        new_col.append(row)
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
            else:
                col = df[f"{cols_to_swap[i]}"]
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i]}", col)
    df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "playback_details_in_US_averageViewPercentage", f"{dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
    df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "playback_details_in_US_averageViewPercentage", f"{dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")


def clean_video_playback_by_location(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents):
    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_provinces:
        filters_to_remove_1.append("province")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    required_dimension = "insightPlaybackLocationType"
    dimensions = ["", "day"]
    filters_1 = ["", "country", "province", "continent", "subContinent"]
    filters_2 = ["", "video", "group"]
    filters_3 = ["", "liveOrOnDemand"]
    filters_4 = ["", "subscribedStatus"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)
    
    df = None
    to_merge = None
    for dimension in dimensions:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    for l in range(len(filters_4)):
                        if i != 0 and j != 0 and k != 0 and l != 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k != 0 and l == 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k == 0 and l != 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k == 0 and l == 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{filters_1[i]},{filters_2[j]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k != 0 and l != 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k != 0 and l == 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{dimension},{filters_1[i]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{filters_1[i]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k == 0 and l != 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{dimension},{filters_1[i]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{filters_1[i]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k == 0 and l == 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{dimension},{filters_1[i]}.csv"))
                                merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{filters_1[i]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k != 0 and l != 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k != 0 and l == 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k == 0 and l != 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{dimension},{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k == 0 and l == 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{dimension},{filters_2[j]}.csv"))
                                merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{filters_2[j]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k != 0 and l != 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{dimension},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k != 0 and l == 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{dimension},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{filters_3[k]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k == 0 and l != 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{dimension},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{filters_4[l]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k == 0 and l == 0:
                            if dimension == "day":
                                df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{dimension}.csv"))
                            else:
                                df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "video_playback_by_location", f"{required_dimension}.csv"))

        cols_to_swap = filters_4[1:] + filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if dimension == "day":
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i]}", col)
            else:
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", col)
        if dimension == "day":
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "video_playback_by_location", f"{required_dimension},{dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")
        else:
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "video_playback_by_location", f"{required_dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "video_playback_by_location", f"{required_dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")


def clean_playback_location_details(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents):
    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_provinces:
        filters_to_remove_1.append("province")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    required_dimension = "insightPlaybackLocationDetail"
    required_filter = "insightPlaybackLocationType==EMBEDDED"
    sorting_options = ["-views", "-estimatedMinutesWatched"]
    filters_1 = ["", "country", "province", "continent", "subContinent"]
    filters_2 = ["", "video", "group"]
    filters_3 = ["", "liveOrOnDemand"]
    filters_4 = ["", "subscribedStatus"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)
    
    df = None
    to_merge = None
    for sort_by in sorting_options:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    for l in range(len(filters_4)):
                        if i != 0 and j != 0 and k != 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_location_details", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k != 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_location_details", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k == 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_location_details", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k == 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_location_details", f"{required_dimension},{filters_1[i]},{filters_2[j]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k != 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_location_details", f"{required_dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k != 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_location_details", f"{required_dimension},{filters_1[i]},{filters_3[k]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k == 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_location_details", f"{required_dimension},{filters_1[i]},{filters_4[l]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k == 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_location_details", f"{required_dimension},{filters_1[i]},{sort_by}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k != 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_location_details", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k != 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_location_details", f"{required_dimension},{filters_2[j]},{filters_3[k]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k == 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_location_details", f"{required_dimension},{filters_2[j]},{filters_4[l]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k == 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_location_details", f"{required_dimension},{filters_2[j]},{sort_by}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k != 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_location_details", f"{required_dimension},{filters_3[k]},{filters_4[l]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k != 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_location_details", f"{required_dimension},{filters_3[k]},{sort_by}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k == 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_location_details", f"{required_dimension},{filters_4[l]},{sort_by}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k == 0 and l == 0:
                            df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "playback_location_details", f"{required_dimension},{sort_by}.csv"))

        cols_to_swap = filters_4[1:] + filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if cols_to_swap[i] == "continent":
                col = df[f"{cols_to_swap[i]}"]
                new_col = []
                for row in col:
                    if row == row:
                        if row < 10:
                            new_col.append(continents_rev_dict["00" + str(int(row))])
                        elif row < 100:
                            new_col.append(continents_rev_dict["0" + str(int(row))])
                        else:
                            new_col.append(continents_rev_dict[str(int(row))])
                    else:
                        new_col.append(row)
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i]}", new_col)
            elif cols_to_swap[i] == "subContinent":
                col = df[f"{cols_to_swap[i]}"]
                new_col = []
                for row in col:
                    if row == row:
                        if row < 10:
                            new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                        elif row < 100:
                            new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                        else:
                            new_col.append(subcontinents_rev_dict[str(int(row))])
                    else:
                        new_col.append(row)
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
            else:
                col = df[f"{cols_to_swap[i]}"]
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i]}", col)
        df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "playback_location_details", f"{required_dimension},{','.join(cols_to_swap)},{sort_by}.csv"), index=False, na_rep="NULL")
        df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "playback_location_details", f"{required_dimension},{','.join(cols_to_swap)},{sort_by}.xlsx"), index=False, na_rep="NULL")


def clean_traffic_source(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents):
    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_provinces:
        filters_to_remove_1.append("province")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    required_dimension = "insightTrafficSourceType"
    dimensions = ["", "day"]
    filters_1 = ["", "country", "province", "continent", "subContinent"]
    filters_2 = ["", "video", "group"]
    filters_3 = ["", "liveOrOnDemand"]
    filters_4 = ["", "subscribedStatus"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)

    df = None
    to_merge = None
    for dimension in dimensions:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    for l in range(len(filters_4)):
                        if i != 0 and j != 0 and k != 0 and l != 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k != 0 and l == 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k == 0 and l != 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k == 0 and l == 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{filters_1[i]},{filters_2[j]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k != 0 and l != 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k != 0 and l == 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{dimension},{filters_1[i]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{filters_1[i]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k == 0 and l != 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{dimension},{filters_1[i]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{filters_1[i]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k == 0 and l == 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{dimension},{filters_1[i]}.csv"))
                                merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{filters_1[i]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k != 0 and l != 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k != 0 and l == 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k == 0 and l != 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{dimension},{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k == 0 and l == 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{dimension},{filters_2[j]}.csv"))
                                merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{filters_2[j]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k != 0 and l != 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{dimension},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k != 0 and l == 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{dimension},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{filters_3[k]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k == 0 and l != 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{dimension},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{filters_4[l]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k == 0 and l == 0:
                            if dimension == "day":
                                df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension},{dimension}.csv"))
                            else:
                                df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source", f"{required_dimension}.csv"))

        cols_to_swap = filters_4[1:] + filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if dimension == "day":
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i]}", col)
            else:
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", col)
        if dimension == "day":
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "traffic_source", f"{required_dimension},{dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "traffic_source", f"{required_dimension},{dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")
        else:
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "traffic_source", f"{required_dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "traffic_source", f"{required_dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")


def clean_traffic_source_details(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents):
    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_provinces:
        filters_to_remove_1.append("province")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    required_dimension = "insightTrafficSourceDetail"
    insight_types = ["ADVERTISING", "EXT_URL", "RELATED_VIDEO", "SUBSCRIBER", "YT_CHANNEL", "YT_OTHER_PAGE", "YT_SEARCH"]
    sorting_options = ["-views", "-estimatedMinutesWatched"]
    filters_1 = ["", "country", "province", "continent", "subContinent"]
    filters_2 = ["", "video", "group"]
    filters_3 = ["", "liveOrOnDemand"]
    filters_4 = ["", "subscribedStatus"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)
    
    df = None
    to_merge = None
    for sort_by in sorting_options:
        for insight_type in insight_types:
            for i in range(len(filters_1)):
                for j in range(len(filters_2)):
                    for k in range(len(filters_3)):
                        for l in range(len(filters_4)):
                            if i != 0 and j != 0 and k != 0 and l != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source_details", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]},{sort_by},{insight_type}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k != 0 and l == 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source_details", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{sort_by},{insight_type}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k == 0 and l != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source_details", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{sort_by},{insight_type}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k == 0 and l == 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source_details", f"{required_dimension},{filters_1[i]},{filters_2[j]},{sort_by},{insight_type}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k != 0 and l != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source_details", f"{required_dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]},{sort_by},{insight_type}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k != 0 and l == 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source_details", f"{required_dimension},{filters_1[i]},{filters_3[k]},{sort_by},{insight_type}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k == 0 and l != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source_details", f"{required_dimension},{filters_1[i]},{filters_4[l]},{sort_by},{insight_type}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k == 0 and l == 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source_details", f"{required_dimension},{filters_1[i]},{sort_by},{insight_type}.csv"))
                                merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k != 0 and l != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source_details", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{sort_by},{insight_type}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k != 0 and l == 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source_details", f"{required_dimension},{filters_2[j]},{filters_3[k]},{sort_by},{insight_type}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k == 0 and l != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source_details", f"{required_dimension},{filters_2[j]},{filters_4[l]},{sort_by},{insight_type}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k == 0 and l == 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source_details", f"{required_dimension},{filters_2[j]},{sort_by},{insight_type}.csv"))
                                merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k != 0 and l != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source_details", f"{required_dimension},{filters_3[k]},{filters_4[l]},{sort_by},{insight_type}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k != 0 and l == 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source_details", f"{required_dimension},{filters_3[k]},{sort_by},{insight_type}.csv"))
                                merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k == 0 and l != 0:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source_details", f"{required_dimension},{filters_4[l]},{sort_by},{insight_type}.csv"))
                                merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k == 0 and l == 0:
                                df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "traffic_source_details", f"{required_dimension},{sort_by},{insight_type}.csv"))

            cols_to_swap = filters_4[1:] + filters_3[1:] + filters_2[1:] + filters_1[1:]
            for i in range(len(cols_to_swap)):
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", col)
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "traffic_source_details", f"{required_dimension},{','.join(cols_to_swap)},{sort_by},{insight_type}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "traffic_source_details", f"{required_dimension},{','.join(cols_to_swap)},{sort_by},{insight_type}.xlsx"), index=False, na_rep="NULL")

def clean_device_type(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents):

    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_provinces:
        filters_to_remove_1.append("province")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    required_dimension = "deviceType"
    dimensions = ["", "day"]
    filters_1 = ["", "country", "province", "continent", "subContinent"]
    filters_2 = ["", "video", "group"]
    filters_3 = ["", "operatingSystem"]
    filters_4 = ["", "liveOrOnDemand"]
    filters_5 = ["", "subscribedStatus"]
    filters_6 = ["", "youtubeProduct"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)
    
    df = None
    to_merge = None
    for dimension in dimensions:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    for l in range(len(filters_4)):
                        for m in range(len(filters_5)):
                            for n in range(len(filters_6)):
                                if i != 0 and j != 0 and k != 0 and l != 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_3[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j != 0 and k != 0 and l != 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_3[j]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[j]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j != 0 and k != 0 and l == 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_3[j]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[j]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j != 0 and k != 0 and l == 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_3[j]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[j]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j != 0 and k == 0 and l != 0 and m != 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j != 0 and k == 0 and l != 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j != 0 and k == 0 and l != 0 and m == 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j != 0 and k == 0 and l != 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j != 0 and k == 0 and l == 0 and m != 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j != 0 and k == 0 and l == 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j != 0 and k == 0 and l == 0 and m == 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j != 0 and k == 0 and l == 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]},{filters_2[j]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j == 0 and k != 0 and l != 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j == 0 and k != 0 and l != 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j == 0 and k != 0 and l == 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_3[k]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]},{filters_3[k]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j == 0 and k != 0 and l == 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_3[k]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]},{filters_3[k]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j == 0 and k == 0 and l != 0 and m != 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_4[l]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]},{filters_4[l]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j == 0 and k == 0 and l != 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j == 0 and k == 0 and l != 0 and m == 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_4[l]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]},{filters_4[l]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j == 0 and k == 0 and l != 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j == 0 and k == 0 and l == 0 and m != 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j == 0 and k == 0 and l == 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j == 0 and k == 0 and l == 0 and m == 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j == 0 and k == 0 and l == 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_1[i]}.csv"))
                                        merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_1[i]}.csv"))
                                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k != 0 and l != 0 and m != 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k != 0 and l != 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k != 0 and l != 0 and m == 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k != 0 and l != 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k != 0 and l == 0 and m != 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k != 0 and l == 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k != 0 and l == 0 and m == 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k != 0 and l == 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_2[j]},{filters_3[k]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k == 0 and l != 0 and m != 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k == 0 and l != 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k == 0 and l != 0 and m == 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_2[j]},{filters_4[l]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_2[j]},{filters_4[l]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k == 0 and l != 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_2[j]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_2[j]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k == 0 and l == 0 and m != 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_2[j]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_2[j]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k == 0 and l == 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_2[j]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_2[j]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k == 0 and l == 0 and m == 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_2[j]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_2[j]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k == 0 and l == 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_2[j]}.csv"))
                                        merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_2[j]}.csv"))
                                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k != 0 and l != 0 and m != 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_3[k]},{filters_4[l]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_3[k]},{filters_4[l]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k != 0 and l != 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k != 0 and l != 0 and m == 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_3[k]},{filters_4[l]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_3[k]},{filters_4[l]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k != 0 and l != 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_3[k]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_3[k]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k != 0 and l == 0 and m != 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_3[k]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_3[k]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k != 0 and l == 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_3[k]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_3[k]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k != 0 and l == 0 and m == 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_3[k]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_3[k]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k != 0 and l == 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_3[k]}.csv"))
                                        merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_3[k]}.csv"))
                                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k == 0 and l != 0 and m != 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_4[l]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_4[l]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k == 0 and l != 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k == 0 and l != 0 and m == 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_4[l]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_4[l]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k == 0 and l != 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_4[l]}.csv"))
                                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k == 0 and l == 0 and m != 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k == 0 and l == 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_5[m]}.csv"))
                                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k == 0 and l == 0 and m == 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{filters_6[n]}.csv"))
                                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k == 0 and l == 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension},{dimension}.csv"))
                                    else:
                                        df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "device_type", f"{required_dimension}.csv"))

        cols_to_swap = filters_6[1:] + filters_5[1:] + filters_4[1:] + filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if dimension == "day":
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i]}", col)
            else:
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", col)
        if dimension == "day":
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "device_type", f"{dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "device_type", f"{dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")
        else:
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "device_type", f"{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "device_type", f"{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")


def clean_operating_system(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents):

    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_provinces:
        filters_to_remove_1.append("province")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    required_dimension = "operatingSystem"
    dimensions = ["", "day"]
    filters_1 = ["", "country", "province", "continent", "subContinent"]
    filters_2 = ["", "video", "group"]
    filters_3 = ["", "deviceType"]
    filters_4 = ["", "liveOrOnDemand"]
    filters_5 = ["", "subscribedStatus"]
    filters_6 = ["", "youtubeProduct"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)
    
    df = None
    to_merge = None
    for dimension in dimensions:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    for l in range(len(filters_4)):
                        for m in range(len(filters_5)):
                            for n in range(len(filters_6)):
                                if i != 0 and j != 0 and k != 0 and l != 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_3[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j != 0 and k != 0 and l != 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_3[j]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[j]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j != 0 and k != 0 and l == 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_3[j]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[j]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j != 0 and k != 0 and l == 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_3[j]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[j]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j != 0 and k == 0 and l != 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j != 0 and k == 0 and l != 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j != 0 and k == 0 and l == 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j != 0 and k == 0 and l == 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_1[i]},{filters_2[j]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j == 0 and k != 0 and l != 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j == 0 and k != 0 and l != 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j == 0 and k != 0 and l == 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_1[i]},{filters_3[k]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_1[i]},{filters_3[k]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j == 0 and k != 0 and l == 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_1[i]},{filters_3[k]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_1[i]},{filters_3[k]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j == 0 and k == 0 and l != 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_1[i]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_1[i]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j == 0 and k == 0 and l != 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_1[i]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_1[i]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j == 0 and k == 0 and l == 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_1[i]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_1[i]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i != 0 and j == 0 and k == 0 and l == 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_1[i]}.csv"))
                                        merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_1[i]}.csv"))
                                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k != 0 and l != 0 and m != 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k != 0 and l != 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k != 0 and l != 0 and m == 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k != 0 and l != 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k != 0 and l == 0 and m != 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k != 0 and l == 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k != 0 and l == 0 and m == 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k != 0 and l == 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_2[j]},{filters_3[k]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k == 0 and l != 0 and m != 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k == 0 and l != 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k == 0 and l != 0 and m == 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_2[j]},{filters_4[l]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_2[j]},{filters_4[l]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k == 0 and l != 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_2[j]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_2[j]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k == 0 and l == 0 and m != 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_2[j]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_2[j]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k == 0 and l == 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_2[j]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_2[j]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k == 0 and l == 0 and m == 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_2[j]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_2[j]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j != 0 and k == 0 and l == 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_2[j]}.csv"))
                                        merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_2[j]}.csv"))
                                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k != 0 and l != 0 and m != 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_3[k]},{filters_4[l]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_3[k]},{filters_4[l]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k != 0 and l != 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k != 0 and l != 0 and m == 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_3[k]},{filters_4[l]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_3[k]},{filters_4[l]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k != 0 and l != 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_3[k]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_3[k]},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k != 0 and l == 0 and m != 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_3[k]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_3[k]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k != 0 and l == 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_3[k]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_3[k]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k != 0 and l == 0 and m == 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_3[k]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_3[k]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k != 0 and l == 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_3[k]}.csv"))
                                        merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_3[k]}.csv"))
                                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k == 0 and l != 0 and m != 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_4[l]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_4[l]},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k == 0 and l != 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_4[l]},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k == 0 and l != 0 and m == 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_4[l]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_4[l]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k == 0 and l != 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_4[l]}.csv"))
                                        merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_4[l]}.csv"))
                                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k == 0 and l == 0 and m != 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_5[m]},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k == 0 and l == 0 and m != 0 and n == 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_5[m]}.csv"))
                                        merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_5[m]}.csv"))
                                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                elif i == 0 and j == 0 and k == 0 and l == 0 and m == 0 and n != 0:
                                    if dimension == "day":
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension},{filters_6[n]}.csv"))
                                        merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)
                                    else:
                                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{filters_6[n]}.csv"))
                                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                        df = df.merge(to_merge, how="outer", on=merge_on)

                                elif i == 0 and j == 0 and k == 0 and l == 0 and m == 0 and n == 0:
                                    if dimension == "day":
                                        df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension},{dimension}.csv"))
                                    else:
                                        df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system", f"{required_dimension}.csv"))


        cols_to_swap = filters_6[1:] + filters_5[1:] + filters_4[1:] + filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if dimension == "day":
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i]}", col)
            else:
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", col)
        if dimension == "day":
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "operating_system", f"{dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "operating_system", f"{dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")
        else:
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "operating_system", f"{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "operating_system", f"{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_operating_system_and_device_type(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents):

    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_provinces:
        filters_to_remove_1.append("province")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    required_dimension = "deviceType,operatingSystem"
    dimensions = ["", "day"]
    filters_1 = ["", "country", "province", "continent", "subContinent"]
    filters_2 = ["", "video", "group"]
    filters_3 = ["", "liveOrOnDemand"]
    filters_4 = ["", "subscribedStatus"]
    filters_5 = ["", "youtubeProduct"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)

    df = None
    to_merge = None
    for dimension in dimensions:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    for l in range(len(filters_4)):
                        for m in range(len(filters_5)):
                            if i != 0 and j != 0 and k != 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k != 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k == 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k == 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{filters_1[i]},{filters_2[j]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k != 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k != 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{filters_1[i]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k == 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension},{filters_1[i]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{filters_1[i]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k == 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension},{filters_1[i]}.csv"))
                                    merge_on = list(to_merge.columns)[:3] + list(to_merge.columns)[4:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{filters_1[i]}.csv"))
                                    merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k != 0 and l != 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k != 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k != 0 and l == 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k != 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{filters_2[j]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k == 0 and l != 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k == 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension},{filters_2[j]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{filters_2[j]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k == 0 and l == 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension},{filters_2[j]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{filters_2[j]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k == 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension},{filters_2[j]}.csv"))
                                    merge_on = list(to_merge.columns)[:3] + list(to_merge.columns)[4:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{filters_2[j]}.csv"))
                                    merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k != 0 and l != 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k != 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k != 0 and l == 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension},{filters_3[k]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{filters_3[k]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k != 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)[:3] + list(to_merge.columns)[4:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k == 0 and l != 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k == 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)[:3] + list(to_merge.columns)[4:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k == 0 and l == 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)[:3] + list(to_merge.columns)[4:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k == 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension},{dimension}.csv"))
                                else:
                                    df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "operating_system_and_device_type", f"{required_dimension}.csv"))

        cols_to_swap = filters_5[1:] + filters_4[1:] + filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if dimension == "day":
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+3, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+3, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+3, f"{cols_to_swap[i]}", col)
            else:
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i]}", col)
        if dimension == "day":
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "operating_system_and_device_type", f"{dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "operating_system_and_device_type", f"{dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")
        else:
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "operating_system_and_device_type", f"{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "operating_system_and_device_type", f"{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_viewer_demographics(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents):

    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_provinces:
        filters_to_remove_1.append("province")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    dimensions = ["ageGroup", "gender", "ageGroup,gender"]
    filters_1 = ["", "country", "province", "continent", "subContinent"]
    filters_2 = ["", "video", "group"]
    filters_3 = ["", "liveOrOnDemand"]
    filters_4 = ["", "subscribedStatus"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)
    
    df = None
    to_merge = None
    for dimension in dimensions:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    for l in range(len(filters_4)):
                        if i != 0 and j != 0 and k != 0 and l != 0:
                            if dimension == "ageGroup,gender":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k != 0 and l == 0:
                            if dimension == "ageGroup,gender":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k == 0 and l != 0:
                            if dimension == "ageGroup,gender":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k == 0 and l == 0:
                            if dimension == "ageGroup,gender":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k != 0 and l != 0:
                            if dimension == "ageGroup,gender":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k != 0 and l == 0:
                            if dimension == "ageGroup,gender":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_1[i]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_1[i]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k == 0 and l != 0:
                            if dimension == "ageGroup,gender":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_1[i]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_1[i]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k == 0 and l == 0:
                            if dimension == "ageGroup,gender":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_1[i]}.csv"))
                                merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_1[i]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k != 0 and l != 0:
                            if dimension == "ageGroup,gender":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k != 0 and l == 0:
                            if dimension == "ageGroup,gender":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k == 0 and l != 0:
                            if dimension == "ageGroup,gender":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k == 0 and l == 0:
                            if dimension == "ageGroup,gender":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_2[j]}.csv"))
                                merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_2[j]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k != 0 and l != 0:
                            if dimension == "ageGroup,gender":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k != 0 and l == 0:
                            if dimension == "ageGroup,gender":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_3[k]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k == 0 and l != 0:
                            if dimension == "ageGroup,gender":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension},{filters_4[l]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k == 0 and l == 0:
                            if dimension == "ageGroup,gender":
                                df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension}.csv"))
                            else:
                                df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "viewer_demographics", f"{dimension}.csv"))

        cols_to_swap = filters_4[1:] + filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if dimension == "ageGroup,gender":
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i]}", col)
            else:
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", col)
        if dimension == "ageGroup,gender":
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "viewer_demographics", f"{dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "viewer_demographics", f"{dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")
        else:
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "viewer_demographics", f"{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "viewer_demographics", f"{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_engagement_and_content_sharing(channel_name, date, has_groups, has_countries, has_continents, has_subcontinents):

    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    required_dimension = "sharingService"
    filters_1 = ["", "country", "continent", "subContinent"]
    filters_2 = ["", "video", "group"]
    filters_3 = ["", "subscribedStatus"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)
   
    df = None
    to_merge = None
    for i in range(len(filters_1)):
        for j in range(len(filters_2)):
            for k in range(len(filters_3)):
                if i != 0 and j != 0 and k != 0:
                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "engagement_and_content_sharing", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                    merge_on = list(to_merge.columns)
                    df = df.merge(to_merge, how="outer", on=merge_on)
                elif i != 0 and j != 0 and k == 0:
                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "engagement_and_content_sharing", f"{required_dimension},{filters_1[i]},{filters_2[j]}.csv"))
                    merge_on = list(to_merge.columns)
                    df = df.merge(to_merge, how="outer", on=merge_on)
                elif i != 0 and j == 0 and k != 0:
                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "engagement_and_content_sharing", f"{required_dimension},{filters_1[i]},{filters_3[k]}.csv"))
                    merge_on = list(to_merge.columns)
                    df = df.merge(to_merge, how="outer", on=merge_on)
                elif i != 0 and j == 0 and k == 0:
                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "engagement_and_content_sharing", f"{required_dimension},{filters_1[i]}.csv"))
                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                    df = df.merge(to_merge, how="outer", on=merge_on)
                elif i == 0 and j != 0 and k != 0:
                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "engagement_and_content_sharing", f"{required_dimension},{filters_2[j]},{filters_3[k]}.csv"))
                    merge_on = list(to_merge.columns)
                    df = df.merge(to_merge, how="outer", on=merge_on)
                elif i == 0 and j != 0 and k == 0:
                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "engagement_and_content_sharing", f"{required_dimension},{filters_2[j]}.csv"))
                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                    df = df.merge(to_merge, how="outer", on=merge_on)
                elif i == 0 and j == 0 and k != 0:
                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "engagement_and_content_sharing", f"{required_dimension},{filters_3[k]}.csv"))
                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                    df = df.merge(to_merge, how="outer", on=merge_on)
                elif i == 0 and j == 0 and k == 0:
                    df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "engagement_and_content_sharing", f"{required_dimension}.csv"))

    cols_to_swap = filters_3[1:] + filters_2[1:] + filters_1[1:]
    for i in range(len(cols_to_swap)):
        if cols_to_swap[i] == "continent":
            col = df[f"{cols_to_swap[i]}"]
            new_col = []
            for row in col:
                if row == row:
                    if row < 10:
                        new_col.append(continents_rev_dict["00" + str(int(row))])
                    elif row < 100:
                        new_col.append(continents_rev_dict["0" + str(int(row))])
                    else:
                        new_col.append(continents_rev_dict[str(int(row))])
                else:
                    new_col.append(row)
            df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
            df.insert(i+1, f"{cols_to_swap[i]}", new_col)
        elif cols_to_swap[i] == "subContinent":
            col = df[f"{cols_to_swap[i]}"]
            new_col = []
            for row in col:
                if row == row:
                    if row < 10:
                        new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                    elif row < 100:
                        new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                    else:
                        new_col.append(subcontinents_rev_dict[str(int(row))])
                else:
                    new_col.append(row)
            df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
            df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
        else:
            col = df[f"{cols_to_swap[i]}"]
            df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
            df.insert(i+1, f"{cols_to_swap[i]}", col)
    df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "engagement_and_content_sharing", f"{required_dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
    df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "engagement_and_content_sharing", f"{required_dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_audience_retention(channel_name, date):

    vid_ids = ['BUHjF81OqKE', 'RzfTVTAz0MM', 'HsU-3vdKC9A', 'K4L_6RinZec', 'mQ7iFmzg4vs', 'wDAefGS4B2Q', 'NaczzJYUCQc', '5by1jF54fEo', 'RbUn1WY3UYE', 'xzn1JjU-0G0', 'egC4D94WBR0', 'wCcV9h_91SA', '80onzM604aw', 'XMS9dRzRdeU', '549LM6U1yfs', 'goMuir1zJOM', 'SV_eqPj3HyQ', '5OpcECI2ZH4', 'UPVuzN48dHQ', 'MignYuClO4A', 'SktC4aU2_L8', 'PAKKJSZjY34', 'QMIadtys-rg', 'gMjq09lP2Dc', '44mfjROTX6w', 'aaURBmEvvyU', '-2j_mud7JPE', 'RtgJn2kn3WM', '9zk2Gi7IVps', '5pbDN9uW9ZU', 'RJPUg-XqQS0', '-zAbOcb4Pns', 'CEH4E6lEsgI', '7V5ZHKqYCJI', 'Pp9bdeK5LFI', 'm2qCaSAE4Kk', 'vR0uf0nK50s', 'sfF8BzoUVGw', 'twZArnTFKvA', '-o9Naxpk0cg', 'k-hNC9Dl9OQ', 'tNW2Y10a2II', 'btDDSg_n65A', 'rdC3eFn1osI', 'MTp5w7zmrRA', '4Ecp_xiU6Gc', 'KTfvWxESi-0', 'GJApEApVzNo', 'oZZoi1b0NWc', 'p3tyqqfKcfs', 'e8Z1vVr-5oo', 'vXd5--xqnVU', 'b9zTV-pIGUs', 'OMdpHaLLXDs', 'hThr9qf-5oo', 'IAXUo7xXFtw', 'XyLdk9C8TA0', 'oS-zGAHQGmE', 'Cann_az2T8E']
    
    required_dimension = "elapsedVideoTimeRatio"
    filters_1 = ["", "audienceType"]
    filters_2 = ["", "subscribedStatus"]
    filters_3 = ["", "youtubeProduct"]
    
    df = None
    to_merge = None
    for vid_id in vid_ids:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    if i != 0 and j != 0 and k != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "audience_retention", f"{required_dimension},{vid_id},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j != 0 and k == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "audience_retention", f"{required_dimension},{vid_id},{filters_1[i]},{filters_2[j]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "audience_retention", f"{required_dimension},{vid_id},{filters_1[i]},{filters_3[k]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "audience_retention", f"{required_dimension},{vid_id},{filters_1[i]}.csv"))
                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "audience_retention", f"{required_dimension},{vid_id},{filters_2[j]},{filters_3[k]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "audience_retention", f"{required_dimension},{vid_id},{filters_2[j]}.csv"))
                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "audience_retention", f"{required_dimension},{vid_id},{filters_3[k]}.csv"))
                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k == 0:
                        df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "audience_retention", f"{required_dimension},{vid_id}.csv"))

        cols_to_swap = filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            col = df[f"{cols_to_swap[i]}"]
            df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
            df.insert(i+1, f"{cols_to_swap[i]}", col)
        df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "audience_retention", f"{required_dimension},{vid_id},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
        df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "audience_retention", f"{required_dimension},{vid_id},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_top_videos_regional(channel_name, date, has_countries, has_continents, has_subcontinents):

    filters_to_remove = []
    if not has_countries:
        filters_to_remove.append("country")
    if not has_continents:
        filters_to_remove.append("continent")
    if not has_subcontinents:
        filters_to_remove.append("subContinent")

    sorting_options = ["-views", "-redViews", "-estimatedMinutesWatched", "-estimatedRedMinutesWatched", "-subscribersGained", "-subscribersLost"]
    required_dimension = "video"
    filters = ["", "country", "continent", "subContinent"]

    for fil_to_rem in filters_to_remove:
        filters.remove(fil_to_rem)
    
    df = None
    to_merge = None
    for sort_by in sorting_options:
        for i in range(len(filters)):
            if i != 0:
                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_regional", f"{required_dimension},{filters[i]},{sort_by}.csv"))
                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                df = df.merge(to_merge, how="outer", on=merge_on)
            elif i == 0:
                df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_regional", f"{required_dimension},{sort_by}.csv"))

        cols_to_swap = filters[1:]
        for i in range(len(cols_to_swap)):
            if cols_to_swap[i] == "continent":
                col = df[f"{cols_to_swap[i]}"]
                new_col = []
                for row in col:
                    if row == row:
                        if row < 10:
                            new_col.append(continents_rev_dict["00" + str(int(row))])
                        elif row < 100:
                            new_col.append(continents_rev_dict["0" + str(int(row))])
                        else:
                            new_col.append(continents_rev_dict[str(int(row))])
                    else:
                        new_col.append(row)
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i]}", new_col)
            elif cols_to_swap[i] == "subContinent":
                col = df[f"{cols_to_swap[i]}"]
                new_col = []
                for row in col:
                    if row == row:
                        if row < 10:
                            new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                        elif row < 100:
                            new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                        else:
                            new_col.append(subcontinents_rev_dict[str(int(row))])
                    else:
                        new_col.append(row)
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
            else:
                col = df[f"{cols_to_swap[i]}"]
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i]}", col)
        df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "top_videos_regional", f"{required_dimension},{','.join(cols_to_swap)},{sort_by}.csv"), index=False, na_rep="NULL")
        df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "top_videos_regional", f"{required_dimension},{','.join(cols_to_swap)},{sort_by}.xlsx"), index=False, na_rep="NULL")

def clean_top_videos_in_US(channel_name, date, has_provinces):

    if not has_provinces:
        return

    sorting_options = ["-views", "-redViews", "-estimatedMinutesWatched", "-estimatedRedMinutesWatched"]
    required_dimension = "video"
    required_filter = "province"
    filters = ["", "subscribedStatus"]

    df = None
    to_merge = None
    for sort_by in sorting_options:
        for i in range(len(filters)):
            if i != 0:
                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_in_US", f"{required_dimension},{required_filter},{filters[i]},{sort_by}.csv"))
                merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                df = df.merge(to_merge, how="outer", on=merge_on)
            elif i == 0:
                df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_in_US", f"{required_dimension},{required_filter},{sort_by}.csv"))

        cols_to_swap = filters[1:]
        for i in range(len(cols_to_swap)):
            col = df[f"{cols_to_swap[i]}"]
            df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
            df.insert(i+1, f"{cols_to_swap[i]}", col)
        df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "top_videos_in_US", f"{required_dimension},{','.join(cols_to_swap)},{sort_by}.csv"), index=False, na_rep="NULL")
        df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "top_videos_in_US", f"{required_dimension},{','.join(cols_to_swap)},{sort_by}.xlsx"), index=False, na_rep="NULL")

def clean_top_videos_by_subscriber_type(channel_name, date, has_countries, has_continents, has_subcontinents):
    
        filters_to_remove = []
        if not has_countries:
            filters_to_remove.append("country")
        if not has_continents:
            filters_to_remove.append("continent")
        if not has_subcontinents:
            filters_to_remove.append("subContinent")

        sorting_options = ["-views", "-redViews", "-estimatedMinutesWatched", "-estimatedRedMinutesWatched"]
        required_dimension = "video"
        sub_status = ["SUBSCRIBED", "UNSUBSCRIBED"]
        filters_1 = ["", "country", "continent", "subContinent"]
        filters_2 = ["", "subscribedStatus"]

        for fil_to_rem in filters_to_remove:
            filters_1.remove(fil_to_rem)

        df = None
        to_merge = None
        for sort_by in sorting_options:
            for i in range(len(filters_1)):
                for j in range(len(filters_2)):
                    if i != 0 and j != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_subscriber_type", f"{required_dimension},{filters_1[i]},{filters_2[j]},{sort_by}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_subscriber_type", f"{required_dimension},{filters_1[i]},{sort_by}.csv"))
                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_subscriber_type", f"{required_dimension},{filters_2[j]},{sort_by}.csv"))
                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0:
                        df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_subscriber_type", f"{required_dimension},{sort_by}.csv"))
            
            cols_to_swap = filters_2[1:] + filters_1[1:]
            for i in range(len(cols_to_swap)):
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", col)
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "top_videos_by_subscriber_type", f"{required_dimension},{','.join(cols_to_swap)},{sort_by}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "top_videos_by_subscriber_type", f"{required_dimension},{','.join(cols_to_swap)},{sort_by}.xlsx"), index=False, na_rep="NULL")

def clean_top_videos_by_yt_product(channel_name, date, has_countries, has_provinces, has_continents, has_subcontinents):

    filters_to_remove = []
    if not has_countries:
        filters_to_remove.append("country")
    if not has_provinces:
        filters_to_remove.append("province")
    if not has_continents:
        filters_to_remove.append("continent")
    if not has_subcontinents:
        filters_to_remove.append("subContinent")

    sorting_options = ["-views", "-redViews", "-estimatedMinutesWatched", "-estimatedRedMinutesWatched"]
    required_dimension = "video" 
    filters_1 = ["", "country", "province", "continent", "subContinent"]
    filters_2 = ["", "subscribedStatus"]
    filters_3 = ["", "youtubeProduct"]

    for fil_to_rem in filters_to_remove:
        filters_1.remove(fil_to_rem)

    df = None
    to_merge = None
    for sort_by in sorting_options:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    if i != 0 and j != 0 and k != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{sort_by}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j != 0 and k == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_2[j]},{sort_by}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{filters_3[k]},{sort_by}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_1[i]},{sort_by}.csv"))
                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_2[j]},{filters_3[k]},{sort_by}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_2[j]},{sort_by}.csv"))
                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{filters_3[k]},{sort_by}.csv"))
                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k == 0:
                        df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{sort_by}.csv"))

        cols_to_swap = filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if cols_to_swap[i] == "continent":
                col = df[f"{cols_to_swap[i]}"]
                new_col = []
                for row in col:
                    if row == row:
                        if row < 10:
                            new_col.append(continents_rev_dict["00" + str(int(row))])
                        elif row < 100:
                            new_col.append(continents_rev_dict["0" + str(int(row))])
                        else:
                            new_col.append(continents_rev_dict[str(int(row))])
                    else:
                        new_col.append(row)
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i]}", new_col)
            elif cols_to_swap[i] == "subContinent":
                col = df[f"{cols_to_swap[i]}"]
                new_col = []
                for row in col:
                    if row == row:
                        if row < 10:
                            new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                        elif row < 100:
                            new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                        else:
                            new_col.append(subcontinents_rev_dict[str(int(row))])
                    else:
                        new_col.append(row)
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
            else:
                col = df[f"{cols_to_swap[i]}"]
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i]}", col)
        df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "top_videos_by_yt_product", f"{required_dimension},{','.join(cols_to_swap)},{sort_by}.csv"), index=False, na_rep="NULL")
        df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "top_videos_by_yt_product", f"{required_dimension},{','.join(cols_to_swap)},{sort_by}.xlsx"), index=False, na_rep="NULL")

def clean_top_videos_by_playback_details(channel_name, date, has_countries, has_provinces, has_continents, has_subcontinents):

    filters_to_remove = []
    if not has_countries:
        filters_to_remove.append("country")
    if not has_provinces:
        filters_to_remove.append("province")
    if not has_continents:
        filters_to_remove.append("continent")
    if not has_subcontinents:
        filters_to_remove.append("subContinent")

    sorting_options = ["-views", "-redViews", "-estimatedMinutesWatched", "-estimatedRedMinutesWatched"]
    required_dimension = "video"
    filters_1 = ["", "country", "province", "continent", "subContinent"]
    filters_2 = ["", "liveOrOnDemand"]
    filters_3 = ["", "subscribedStatus"]
    filters_4 = ["", "youtubeProduct"]

    for fil_to_rem in filters_to_remove:
        filters_1.remove(fil_to_rem)
    
    df = None
    to_merge = None
    for sort_by in sorting_options:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    for l in range(len(filters_4)):
                        if i != 0 and j != 0 and k != 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_playback_details", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k != 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_playback_details", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k == 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_playback_details", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k == 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_playback_details", f"{required_dimension},{filters_1[i]},{filters_2[j]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k != 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_playback_details", f"{required_dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k != 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_playback_details", f"{required_dimension},{filters_1[i]},{filters_3[k]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k == 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_playback_details", f"{required_dimension},{filters_1[i]},{filters_4[l]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k == 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_playback_details", f"{required_dimension},{filters_1[i]},{sort_by}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k != 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_playback_details", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k != 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_playback_details", f"{required_dimension},{filters_2[j]},{filters_3[k]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k == 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_playback_details", f"{required_dimension},{filters_2[j]},{filters_4[l]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k == 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_playback_details", f"{required_dimension},{filters_2[j]},{sort_by}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k != 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_playback_details", f"{required_dimension},{filters_3[k]},{filters_4[l]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k != 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_playback_details", f"{required_dimension},{filters_3[k]},{sort_by}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k == 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_playback_details", f"{required_dimension},{filters_4[l]},{sort_by}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k == 0 and l == 0:
                            df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "video_reports", "csv", "top_videos_by_playback_details", f"{required_dimension},{sort_by}.csv"))

        cols_to_swap = filters_4[1:] + filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if cols_to_swap[i] == "continent":
                col = df[f"{cols_to_swap[i]}"]
                new_col = []
                for row in col:
                    if row == row:
                        if row < 10:
                            new_col.append(continents_rev_dict["00" + str(int(row))])
                        elif row < 100:
                            new_col.append(continents_rev_dict["0" + str(int(row))])
                        else:
                            new_col.append(continents_rev_dict[str(int(row))])
                    else:
                        new_col.append(row)
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i]}", new_col)
            elif cols_to_swap[i] == "subContinent":
                col = df[f"{cols_to_swap[i]}"]
                new_col = []
                for row in col:
                    if row == row:
                        if row < 10:
                            new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                        elif row < 100:
                            new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                        else:
                            new_col.append(subcontinents_rev_dict[str(int(row))])
                    else:
                        new_col.append(row)
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
            else:
                col = df[f"{cols_to_swap[i]}"]
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i]}", col)
        df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "csv", "top_videos_by_playback_details", f"{required_dimension},{','.join(cols_to_swap)},{sort_by}.csv"), index=False, na_rep="NULL")
        df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "video_reports", "excel", "top_videos_by_playback_details", f"{required_dimension},{','.join(cols_to_swap)},{sort_by}.xlsx"), index=False, na_rep="NULL")

def clean_basic_stats_playlist(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents):

    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_provinces:
        filters_to_remove_1.append("province")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    required_filter = "isCurated==1"
    filters_1 = ["", "country", "province", "continent", "subContinent"]
    filters_2 = ["", "playlist", "group"]
    filters_3 = ["", "subscribedStatus"]
    filters_4 = ["", "youtubeProduct"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)
   
    df = None
    to_merge = None
    for i in range(len(filters_1)):
        for j in range(len(filters_2)):
            for k in range(len(filters_3)):
                for l in range(len(filters_4)):
                    if i != 0 and j != 0 and k != 0 and l != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "basic_stats_playlist", f"{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j != 0 and k != 0 and l == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "basic_stats_playlist", f"{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j != 0 and k == 0 and l != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "basic_stats_playlist", f"{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j != 0 and k == 0 and l == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "basic_stats_playlist", f"{filters_1[i]},{filters_2[j]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k != 0 and l != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "basic_stats_playlist", f"{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k != 0 and l == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "basic_stats_playlist", f"{filters_1[i]},{filters_3[k]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k == 0 and l != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "basic_stats_playlist", f"{filters_1[i]},{filters_4[l]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k == 0 and l == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "basic_stats_playlist", f"{filters_1[i]}.csv"))
                        merge_on = list(to_merge.columns)[1:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k != 0 and l != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "basic_stats_playlist", f"{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k != 0 and l == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "basic_stats_playlist", f"{filters_2[j]},{filters_3[k]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k == 0 and l != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "basic_stats_playlist", f"{filters_2[j]},{filters_4[l]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k == 0 and l == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "basic_stats_playlist", f"{filters_2[j]}.csv"))
                        merge_on = list(to_merge.columns)[1:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k != 0 and l != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "basic_stats_playlist", f"{filters_3[k]},{filters_4[l]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k != 0 and l == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "basic_stats_playlist", f"{filters_3[k]}.csv"))
                        merge_on = list(to_merge.columns)[1:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k == 0 and l != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "basic_stats_playlist", f"{filters_4[l]}.csv"))
                        merge_on = list(to_merge.columns)[1:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k == 0 and l == 0:
                        df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "basic_stats_playlist", "unfiltered.csv"))

    cols_to_swap = filters_4[1:] + filters_3[1:] + filters_2[1:] + filters_1[1:]
    for i in range(len(cols_to_swap)):
        if cols_to_swap[i] == "continent":
            col = df[f"{cols_to_swap[i]}"]
            new_col = []
            for row in col:
                if row == row:
                    if row < 10:
                        new_col.append(continents_rev_dict["00" + str(int(row))])
                    elif row < 100:
                        new_col.append(continents_rev_dict["0" + str(int(row))])
                    else:
                        new_col.append(continents_rev_dict[str(int(row))])
                else:
                    new_col.append(row)
            df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
            df.insert(i, f"{cols_to_swap[i]}", new_col)
        elif cols_to_swap[i] == "subContinent":
            col = df[f"{cols_to_swap[i]}"]
            new_col = []
            for row in col:
                if row == row:
                    if row < 10:
                        new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                    elif row < 100:
                        new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                    else:
                        new_col.append(subcontinents_rev_dict[str(int(row))])
                else:
                    new_col.append(row)
            df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
            df.insert(i, f"{cols_to_swap[i].lower()}", new_col)
        else:
            col = df[f"{cols_to_swap[i]}"]
            df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
            df.insert(i, f"{cols_to_swap[i]}", col)
    df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "csv", "basic_stats_playlist", f"{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
    df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "excel", "basic_stats_playlist", f"{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")


def clean_time_based_playlist(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents):

    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_provinces:
        filters_to_remove_1.append("province")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    required_filter = "isCurated==1"
    dimensions = ["", "day", "month"]
    filters_1 = ["", "country", "province", "continent", "subContinent"]
    filters_2 = ["", "playlist", "group"]
    filters_3 = ["", "subscribedStatus"]
    filters_4 = ["", "youtubeProduct"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)
    
    df = None
    to_merge = None
    for dimension in dimensions:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    for l in range(len(filters_4)):
                        if i != 0 and j != 0 and k != 0 and l != 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k != 0 and l == 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k == 0 and l != 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k == 0 and l == 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{filters_1[i]},{filters_2[j]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k != 0 and l != 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k != 0 and l == 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{dimension},{filters_1[i]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{filters_1[i]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k == 0 and l != 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{dimension},{filters_1[i]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{filters_1[i]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k == 0 and l == 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{dimension},{filters_1[i]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{filters_1[i]}.csv"))
                                merge_on = list(to_merge.columns)[1:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k != 0 and l != 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k != 0 and l == 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{dimension},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k == 0 and l != 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{dimension},{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k == 0 and l == 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{dimension},{filters_2[j]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{filters_2[j]}.csv"))
                                merge_on = list(to_merge.columns)[1:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k != 0 and l != 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{dimension},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k != 0 and l == 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{dimension},{filters_3[k]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)[1:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k == 0 and l != 0:
                            if dimension == "day" or dimension == "month":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{dimension},{filters_4[l]}.csv"))
                                merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)[1:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k == 0 and l == 0:
                            if dimension == "day" or dimension == "month":
                                df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", f"{dimension}.csv"))
                            else:
                                df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "time_based_playlist", "unfiltered.csv"))

        cols_to_swap = filters_4[1:] + filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if dimension == "day" or dimension == "month":
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", col)
            else:
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i, f"{cols_to_swap[i]}", col)
        if dimension == "day" or dimension == "month":
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "csv", "time_based_playlist", f"{dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "excel", "time_based_playlist", f"{dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")
        else:
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "csv", "time_based_playlist", f"{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "excel", "time_based_playlist", f"{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_activity_by_location_playlist(channel_name, date, has_groups, has_countries, has_continents, has_subcontinents):

    if not has_countries:
        return

    required_dimension = "country"
    required_filter = "isCurated==1"
    sub_status = ["SUBSCRIBED", "UNSUBSCRIBED"]
    yt_status = ["CORE", "GAMING", "KIDS", "MUSIC"]

    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    filters_1 = ["", "continent", "subContinent"]
    filters_2 = ["", "playlist", "group"]
    filters_3 = ["", "subscribedStatus"]
    filters_4 = ["", "youtubeProduct"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)
    
    df = None
    to_merge = None
    for i in range(len(filters_1)):
        for j in range(len(filters_2)):
            for k in range(len(filters_3)):
                for l in range(len(filters_4)):
                    if i != 0 and j != 0 and k != 0 and l != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_by_location_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j != 0 and k != 0 and l == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_by_location_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j != 0 and k == 0 and l != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_by_location_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j != 0 and k == 0 and l == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_by_location_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k != 0 and l != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_by_location_playlist", f"{required_dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k != 0 and l == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_by_location_playlist", f"{required_dimension},{filters_1[i]},{filters_3[k]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k == 0 and l != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_by_location_playlist", f"{required_dimension},{filters_1[i]},{filters_4[l]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k == 0 and l == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_by_location_playlist", f"{required_dimension},{filters_1[i]}.csv"))
                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k != 0 and l != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_by_location_playlist", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k != 0 and l == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_by_location_playlist", f"{required_dimension},{filters_2[j]},{filters_3[k]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k == 0 and l != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_by_location_playlist", f"{required_dimension},{filters_2[j]},{filters_4[l]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k == 0 and l == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_by_location_playlist", f"{required_dimension},{filters_2[j]}.csv"))
                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k != 0 and l != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_by_location_playlist", f"{required_dimension},{filters_3[k]},{filters_4[l]}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k != 0 and l == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_by_location_playlist", f"{required_dimension},{filters_3[k]}.csv"))
                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k == 0 and l != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_by_location_playlist", f"{required_dimension},{filters_4[l]}.csv"))
                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k == 0 and l == 0:
                        df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_by_location_playlist", f"{required_dimension}.csv"))

    cols_to_swap = filters_4[1:] + filters_3[1:] + filters_2[1:] + filters_1[1:]
    for i in range(len(cols_to_swap)):
        if cols_to_swap[i] == "continent":
            col = df[f"{cols_to_swap[i]}"]
            new_col = []
            for row in col:
                if row == row:
                    if row < 10:
                        new_col.append(continents_rev_dict["00" + str(int(row))])
                    elif row < 100:
                        new_col.append(continents_rev_dict["0" + str(int(row))])
                    else:
                        new_col.append(continents_rev_dict[str(int(row))])
                else:
                    new_col.append(row)
            df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
            df.insert(i+2, f"{cols_to_swap[i]}", new_col)
        elif cols_to_swap[i] == "subContinent":
            col = df[f"{cols_to_swap[i]}"]
            new_col = []
            for row in col:
                if row == row:
                    if row < 10:
                        new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                    elif row < 100:
                        new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                    else:
                        new_col.append(subcontinents_rev_dict[str(int(row))])
                else:
                    new_col.append(row)
            df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
            df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
        else:
            col = df[f"{cols_to_swap[i]}"]
            df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
            df.insert(i+2, f"{cols_to_swap[i]}", col)
    df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "csv", "activity_by_location_playlist", f"{required_dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
    df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "excel", "activity_by_location_playlist", f"{required_dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_activity_in_US_playlist(channel_name, date, has_groups, has_provinces):
   
    if not has_provinces:
        return

    filters_to_remove = []
    if not has_groups:
        filters_to_remove.append("group")

    required_dimension = "province"
    required_filter = "isCurated==1;country==US"
    filters_1 = ["", "playlist", "group"]
    filters_2 = ["", "subscribedStatus"]
    filters_3 = ["", "youtubeProduct"]

    for fil_to_rem in filters_to_remove:
        filters_1.remove(fil_to_rem)
    
    df = None
    to_merge = None
    for i in range(len(filters_1)):
        for j in range(len(filters_2)):
            for k in range(len(filters_3)):
                if i != 0 and j != 0 and k != 0:
                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_in_US_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                    merge_on = list(to_merge.columns)
                    df = df.merge(to_merge, how="outer", on=merge_on)
                elif i != 0 and j != 0 and k == 0:
                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_in_US_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]}.csv"))
                    merge_on = list(to_merge.columns)
                    df = df.merge(to_merge, how="outer", on=merge_on)
                elif i != 0 and j == 0 and k != 0:
                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_in_US_playlist", f"{required_dimension},{filters_1[i]},{filters_3[k]}.csv"))
                    merge_on = list(to_merge.columns)
                    df = df.merge(to_merge, how="outer", on=merge_on)
                elif i != 0 and j == 0 and k == 0:
                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_in_US_playlist", f"{required_dimension},{filters_1[i]}.csv"))
                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                    df = df.merge(to_merge, how="outer", on=merge_on)
                elif i == 0 and j != 0 and k != 0:
                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_in_US_playlist", f"{required_dimension},{filters_2[j]},{filters_3[k]}.csv"))
                    merge_on = list(to_merge.columns)
                    df = df.merge(to_merge, how="outer", on=merge_on)
                elif i == 0 and j != 0 and k == 0:
                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_in_US_playlist", f"{required_dimension},{filters_2[j]}.csv"))
                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                    df = df.merge(to_merge, how="outer", on=merge_on)
                elif i == 0 and j == 0 and k != 0:
                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_in_US_playlist", f"{required_dimension},{filters_3[k]}.csv"))
                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                    df = df.merge(to_merge, how="outer", on=merge_on)
                elif i == 0 and j == 0 and k == 0:
                    df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "activity_in_US_playlist", f"{required_dimension}.csv"))

    cols_to_swap = filters_3[1:] + filters_2[1:] + filters_1[1:]
    for i in range(len(cols_to_swap)):
        col = df[f"{cols_to_swap[i]}"]
        df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
        df.insert(i+1, f"{cols_to_swap[i]}", col)
    df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "csv", "activity_in_US_playlist", f"{required_dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
    df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "excel", "activity_in_US_playlist", f"{required_dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_playback_locations_playlist(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents):

    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_provinces:
        filters_to_remove_1.append("province")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    required_dimension = "insightPlaybackLocationType"
    required_filter = "isCurated==1"
    dimensions = ["", "day"]
    filters_1 = ["", "country", "province", "continent", "subContinent"]
    filters_2 = ["", "playlist", "group"]
    filters_3 = ["", "subscribedStatus"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)
    
    df = None
    to_merge = None
    for dimension in dimensions:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    if i != 0 and j != 0 and k != 0:
                        if dimension == "day":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j != 0 and k == 0:
                        if dimension == "day":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k != 0:
                        if dimension == "day":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_playlist", f"{required_dimension},{filters_1[i]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k == 0:
                        if dimension == "day":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_playlist", f"{required_dimension},{dimension},{filters_1[i]}.csv"))
                            merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_playlist", f"{required_dimension},{filters_1[i]}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k != 0:
                        if dimension == "day":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_playlist", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_playlist", f"{required_dimension},{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k == 0:
                        if dimension == "day":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_playlist", f"{required_dimension},{dimension},{filters_2[j]}.csv"))
                            merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_playlist", f"{required_dimension},{filters_2[j]}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k != 0:
                        if dimension == "day":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_playlist", f"{required_dimension},{dimension},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_playlist", f"{required_dimension},{filters_3[k]}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k == 0:
                        if dimension == "day":
                            df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_playlist", f"{required_dimension},{dimension}.csv"))
                        else:
                            df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_playlist", f"{required_dimension}.csv"))

        cols_to_swap = filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if dimension == "day":
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i]}", col)
            else:
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", col)
        if dimension == "day":
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "csv", "playback_locations_playlist", f"{required_dimension},{dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "excel", "playback_locations_playlist", f"{required_dimension},{dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")
        else:
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "csv", "playback_locations_playlist", f"{required_dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "excel", "playback_locations_playlist", f"{required_dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_playback_locations_details_playlist(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents):
    
    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_provinces:
        filters_to_remove_1.append("province")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    sorting_options = ["-views", "-estimatedMinutesWatched", "-playlistStarts"]
    required_dimension = "insightPlaybackLocationDetail"
    required_filter = "isCurated==1;insightPlaybackLocationType==EMBEDDED"
    dimensions = ["", "day"]
    filters_1 = ["", "country", "province", "continent", "subContinent"]
    filters_2 = ["", "playlist", "group"]
    filters_3 = ["", "subscribedStatus"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)
    
    df = None
    to_merge = None
    for sort_by in sorting_options:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    if i != 0 and j != 0 and k != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_details_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{sort_by}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j != 0 and k == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_details_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]},{sort_by}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_details_playlist", f"{required_dimension},{filters_1[i]},{filters_3[k]},{sort_by}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_details_playlist", f"{required_dimension},{filters_1[i]},{sort_by}.csv"))
                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_details_playlist", f"{required_dimension},{filters_2[j]},{filters_3[k]},{sort_by}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_details_playlist", f"{required_dimension},{filters_2[j]},{sort_by}.csv"))
                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_details_playlist", f"{required_dimension},{filters_3[k]},{sort_by}.csv"))
                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k == 0:
                        df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "playback_locations_details_playlist", f"{required_dimension},{sort_by}.csv"))

        cols_to_swap = filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if cols_to_swap[i] == "continent":
                col = df[f"{cols_to_swap[i]}"]
                new_col = []
                for row in col:
                    if row == row:
                        if row < 10:
                            new_col.append(continents_rev_dict["00" + str(int(row))])
                        elif row < 100:
                            new_col.append(continents_rev_dict["0" + str(int(row))])
                        else:
                            new_col.append(continents_rev_dict[str(int(row))])
                    else:
                        new_col.append(row)
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i, f"{cols_to_swap[i]}", new_col)
            elif cols_to_swap[i] == "subContinent":
                col = df[f"{cols_to_swap[i]}"]
                new_col = []
                for row in col:
                    if row == row:
                        if row < 10:
                            new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                        elif row < 100:
                            new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                        else:
                            new_col.append(subcontinents_rev_dict[str(int(row))])
                    else:
                        new_col.append(row)
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i, f"{cols_to_swap[i].lower()}", new_col)
            else:
                col = df[f"{cols_to_swap[i]}"]
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i]}", col)
        df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "csv", "playback_locations_details_playlist", f"{required_dimension},{','.join(cols_to_swap)},{sort_by}.csv"), index=False, na_rep="NULL")
        df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "excel", "playback_locations_details_playlist", f"{required_dimension},{','.join(cols_to_swap)},{sort_by}.xlsx"), index=False, na_rep="NULL")

def clean_traffic_sources_playlist(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents): 
    
    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_provinces:
        filters_to_remove_1.append("province")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    required_dimension = "insightTrafficSourceType"
    required_filter = "isCurated==1"
    dimensions = ["", "day"]
    filters_1 = ["", "country", "province", "continent", "subContinent"]
    filters_2 = ["", "playlist", "group"]
    filters_3 = ["", "subscribedStatus"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)
    
    df = None
    to_merge = None
    for dimension in dimensions:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    if i != 0 and j != 0 and k != 0:
                        if dimension == "day":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j != 0 and k == 0:
                        if dimension == "day":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k != 0:
                        if dimension == "day":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_playlist", f"{required_dimension},{filters_1[i]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k == 0:
                        if dimension == "day":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_playlist", f"{required_dimension},{dimension},{filters_1[i]}.csv"))
                            merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_playlist", f"{required_dimension},{filters_1[i]}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k != 0:
                        if dimension == "day":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_playlist", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_playlist", f"{required_dimension},{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k == 0:
                        if dimension == "day":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_playlist", f"{required_dimension},{dimension},{filters_2[j]}.csv"))
                            merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_playlist", f"{required_dimension},{filters_2[j]}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k != 0:
                        if dimension == "day":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_playlist", f"{required_dimension},{dimension},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_playlist", f"{required_dimension},{filters_3[k]}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k == 0:
                        if dimension == "day":
                            df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_playlist", f"{required_dimension},{dimension}.csv"))
                        else:
                            df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_playlist", f"{required_dimension}.csv"))

        cols_to_swap = filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if dimension == "day":
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i]}", col)
            else:
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", col)
        if dimension == "day":
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "csv", "traffic_sources_playlist", f"{required_dimension},{dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "excel", "traffic_sources_playlist", f"{required_dimension},{dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")
        else:
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "csv", "traffic_sources_playlist", f"{required_dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "excel", "traffic_sources_playlist", f"{required_dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_traffic_sources_details_playlist(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents):

    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_provinces:
        filters_to_remove_1.append("province")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    sorting_options = ["-views", "-estimatedMinutesWatched", "-playlistStarts"]
    required_dimension = "insightTrafficSourceDetail"
    filters_1 = ["", "country", "province", "continent", "subContinent"]
    filters_2 = ["", "playlist", "group"]
    filters_3 = ["", "subscribedStatus"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)
    
    df = None
    to_merge = None
    for sort_by in sorting_options:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    if i != 0 and j != 0 and k != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_details_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{sort_by}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j != 0 and k == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_details_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]},{sort_by}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_details_playlist", f"{required_dimension},{filters_1[i]},{filters_3[k]},{sort_by}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_details_playlist", f"{required_dimension},{filters_1[i]},{sort_by}.csv"))
                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_details_playlist", f"{required_dimension},{filters_2[j]},{filters_3[k]},{sort_by}.csv"))
                        merge_on = list(to_merge.columns)
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k == 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_details_playlist", f"{required_dimension},{filters_2[j]},{sort_by}.csv"))
                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k != 0:
                        to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_details_playlist", f"{required_dimension},{filters_3[k]},{sort_by}.csv"))
                        merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                        df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k == 0:
                        df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "traffic_sources_details_playlist", f"{required_dimension},{sort_by}.csv"))
        
        cols_to_swap = filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if cols_to_swap[i] == "continent":
                col = df[f"{cols_to_swap[i]}"]
                new_col = []
                for row in col:
                    if row == row:
                        if row < 10:
                            new_col.append(continents_rev_dict["00" + str(int(row))])
                        elif row < 100:
                            new_col.append(continents_rev_dict["0" + str(int(row))])
                        else:
                            new_col.append(continents_rev_dict[str(int(row))])
                    else:
                        new_col.append(row)
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i, f"{cols_to_swap[i]}", new_col)
            elif cols_to_swap[i] == "subContinent":
                col = df[f"{cols_to_swap[i]}"]
                new_col = []
                for row in col:
                    if row == row:
                        if row < 10:
                            new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                        elif row < 100:
                            new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                        else:
                            new_col.append(subcontinents_rev_dict[str(int(row))])
                    else:
                        new_col.append(row)
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i, f"{cols_to_swap[i].lower()}", new_col)
            else:
                col = df[f"{cols_to_swap[i]}"]
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i]}", col)
        df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "csv", "traffic_sources_details_playlist", f"{required_dimension},{','.join(cols_to_swap)},{sort_by}.csv"), index=False, na_rep="NULL")
        df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "excel", "traffic_sources_details_playlist", f"{required_dimension},{','.join(cols_to_swap)},{sort_by}.xlsx"), index=False, na_rep="NULL")

def clean_device_type_playlist(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents):

    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_provinces:
        filters_to_remove_1.append("province")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    dimensions = ["", "day"]
    required_dimension = "deviceType"
    filters_1 = ["", "country", "province", "continent", "subContinent"]
    filters_2 = ["", "playlist", "group"]
    filters_3 = ["", "operatingSystem"]
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
                            if i != 0 and j != 0 and k != 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k != 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k == 0 and l != 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k == 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k == 0 and l == 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k == 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k != 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k != 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_1[i]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k == 0 and l != 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_1[i]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k == 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_1[i]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k == 0 and l == 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_1[i]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k == 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_1[i]}.csv"))
                                    merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_1[i]}.csv"))
                                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k != 0 and l != 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k != 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k != 0 and l == 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k != 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_2[j]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k == 0 and l != 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k == 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_2[j]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_2[j]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k == 0 and l == 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_2[j]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_2[j]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k == 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_2[j]}.csv"))
                                    merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_2[j]}.csv"))
                                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k != 0 and l != 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k != 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k != 0 and l == 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_3[k]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_3[k]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k != 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_3[k]}.csv"))
                                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k == 0 and l != 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k == 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_4[l]}.csv"))
                                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k == 0 and l == 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{filters_5[m]}.csv"))
                                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k == 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension}.csv"))
                                else:
                                    df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension}.csv"))
        
        cols_to_swap = filters_5[1:] + filters_4[1:] + filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if dimension == "day":
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i]}", col)
            else:
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", col)
        if dimension == "day":
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "excel", "device_type_playlist", f"{required_dimension},{dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")
        else:
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "csv", "device_type_playlist", f"{required_dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "excel", "device_type_playlist", f"{required_dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_operating_system_playlist(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents):

    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_provinces:
        filters_to_remove_1.append("province")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    dimensions = ["", "day"]
    required_dimension = "operatingSystem"
    filters_1 = ["", "country", "province", "continent", "subContinent"]
    filters_2 = ["", "playlist", "group"]
    filters_3 = ["", "deviceType"]
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
                            if i != 0 and j != 0 and k != 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k != 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k == 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j != 0 and k == 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k != 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k != 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{filters_1[i]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k == 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{filters_1[i]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i != 0 and j == 0 and k == 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{filters_1[i]}.csv"))
                                    merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{filters_1[i]}.csv"))
                                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k != 0 and l != 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k != 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k != 0 and l == 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k != 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{filters_2[j]},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k == 0 and l != 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{filters_2[j]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k == 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{filters_2[j]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{filters_2[j]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k == 0 and l == 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{filters_2[j]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{filters_2[j]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j != 0 and k == 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{filters_2[j]}.csv"))
                                    merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{filters_2[j]}.csv"))
                                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k != 0 and l != 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{filters_3[k]},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k != 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{filters_3[k]},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k != 0 and l == 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{filters_3[k]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{filters_3[k]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k != 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{filters_3[k]}.csv"))
                                    merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{filters_3[k]}.csv"))
                                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k == 0 and l != 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{filters_4[l]},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k == 0 and l != 0 and m == 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{filters_4[l]}.csv"))
                                    merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{filters_4[l]}.csv"))
                                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k == 0 and l == 0 and m != 0:
                                if dimension == "day":
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{filters_5[m]}.csv"))
                                    merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                                else:
                                    to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{filters_5[m]}.csv"))
                                    merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                                    df = df.merge(to_merge, how="outer", on=merge_on)
                            elif i == 0 and j == 0 and k == 0 and l == 0 and m == 0:
                                if dimension == "day":
                                    df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension}.csv"))
                                else:
                                    df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension}.csv"))
        
        print(df)
        cols_to_swap = filters_5[1:] + filters_4[1:] + filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if dimension == "day":
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i]}", col)
            else:
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", col)
        if dimension == "day":
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "excel", "operating_system_playlist", f"{required_dimension},{dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")
        else:
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "csv", "operating_system_playlist", f"{required_dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "excel", "operating_system_playlist", f"{required_dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")
        print(df)

def clean_operating_system_and_device_type_playlist(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents):

    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_provinces:
        filters_to_remove_1.append("province")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    required_dimension = "deviceType,operatingSystem"
    dimensions = ["", "day"]
    filters_1 = ["", "country", "province", "continent", "subContinent"]
    filters_2 = ["", "playlist", "group"]
    filters_3 = ["", "subscribedStatus"]
    filters_4 = ["", "youtubeProduct"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)

    df = None
    to_merge = None
    for dimension in dimensions:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    for l in range(len(filters_4)):
                        if i != 0 and j != 0 and k != 0 and l == 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k == 0 and l == 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{filters_1[i]},{filters_2[j]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k != 0 and l == 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{dimension},{filters_1[i]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{filters_1[i]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k == 0 and l == 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{dimension},{filters_1[i]}.csv"))
                                merge_on = list(to_merge.columns)[:3] + list(to_merge.columns)[4:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{filters_1[i]}.csv"))
                                merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k != 0 and l != 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k != 0 and l == 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{dimension},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{filters_2[j]},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k == 0 and l != 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{dimension},{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{filters_2[j]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k == 0 and l == 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{dimension},{filters_2[j]}.csv"))
                                merge_on = list(to_merge.columns)[:3] + list(to_merge.columns)[4:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{filters_2[j]}.csv"))
                                merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k != 0 and l != 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{dimension},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{filters_3[k]},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k != 0 and l == 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{dimension},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)[:3] + list(to_merge.columns)[4:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{filters_3[k]}.csv"))
                                merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k == 0 and l != 0:
                            if dimension == "day":
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{dimension},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)[:3] + list(to_merge.columns)[4:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                            else:
                                to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{filters_4[l]}.csv"))
                                merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                                df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k == 0 and l == 0:
                            if dimension == "day":
                                df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{dimension}.csv"))
                            else:
                                df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension}.csv"))

        cols_to_swap = filters_4[1:] + filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if dimension == "day":
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+3, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+3, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+3, f"{cols_to_swap[i]}", col)
            else:
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i]}", col)
        if dimension == "day":
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "excel", "operating_system_and_device_type_playlist", f"{required_dimension},{dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")
        else:
            df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "csv", "operating_system_and_device_type_playlist", f"{required_dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
            df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "excel", "operating_system_and_device_type_playlist", f"{required_dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_viewer_demographics_playlist(channel_name, date, has_groups, has_countries, has_provinces, has_continents, has_subcontinents):

    filters_to_remove_1 = []
    filters_to_remove_2 = []
    if not has_countries:
        filters_to_remove_1.append("country")
    if not has_provinces:
        filters_to_remove_1.append("province")
    if not has_continents:
        filters_to_remove_1.append("continent")
    if not has_subcontinents:
        filters_to_remove_1.append("subContinent")
    if not has_groups:
        filters_to_remove_2.append("group")

    dimensions = ["ageGroup", "gender", "ageGroup,gender"]
    filters_1 = ["", "country", "province", "continent", "subContinent"]
    filters_2 = ["", "playlist", "group"]
    filters_3 = ["", "subscribedStatus"]

    for fil_to_rem in filters_to_remove_1:
        filters_1.remove(fil_to_rem)
    for fil_to_rem in filters_to_remove_2:
        filters_2.remove(fil_to_rem)

    df = None
    to_merge = None
    for dimension in dimensions:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    if i != 0 and j != 0 and k != 0:
                        if dimension == "ageGroup,gender":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j != 0 and k == 0:
                        if dimension == "ageGroup,gender":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_2[j]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k != 0:
                        if dimension == "ageGroup,gender":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i != 0 and j == 0 and k == 0:
                        if dimension == "ageGroup,gender":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]}.csv"))
                            merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_1[i]}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k != 0:
                        if dimension == "ageGroup,gender":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_2[j]},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j != 0 and k == 0:
                        if dimension == "ageGroup,gender":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_2[j]}.csv"))
                            merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_2[j]}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k != 0:
                        if dimension == "ageGroup,gender":
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_3[k]}.csv"))
                            merge_on = list(to_merge.columns)[:2] + list(to_merge.columns)[3:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        else:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{filters_3[k]}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                    elif i == 0 and j == 0 and k == 0:
                        if dimension == "ageGroup,gender":
                            df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension}.csv"))
                        else:
                            df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension}.csv"))

        cols_to_swap = filters_3[1:] + filters_2[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if dimension == "ageGroup,gender":
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+2, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+5, f"{cols_to_swap[i]}", col)
            else:
                if cols_to_swap[i] == "continent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(continents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(continents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(continents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i]}", new_col)
                elif cols_to_swap[i] == "subContinent":
                    col = df[f"{cols_to_swap[i]}"]
                    new_col = []
                    for row in col:
                        if row == row:
                            if row < 10:
                                new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                            elif row < 100:
                                new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                            else:
                                new_col.append(subcontinents_rev_dict[str(int(row))])
                        else:
                            new_col.append(row)
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
                else:
                    col = df[f"{cols_to_swap[i]}"]
                    df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                    df.insert(i+4, f"{cols_to_swap[i]}", col)
        df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "csv", "viewer_demographics_playlist", f"{dimension},{','.join(cols_to_swap)}.csv"), index=False, na_rep="NULL")
        df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "excel", "viewer_demographics_playlist", f"{dimension},{','.join(cols_to_swap)}.xlsx"), index=False, na_rep="NULL")

def clean_top_playlists(channel_name, date, has_countries, has_provinces, has_continents, has_subcontinents):

    sorting_options = ["-views", "-redViews", "-estimatedMinutesWatched", "-estimatedRedMinutesWatched", "-playlistStarts"]
    required_dimension = "playlist"
    
    filters_to_remove = []
    if not has_countries:
        filters_to_remove.append("country")
    if not has_provinces:
        filters_to_remove.append("province")
    if not has_continents:
        filters_to_remove.append("continent")
    if not has_subcontinents:
        filters_to_remove.append("subContinent")

    filters_1 = ["", "country", "province", "continent", "subContinent"]
    filters_2 = ["", "playlist"]
    filters_3 = ["", "subscribedStatus"]
    filters_4 = ["", "youtubeProduct"]

    for fil_to_rem in filters_to_remove:
        filters_1.remove(fil_to_rem)

    df = None
    to_merge = None
    for sort_by in sorting_options:
        for i in range(len(filters_1)):
            for j in range(len(filters_2)):
                for k in range(len(filters_3)):
                    for l in range(len(filters_4)):
                        if i != 0 and j != 0 and k != 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "top_playlists", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{filters_4[l]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k != 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "top_playlists", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_3[k]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k == 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "top_playlists", f"{required_dimension},{filters_1[i]},{filters_2[j]},{filters_4[l]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j != 0 and k == 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "top_playlists", f"{required_dimension},{filters_1[i]},{filters_2[j]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k != 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "top_playlists", f"{required_dimension},{filters_1[i]},{filters_3[k]},{filters_4[l]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k != 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "top_playlists", f"{required_dimension},{filters_1[i]},{filters_3[k]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k == 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "top_playlists", f"{required_dimension},{filters_1[i]},{filters_4[l]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i != 0 and j == 0 and k == 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "top_playlists", f"{required_dimension},{filters_1[i]},{sort_by}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k != 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "top_playlists", f"{required_dimension},{filters_2[j]},{filters_3[k]},{filters_4[l]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k != 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "top_playlists", f"{required_dimension},{filters_2[j]},{filters_3[k]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k == 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "top_playlists", f"{required_dimension},{filters_2[j]},{filters_4[l]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j != 0 and k == 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "top_playlists", f"{required_dimension},{filters_2[j]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k != 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "top_playlists", f"{required_dimension},{filters_3[k]},{filters_4[l]},{sort_by}.csv"))
                            merge_on = list(to_merge.columns)
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k != 0 and l == 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "top_playlists", f"{required_dimension},{filters_3[k]},{sort_by}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k == 0 and l != 0:
                            to_merge = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "top_playlists", f"{required_dimension},{filters_4[l]},{sort_by}.csv"))
                            merge_on = [list(to_merge.columns)[0]] + list(to_merge.columns)[2:]
                            df = df.merge(to_merge, how="outer", on=merge_on)
                        elif i == 0 and j == 0 and k == 0 and l == 0:
                            df = pd.read_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "raw", "playlist_reports", "csv", "top_playlists", f"{required_dimension},{sort_by}.csv"))

        cols_to_swap = filters_4[1:] + filters_3[1:] + filters_1[1:]
        for i in range(len(cols_to_swap)):
            if cols_to_swap[i] == "continent":
                col = df[f"{cols_to_swap[i]}"]
                new_col = []
                for row in col:
                    if row == row:
                        if row < 10:
                            new_col.append(continents_rev_dict["00" + str(int(row))])
                        elif row < 100:
                            new_col.append(continents_rev_dict["0" + str(int(row))])
                        else:
                            new_col.append(continents_rev_dict[str(int(row))])
                    else:
                        new_col.append(row)
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i]}", new_col)
            elif cols_to_swap[i] == "subContinent":
                col = df[f"{cols_to_swap[i]}"]
                new_col = []
                for row in col:
                    if row == row:
                        if row < 10:
                            new_col.append(subcontinents_rev_dict["00" + str(int(row))])
                        elif row < 100:
                            new_col.append(subcontinents_rev_dict["0" + str(int(row))])
                        else:
                            new_col.append(subcontinents_rev_dict[str(int(row))])
                    else:
                        new_col.append(row)
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i].lower()}", new_col)
            else:
                col = df[f"{cols_to_swap[i]}"]
                df.drop(labels=[f"{cols_to_swap[i]}"], axis=1, inplace=True)
                df.insert(i+1, f"{cols_to_swap[i]}", col)
        df.to_csv(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "csv", "top_playlists", f"{required_dimension},{','.join(cols_to_swap)},{sort_by}.csv"), index=False, na_rep="NULL")
        df.to_excel(os.path.join(os.getcwd(), f"{channel_name}_data", f"{date}", "clean", "playlist_reports", "excel", "top_playlists", f"{required_dimension},{','.join(cols_to_swap)},{sort_by}.xlsx"), index=False, na_rep="NULL")



clean("Concept_New_Era", f"{str(date.today())}", False, True, True, True, True)
#clean("Concept_New_Era", "2021-11-03", False, True, True, True, True)
