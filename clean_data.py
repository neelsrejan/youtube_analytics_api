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
    clean_user_activity_in_US_over_subscribed_status(channel_name, date, has_groups, has_provinces)

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

#clean("Concept_New_Era", f"{str(date.today())}", False, True, True, True, True)
clean("Concept_New_Era", "2021-10-27", False, True, True, True, True)
