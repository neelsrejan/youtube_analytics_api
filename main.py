from YT_ANALYTICS import YT_ANALYTICS

def main():
    
    # Intro to program
    print("Welcome to the Youtube Analytics API!")
    print("\nThis program is intended for you to be able to get as much raw data from the youtube analytics api, to do so for various kinds of reports I will be asking for responses in order to properly get the information you wish to get.")
    
    # Get all information to filter on 
    API_KEY = input("Please enter your API KEY: ").strip()
    channel_id = "UCWPXl--e3JsJxRG64Of_msA" #input("Please enter your channel id: ").strip()
    
    YT = YT_ANALYTICS(API_KEY, channel_id)
    YT.get_filter_info()
    print(YT.num_vids)
    print(YT.vid_ids)
    print(YT.playlist_ids)
    print(YT.start_date)
    print(YT.end_date)
    print(YT.groups, YT.has_groups)
    print(YT.countries, YT.has_countries)
    print(YT.continents, YT.has_continents)
    print(YT.subcontinents, YT.has_subcontinents)
    print(YT.provinces, YT.has_provinces)

    YT.basic_user_activity_statistics()

if __name__ == "__main__":
    main()
    
  
