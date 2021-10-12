
import json
import requests
from datetime import datetime, date
from channel_reports_variables import countries, provinces, continents, subcontinents

def num_vids(channel_id, API_KEY):
    url = f"https://www.googleapis.com/youtube/v3/channels?part=statistics&id={channel_id}&key={API_KEY}"
    return int(json.loads(requests.get(url).text)["items"][0]["statistics"]["videoCount"])

def get_vid_ids(num_vids, channel_id, API_KEY):
    vid_ids = []
    date_of_vid = datetime.now().isoformat()[:21] + "Z"
    last_vid_id = ""

    while len(vid_ids) < num_vids:
        url = f"https://www.googleapis.com/youtube/v3/activities?part=contentDetails&channelId={channel_id}&maxResults=256&publishedBefore={date_of_vid}&key={API_KEY}"
        response = json.loads(requests.get(url).text)
        for item in response["items"]:
            if len(item["contentDetails"]) != 0:
                try:
                    if item["contentDetails"]["upload"]["videoId"] not in vid_ids:
                        vid_ids.append(item["contentDetails"]["upload"]["videoId"])
                except KeyError:
                    if item["contentDetails"]["playlistItem"]["resourceId"]["videoId"] not in self.vid_ids:
                        vid_ids.append(item["contentDetails"]["playlistItem"]["resourceId"]["videoId"])

        if last_vid_id == vid_ids[-1]:
            break
        else:
            last_vid_id = vid_ids[-1]
            url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet&id={last_vid_id}&key={API_KEY}"
            date_of_vid = json.loads(requests.get(url).text)["items"][0]["snippet"]["publishedAt"]
    return vid_ids

def get_playlist_ids(channel_id, API_KEY):
    playlist_ids = []
    url = f"https://www.googleapis.com/youtube/v3/playlists?part=id&channelId={channel_id}&maxResults=50&key={API_KEY}"
    response = json.loads(requests.get(url).text)
    if len(response["items"]) != 0:
        for item in response["items"]:
            playlist_ids.append(item["id"])

    while 1:
        try:
            next_page_token = response["nextPageToken"]
            url = f"https://www.googleapis.com/youtube/v3/playlists?part=id&channelId={channel_id}&maxResults=50&pageToken={next_page_token}&key={API_KEY}"
            response = json.loads(requests.get(url).text)
            if len(response["items"]) != 0:
                for item in response["items"]:
                    playlist_ids.append(item["id"])
        except KeyError:
            break
    return playlist_ids

def has_groups():
    groups_list = []
    groups_response = execute_api_request(
        youtubeAnalytics.groups().list,
        mine=True
    )
    has_groups = False
    if len(groups_response["items"]) != 0:
        groups_list.append(groups_response["items"])
        has_groups = True
    return groups_list, has_groups

def ask_countries():
    countries_list = []
    done = False
    has_countries = False
    print("\nPlease list the countries you wish to filter the data on.")
    print(f"\nThe list of countries you can chose from are as follows: \n{', '.join(countries.keys())}")
    print("\nPlease type one country and click enter for each country, when done, type 'done'.")
    while not done:
        if len(countries) != 0:
            user_response = input("Country: ").strip()
            if user_response == "done":
                done = True
            else:
                if user_response in countries.keys():
                    countries_list.append(countries[user_response])
                    del countries[user_response]
                    has_countries = True
                else:
                    print("\nInvalid country, please type a contry from the list or type 'done'.")
        else:
            done = True
    return countries_list, has_countries

def ask_continents():
    continents_list = []
    done = False
    has_continents = False
    print("\nPlease list the continents you wish to filter data on.")
    print(f"\nThe list of continents you can chose from are as follows: \n{', '.join(continents.keys())}")
    print("\nPlease type one continent and click enter for each continent, when done, type 'done'.")
    while not done:
        if len(continents) != 0:
            user_response = input("Continent: ").strip()
            if user_response == "done":
                done = True
            else:
                if user_response in continents.keys():
                    continents_list.append(continents[user_response])
                    del continents[user_response]
                    has_continents = True
                else:
                    print("\nInvalid continent, please type a continent from the list or type 'done'.")
        else:
            done = True
    return continents_list, has_continents
                    
def ask_subcontinents():
    subcontinents_list = []
    done = False
    has_subcontinents = False
    print("\nPlease list the subcontinents you wish to filter data on.")
    print(f"\nThe list of subcontinents you can chose from are as follows: \n{', '.join(subcontinents.keys())}")
    print("\nPlease type one subcontinent and click enter for each subcontinent, when done, type 'done'.")
    while not done:
        if len(subcontinents) != 0:
            user_response = input("Subcontinent: ").strip()
            if user_response == "done":
                done = True
            else:
                if user_response in subcontinents.keys():
                    subcontinents_list.append(subcontinents[user_response])
                    del subcontinents[user_response]
                    has_subcontinents = True
                else:
                    print("\nInvalid subcontinent, please type a subcontinent from the list or type 'done'.")
        else:
            done = True
    return subcontinents_list, has_subcontinents


def ask_provinces():
    provinces_list = []
    done = False
    has_provinces = False
    print("\nPlease list the provinces you wish to filter data on.")
    print(f"\nThe list of provinces you can chose from are as follows: \n{', '.join(provinces.keys())}")
    print("\nPlease type one province and click enter for each province, when done, type 'done'.")
    while not done:
        if len(provinces) != 0:
            user_response = input("Province: ").strip()
            if user_response == "done":
                done = True
            else:
                if user_response in provinces.keys():
                    provinces_list.append(provinces[user_response])
                    del provinces[user_response]
                    has_provinces = True
                else:
                    print("\nInvalid province, please type a province from the list or type 'done'.")
        else:
            done = True
    return provinces_list, has_provinces

def ask_dates(channel_id, API_KEY):
    print("Do you wish to filter the results within a given time period? If so please enter a start date and an end date to filter data between. If you want information from the beginning of the channel until now type answer 'yes' to the following question.")
    user_response = input("Do you want information from your channel inception until today? Type 'yes' or 'no'. ")
    if user_response == 'yes':
        end_date = str(date.today())
        url = f"https://www.googleapis.com/youtube/v3/channels?part=snippet&id={channel_id}&key={API_KEY}"
        start_date = json.loads(requests.get(url).text)["items"][0]["snippet"]["publishedAt"][:10]
    else:
        start_date = input("What is the start date you wish to filter from, start date should be before the end date. Type the date in yyyy-mm-dd format. ").strip()
        end_date = input("What is the end date you wish to filter to, end date should be after the start date. Type the date in yyyy-mm-dd format. ").strip()
    return start_date, end_date
