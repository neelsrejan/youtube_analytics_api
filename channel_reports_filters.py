
import json
import requests
from channel_reports_variables import countries_dict, continents_dict, subcontinents_dict, provinces_dict

class Filters():
    
    def get_num_vids(self):
        url = f"https://www.googleapis.com/youtube/v3/channels?part=statistics&id={self.channel_id}&key={self.API_KEY}"
        self.num_vids = int(json.loads(requests.get(url).text)["items"][0]["statistics"]["videoCount"])
        return 

    def get_vid_ids(self):
        date_of_vid = datetime.now().isoformat()[:21] + "Z"
        last_vid_id = ""

        while len(self.vid_ids) < self.num_vids:
            url = f"https://www.googleapis.com/youtube/v3/activities?part=contentDetails&channelId={self.channel_id}&maxResults=256&publishedBefore={date_of_vid}&key={self.API_KEY}"
            response = json.loads(requests.get(url).text)
            for item in response["items"]:
                if len(item["contentDetails"]) != 0:
                    try:
                        if item["contentDetails"]["upload"]["videoId"] not in self.vid_ids:
                            self.vid_ids.append(item["contentDetails"]["upload"]["videoId"])
                    except KeyError:
                        if item["contentDetails"]["playlistItem"]["resourceId"]["videoId"] not in self.vid_ids:
                            self.vid_ids.append(item["contentDetails"]["playlistItem"]["resourceId"]["videoId"])

            if last_vid_id == self.vid_ids[-1]:
                break
            else:
                last_vid_id = self.vid_ids[-1]
                url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet&id={last_vid_id}&key={self.API_KEY}"
                date_of_vid = json.loads(requests.get(url).text)["items"][0]["snippet"]["publishedAt"]
        return

    def get_playlist_ids(self):
        url = f"https://www.googleapis.com/youtube/v3/playlists?part=id&channelId={self.channel_id}&maxResults=50&key={self.API_KEY}"
        response = json.loads(requests.get(url).text)
        if len(response["items"]) != 0:
            for item in response["items"]:
                self.playlist_ids.append(item["id"])

        while 1:
            try:
                next_page_token = response["nextPageToken"]
                url = f"https://www.googleapis.com/youtube/v3/playlists?part=id&channelId={self.channel_id}&maxResults=50&pageToken={next_page_token}&key={self.API_KEY}"
                response = json.loads(requests.get(url).text)
                if len(response["items"]) != 0:
                    for item in response["items"]:
                        self.playlist_ids.append(item["id"])
            except KeyError:
                break
        return

    def ask_countries(self):
        done = False
        print("\nPlease list the countries you wish to filter the data on.")
        print(f"\nThe list of countries you can chose from are as follows: \n{', '.join(countries_dict.keys())}")
        print("\nPlease type one country and click enter for each country, when done, type 'done'.")
        while not done:
            if len(countries_dict) != 0:
                user_response = input("Country: ").strip()
                if user_response == "done":
                    done = True
                else:
                    if user_response in countries_dict.keys():
                        self.countries.append(countries_dict[user_response])
                        del countries_dict[user_response]
                        self.has_countries = True
                    else:
                        print("\nInvalid country, please type a contry from the list or type 'done'.\n")
            else:
                done = True
        return

    def ask_continents(self):
        done = False
        print("\nPlease list the continents you wish to filter data on.")
        print(f"\nThe list of continents you can chose from are as follows: \n{', '.join(continents_dict.keys())}")
        print("\nPlease type one continent and click enter for each continent, when done, type 'done'.")
        while not done:
            if len(continents_dict) != 0:
                user_response = input("Continent: ").strip()
                if user_response == "done":
                    done = True
                else:
                    if user_response in continents_dict.keys():
                        self.continents.append(continents_dict[user_response])
                        del continents_dict[user_response]
                        self.has_continents = True
                    else:
                        print("\nInvalid continent, please type a continent from the list or type 'done'.\n")
            else:
                done = True
        return
                        
    def ask_subcontinents(self):
        done = False
        print("\nPlease list the subcontinents you wish to filter data on.")
        print(f"\nThe list of subcontinents you can chose from are as follows: \n{', '.join(subcontinents_dict.keys())}")
        print("\nPlease type one subcontinent and click enter for each subcontinent, when done, type 'done'.")
        while not done:
            if len(subcontinents_dict) != 0:
                user_response = input("Subcontinent: ").strip()
                if user_response == "done":
                    done = True
                else:
                    if user_response in subcontinents_dict.keys():
                        self.subcontinents.append(subcontinents_dict[user_response])
                        del subcontinents_dict[user_response]
                        self.has_subcontinents = True
                    else:
                        print("\nInvalid subcontinent, please type a subcontinent from the list or type 'done'.\n")
            else:
                done = True
        return

    def ask_provinces(self):
        done = False
        print("\nPlease list the provinces you wish to filter data on.")
        print(f"\nThe list of provinces you can chose from are as follows: \n{', '.join(provinces_dict.keys())}")
        print("\nPlease type one province and click enter for each province, when done, type 'done'.")
        while not done:
            if len(provinces_dict) != 0:
                user_response = input("Province: ").strip()
                if user_response == "done":
                    done = True
                else:
                    if user_response in provinces_dict.keys():
                        self.provinces.append(provinces_dict[user_response])
                        del provinces_dict[user_response]
                        self.has_provinces = True
                    else:
                        print("\nInvalid province, please type a province from the list or type 'done'.\n")
            else:
                done = True
        return

    def ask_dates(self):
        print("Do you wish to filter the results within a given time period? If so please enter a start date and an end date to filter data between. If you want information from the beginning of the channel until now type answer 'yes' to the following question.")
        user_response = input("Do you want information from your channel inception until today? Type 'yes' or 'no'. ")
        if user_response == 'yes':
            self.end_date = str(date.today())
            url = f"https://www.googleapis.com/youtube/v3/channels?part=snippet&id={self.channel_id}&key={self.API_KEY}"
            self.start_date = json.loads(requests.get(url).text)["items"][0]["snippet"]["publishedAt"][:10]
        else:
            self.start_date = input("What is the start date you wish to filter ions=f"{filters_1[i] + ',' + filters_2[j]}",
            rom, start date should be before the end date. Type the date in yyyy-mm-dd format. ").strip()
            self.end_date = input("What is the end date you wish to filter to, end date should be after the start date. Type the date in yyyy-mm-dd format. ").strip()
        return
