import requests
import json
import os
import gzip
import shutil
from dotenv import load_dotenv

# Initialize variables
api_key = os.getenv("API_KEY") # getting API key from pycharm env, used server-side as per https://developers.faceit.com/docs/auth/api-keys
base_url = "https://open.faceit.com/data/v4" # storing base part of URL for easier manipulation
replays_path = "D:/Personal/School/GRAD SCHOOL/CAPSTONE/Data" # path to downloads storage
regular_season_path = "D:/Personal/School/GRAD SCHOOL/CAPSTONE/Data/Regular Season" # path to regular season replay storage
playoffs_path = "D:/Personal/School/GRAD SCHOOL/CAPSTONE/Data/Playoffs" # path to playoffs replay storage
headers = {
    "Accept": "application/json",
    "Authorization": f"Bearer {api_key}"
}

# DEFINING USEFUL FUNCTIONS
def get_competition_id(match_id):
    """Retrieves the competition id of a competition given a match within the desired competition itself"""
    url = f"{base_url}/matches/{match_id}" # building URL, format found in https://docs.faceit.com/docs/data-api/data#tag/Matches
    response = requests.get(url, headers=headers) # retrieve league ID using requests, calls authorization from headers variable
    if response.status_code == 200:
        data = response.json()
        competition_id = data["competition_id"]
        return competition_id
    else:
        print(f"Error: {response.status_code}")
        return response.status_code

def get_comp_match_ids(competition_id, limit=100):
    """Retrieves ALL past match_ids from a competition"""
    offset = 0
    matches = []
    while True:
        url = f"{base_url}/championships/{competition_id}/matches?type=past&offset={offset}&limit={limit}" # format found in https://docs.faceit.com/docs/data-api/data/#tag/Championships/operation/getChampionshipMatches
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            print(data)
            for item in data["items"]:
                add_id = item["match_id"]
                matches.append(add_id) # loop over elements of response to add ids
            if len(data["items"]) < limit: # breaks loop if there are no more match_ids to be retrieved
                break
            offset += limit # gets next page of results
        else:
            print(f"Error: {response.status_code}")
    return matches

def get_match_demos(match_id):
    """Retrieves demo url link(s) associated with a single match id (some may have multiple)"""
    url = f"{base_url}/matches/{match_id}" # building URL
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        demo_urls = data.get("demo_url")
        if demo_urls:
            return demo_urls
        else:
            # print(f"Match {match_id} was forfeited or is unfinished.")
            return None
    else:
        print(f"Error: {response.status_code}")
        return None

def download_demo(match_id, demo_urls, save_path):
    """Downloads and extracts a demo given the demo url"""
    url = "https://open.faceit.com/download/v2/demos/download"
    if not os.path.exists(save_path):
        os.makedirs(save_path) # creates directory if it doesn't exist

    # loops over all demos in the series given by the match_id
    for i, demo_url in enumerate(demo_urls):
        demo_gz_path = os.path.join(save_path, f"{match_id}-map-{i+1}.dem.gz")  # filepath construction for compressed
        demo_file_path = os.path.join(save_path, f"{match_id}-map-{i+1}.dem")  # filepath construction for extracted
        if os.path.exists(demo_gz_path):
            print(f"Skipping {match_id}") # skips any files that are already downloaded
            continue

        response = requests.post(url, json={"resource_url": demo_url}, headers=headers) # /download/v2/demos/download

        if response.status_code != 200:
            print(f"Failed to download demo for match {match_id}")
            print(response.status_code, response.text)
            raise RuntimeError()

        # https://docs.python.org/3/library/urllib.parse.html
        download_url = response.json()["payload"]["download_url"]
        download = requests.get(download_url, stream=True)
        if download.status_code != 200:
            print(f"Failed to download demo for match {match_id}")
            print(download.status_code, download.text)
            raise RuntimeError()

        # downloads the demo if a response exists
        with open(demo_gz_path, "wb") as f:
            for chunk in download.iter_content(chunk_size=1024):
                f.write(chunk)
        print(f"Downloaded match #{i+1} from {match_id} to {save_path}")
        # extract the .dem from the .gz
        with gzip.open(demo_gz_path, "rb") as f_in:
            with open(demo_file_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"Extracted demo #{i+1} for match {match_id}")

def scan_for_demos(competition_id, save_path):
    """Given a competition, downloads all demos that are not already in the directory"""
    matches = get_comp_match_ids(competition_id) # gets competition matches using our function
    for count, match_id in enumerate(matches, start=1):
        demo_urls = get_match_demos(match_id)
        if not demo_urls:
            print(f"Match {match_id} was forfeited or is unfinished.")
            continue
        download_demo(match_id, demo_urls, save_path)
        print(f"Match {count} of {len(matches)} has been dealt with.")
    print("Done!")

# Executing
regular_season_id = get_competition_id("1-ca53288a-f8c2-4048-903b-60be72ebb7f1")  # match id of a random match in S51 Advanced North America
playoffs_id = get_competition_id("1-d26e3edf-64f5-4565-8cf0-85b5b56ecd86")
scan_for_demos(regular_season_id, regular_season_path)
scan_for_demos(playoffs_id, playoffs_path)