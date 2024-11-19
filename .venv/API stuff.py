import requests
import json
import os
import gzip
import shutil
from dotenv import load_dotenv

# Initialize variables
api_key = os.getenv("API_KEY") # getting API key from pycharm env, used server-side as per https://developers.faceit.com/docs/auth/api-keys
base_url = "https://open.faceit.com/data/v4" # storing base part of URL for easier manipulation
replays_path = "D:/Personal/School/GRAD SCHOOL/CAPSTONE/Data/Replays" # path to replays storage
database_file = "D:/Personal/School/GRAD SCHOOL/CAPSTONE/Data/downloaded_matches.json" # path to database file
headers = {
    "Accept": "application/json",
    "Authorization": f"Bearer {api_key}"
}

# Initialize database to store what demos we have downloaded
if not os.path.exists(database_file):
    os.makedirs(os.path.dirname(database_file))
    with open(database_file, "w") as f:
        json.dump({"downloaded_matches": []}, f)

with open(database_file, "r") as f:
    database = json.load(f)

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
        return None

def get_comp_match_ids(competition_id, limit=100):
    """Retrieves ALL past match_ids from a competition"""
    offset = 0
    matches = []
    while True:
        url = f"{base_url}/championships/{competition_id}/matches?type=past&offset={offset}&limit={limit}" # format found in https://docs.faceit.com/docs/data-api/data/#tag/Championships/operation/getChampionshipMatches
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
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
        demo_urls = data["demo_url"]
        return demo_urls
    else:
        print(f"Error: {response.status_code}")
        return None

def download_demo(match_id, save_path = replays_path):
    """Downloads and extracts a demo given the demo url"""
    demo_urls = get_match_demos(match_id) # calls get_match_demos on inputted match_id to get list of demo_urls
    if not os.path.exists(save_path):
        os.makedirs(save_path) # creates directory if it doesn't exist

    # loops over all demos in the series given by the match_id
    for i, demo_url in enumerate(demo_urls):
        demo_gz_path = os.path.join(save_path, f"{match_id}_map{i+1}.dem.gz")  # filepath construction for compressed
        demo_file_path = os.path.join(save_path, f"{match_id}_map{i+1}.dem")  # filepath construction for extracted
        response = requests.get(demo_url, stream=True) # gets response from the ith demo_url
        if response.status_code == 200:
            # downloads the demo if a response exists
            with open(demo_gz_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024):
                    f.write(chunk)
            print(f"Downloaded match #{i+1} from {match_id} to {save_path}")

            # Extract the .dem from the .gz
            with gzip.open(demo_gz_path, "rb") as f_in:
                with open(demo_file_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            print(f"Extracted demo #{i+1} for match {match_id}")
            return True
        else:
            print(f"Failed to download demo for match {match_id}")
            return False

def scan_for_demos(competition_id):
    """Given a competition, downloads all demos that are not already in the database"""
    matches = get_comp_match_ids(competition_id) # gets competition matches using our function
    for match_id in matches:
        if match_id in database["downloaded_matches"]: # skips match if it has already been downloaded
            continue
        demo_url = get_match_demos(match_id)
        if demo_url:
            if download_demo(match_id):
                database["downloaded_matches"].append(match_id)
                download_demo(match_id)
                with open(database_file, "w") as f:
                    json.dump(database, f)
        else:
            print(f"Match {match_id} was forfeited or is unfinished.")

# Executing
comp_id = get_competition_id("1-ca53288a-f8c2-4048-903b-60be72ebb7f1")  # match id of a random match in S51 Advanced North America
# download_demo("1-a7c33adf-8581-4357-9c25-567cf172a6cc")
# scan_for_demos(comp_id)