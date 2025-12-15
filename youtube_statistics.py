import requests
import json
import os
from dotenv import load_dotenv

load_dotenv(".env")
API_KEY=os.getenv("API_KEY")
HANDLE=os.getenv("HANDLE")
url= f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={HANDLE}&key={API_KEY}"

def get_playlist_id():
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        # print(json.dumps(data,indent=4))
        channel_items=data["items"][0]
        channel_playListId = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
        # print(channel_playListId)
        return channel_playListId

    except requests.exceptions.RequestException as reqErr :
        return reqErr


if __name__=="__main__":
    get_playlist_id()
