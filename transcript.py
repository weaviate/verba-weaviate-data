import requests
import json
from youtube_transcript_api import YouTubeTranscriptApi
from dotenv import load_dotenv
import os

API_ENDPOINT = "https://www.googleapis.com/youtube/v3/search"


def load_configuration():
    env_path = "/."
    load_dotenv(dotenv_path=env_path)
    YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
    CHANNEL_ID = os.getenv("CHANNEL_ID")
    return YOUTUBE_API_KEY, CHANNEL_ID


def get_all_video_ids(api_key, channel_id):
    video_ids = []
    page_token = None

    while True:
        params = {
            "key": api_key,
            "channelId": channel_id,
            "part": "snippet,id",
            "order": "date",
            "maxResults": 50,
            "pageToken": page_token,
        }

        response = requests.get(API_ENDPOINT, params=params)
        data = response.json()

        for item in data.get("items", []):
            if item["id"]["kind"] == "youtube#video":
                video_ids.append(item["id"]["videoId"])

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return video_ids


# Fetch transcripts for each video ID
def fetch_transcripts(video_ids):
    transcripts = []

    for video_id in video_ids:
        print(f"Downloading Transcript from {video_id}")
        try:
            transcript_data = YouTubeTranscriptApi.get_transcript(video_id)
            chunked_transcript = []
            chunk = []
            chunk_start = 0
            chunk_duration = 0

            for entry in transcript_data:
                # Initialize chunk start time
                if not chunk:
                    chunk_start = entry["start"]

                # Add entry to chunk
                chunk.append(entry["text"])
                chunk_duration += entry["duration"]

                if len(chunk) >= 20 or chunk_duration >= 300:
                    chunked_transcript.append(
                        {
                            "start": chunk_start,
                            "duration": chunk_duration,
                            "text": " ".join(chunk),
                        }
                    )
                    # Reset chunk
                    chunk = []
                    chunk_duration = 0

            if chunk:
                chunked_transcript.append(
                    {
                        "start": chunk_start,
                        "duration": chunk_duration,
                        "text": " ".join(chunk),
                    }
                )

            transcripts.append({video_id: chunked_transcript})

        except:
            print(f"Failed to fetch transcript for video ID: {video_id}")

    return transcripts


def fetch_youtube_transcripts():
    YOUTUBE_API_KEY, CHANNEL_ID = load_configuration()
    video_ids = get_all_video_ids(YOUTUBE_API_KEY, CHANNEL_ID)
    return fetch_transcripts(video_ids)
