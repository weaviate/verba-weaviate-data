import requests
import json
from youtube_transcript_api import YouTubeTranscriptApi
from dotenv import load_dotenv
import os

from goldenverba.components.reader.document import Document
from goldenverba.components.chunking.chunk import Chunk

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
                video_ids.append(
                    (
                        item["id"]["videoId"],
                        item["snippet"]["title"],
                        item["snippet"]["description"],
                    )
                )

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return video_ids


# Fetch transcripts for each video ID
def fetch_transcripts(video_ids):
    for snippet_tuple in video_ids:
        video_id = snippet_tuple[0]
        title = snippet_tuple[1]
        description = snippet_tuple[2]
        print(f"Downloading Transcript from {video_id}")
        try:
            transcript_data = YouTubeTranscriptApi.get_transcript(video_id)
            chunks = []
            whole_text = description + " \n"

            chunk_id = 0
            for entry in transcript_data:
                chunk_obj = Chunk(
                    text=entry["text"],
                    doc_name=title,
                    chunk_id=chunk_id,
                    doc_uuid="",
                    doc_type="Video",
                )
                chunk_id += 1
                chunks.append(chunk_obj)
                whole_text += entry["text"] + " \n"

            document_obj = Document(
                text=whole_text,
                type="Video",
                name=title,
                link=f"https://www.youtube.com/watch?v={video_id}",
                reader="JSON",
            )
            document_obj.chunks = chunks

            with open(f"data/Video/{document_obj.name}.json", "w") as writer:
                json_obj = Document.to_json(document_obj)
                json.dump(json_obj, writer)
                print(f"Loaded and saved {document_obj.name}")

        except:
            print(f"Failed to fetch transcript for video ID: {video_id}")


def fetch_youtube_transcripts():
    YOUTUBE_API_KEY, CHANNEL_ID = load_configuration()
    video_ids = get_all_video_ids(YOUTUBE_API_KEY, CHANNEL_ID)
    return fetch_transcripts(video_ids)
