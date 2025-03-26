import os
import json
import subprocess
import requests
import time
import sqlite3
import tempfile
import re
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs

# Configuration
API_ENDPOINT = os.environ.get("API_ENDPOINT", "https://open-source-content.xyz/v1")
CLOUDFLARE_API_TOKEN = os.environ.get("CLOUDFLARE_API_TOKEN")
CLOUDFLARE_ACCOUNT_ID = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
DATABASE_NAME = os.environ.get("DATABASE_NAME", "youtube_transcripts")
PAGE_SIZE = 20
RATE_LIMIT_SLEEP = 20  # Sleep time in seconds when rate limited

def extract_youtube_id(url):
    """Extract YouTube video ID from a URL."""
    parsed_url = urlparse(url)
    
    if 'youtube.com' in parsed_url.netloc:
        if parsed_url.path == '/watch':
            return parse_qs(parsed_url.query).get('v', [None])[0]
        elif parsed_url.path.startswith('/embed/'):
            return parsed_url.path.split('/')[2]
        elif parsed_url.path.startswith('/v/'):
            return parsed_url.path.split('/')[2]
    elif 'youtu.be' in parsed_url.netloc:
        return parsed_url.path[1:]
    return None

def fetch_youtube_videos_from_api(cursor=None):
    """
    Fetch YouTube videos from the API with pagination support.
    Returns a tuple of (videos, next_cursor, has_more)
    """
    url = API_ENDPOINT
    params = {"pageSize": PAGE_SIZE}
    
    if cursor:
        params["cursor"] = cursor
    
    try:
        response = requests.get(url, params=params)
        
        # Handle rate limiting
        if response.status_code == 429:
            print(f"Rate limited. Sleeping for {RATE_LIMIT_SLEEP} seconds...")
            time.sleep(RATE_LIMIT_SLEEP)
            return fetch_youtube_videos_from_api(cursor)
        
        if response.status_code != 200:
            print(f"API request failed with status code {response.status_code}")
            print(f"Response: {response.text}")
            return [], None, False
        
        data = response.json()
        
        # Filter only YouTube videos
        youtube_videos = []
        for entry in data.get("entries", []):
            url = entry.get("url", "")
            if "youtube.com" in url or "youtu.be" in url:
                video_id = extract_youtube_id(url)
                if video_id:
                    youtube_videos.append({
                        "video_id": video_id,
                        "title": entry.get("title", ""),
                        "url": url,
                        "created_time": entry.get("createdTime", "")
                    })
        
        next_cursor = data.get("nextCursor")
        has_more = data.get("hasMore", False)
        
        return youtube_videos, next_cursor, has_more
    
    except requests.RequestException as e:
        print(f"Error fetching from API: {str(e)}")
        return [], None, False

def fetch_all_youtube_videos():
    """Fetch all YouTube videos from API using pagination."""
    all_videos = []
    cursor = None
    has_more = True
    
    while has_more:
        print(f"Fetching videos with cursor: {cursor}")
        videos, cursor, has_more = fetch_youtube_videos_from_api(cursor)
        all_videos.extend(videos)
        
        # Respect rate limits
        if has_more:
            time.sleep(2)
    
    return all_videos

def get_existing_video_ids():
    """Get list of video IDs already in the database."""
    # Create a temporary wrangler config
    with tempfile.NamedTemporaryFile(suffix='.json', mode='w') as config_file:
        wrangler_config = {
            "d1_databases": [
                {
                    "binding": "DB",
                    "database_name": DATABASE_NAME,
                    "database_id": f"{CLOUDFLARE_ACCOUNT_ID}/{DATABASE_NAME}"
                }
            ]
        }
        json.dump(wrangler_config, config_file)
        config_file.flush()
        
        # Use Wrangler to get existing video IDs
        try:
            command = [
                "wrangler",
                "d1",
                "execute",
                DATABASE_NAME,
                "--command", "SELECT video_id FROM transcripts",
                "--json",
                "--config", config_file.name
            ]
            
            env = os.environ.copy()
            env["CLOUDFLARE_API_TOKEN"] = CLOUDFLARE_API_TOKEN
            env["CLOUDFLARE_ACCOUNT_ID"] = CLOUDFLARE_ACCOUNT_ID
            
            result = subprocess.run(
                command, 
                env=env, 
                capture_output=True, 
                text=True
            )
            
            if result.returncode != 0:
                print(f"Error getting existing video IDs: {result.stderr}")
                return set()
            
            try:
                data = json.loads(result.stdout)
                # Extract video_id from results
                return set(row["video_id"] for row in data.get("results", []))
            except json.JSONDecodeError:
                print(f"Error parsing JSON: {result.stdout}")
                return set()
                
        except Exception as e:
            print(f"Error getting existing video IDs: {str(e)}")
            return set()

def fetch_transcript(video_id):
    """Fetch transcript for a YouTube video using yt-dlp."""
    try:
        # Create temporary directory for output
        with tempfile.TemporaryDirectory() as temp_dir:
            # Run yt-dlp to get the video info including subtitles
            command = [
                "yt-dlp",
                f"https://www.youtube.com/watch?v={video_id}",
                "--skip-download",
                "--write-auto-subs",
                "--sub-langs", "en.*",
                "--sub-format", "vtt",
                "--convert-subs", "vtt",
                "-o", os.path.join(temp_dir, "%(id)s"),
                "--quiet"
            ]
            
            subprocess.run(command, check=True)
            
            # Find the subtitle file
            subtitle_file = None
            for file in os.listdir(temp_dir):
                if file.endswith(".vtt") and video_id in file:
                    subtitle_file = os.path.join(temp_dir, file)
                    break
            
            if not subtitle_file:
                print(f"No subtitle file found for {video_id}")
                return None
            
            # Parse VTT file into text
            transcript = parse_vtt_file(subtitle_file)
            
            return {
                "video_id": video_id,
                "transcript": transcript,
                "fetched_at": datetime.now(timezone.utc).isoformat()
            }
    except Exception as e:
        print(f"Error fetching transcript for {video_id}: {str(e)}")
        return None

def parse_vtt_file(vtt_file):
    """Parse a VTT file into plain text."""
    transcript = ""
    with open(vtt_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Skip header and timing information
    start_parsing = False
    current_line = ""
    
    for line in lines:
        line = line.strip()
        
        # Skip WEBVTT header
        if line == "WEBVTT" or line.startswith("Kind:") or line.startswith("Language:"):
            continue
        
        # Skip empty lines
        if not line:
            continue
        
        # Skip timestamp lines
        if re.match(r'\d{2}:\d{2}:\d{2}\.\d{3} --> \d{2}:\d{2}:\d{2}\.\d{3}', line):
            start_parsing = True
            # If we have accumulated text, add it to transcript
            if current_line:
                transcript += current_line + " "
                current_line = ""
            continue
        
        # If we've started parsing and this isn't a cue identifier (number)
        if start_parsing and not line.isdigit():
            current_line += " " + line if current_line else line
    
    # Add the last line
    if current_line:
        transcript += current_line
    
    # Clean up the transcript
    transcript = re.sub(r'\s+', ' ', transcript).strip()
    
    return transcript

def store_in_d1_database(transcripts):
    """Store transcripts in Cloudflare D1 database."""
    if not transcripts:
        print("No transcripts to store.")
        return
    
    # Create a temporary SQLite database
    with tempfile.NamedTemporaryFile(suffix='.sqlite') as temp_db:
        conn = sqlite3.connect(temp_db.name)
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS transcripts (
            video_id TEXT PRIMARY KEY,
            transcript TEXT,
            fetched_at TEXT
        )
        ''')
        
        # Insert or update transcripts
        for transcript in transcripts:
            if transcript:
                cursor.execute('''
                INSERT OR REPLACE INTO transcripts (video_id, transcript, fetched_at)
                VALUES (?, ?, ?)
                ''', (
                    transcript["video_id"],
                    transcript["transcript"],
                    transcript["fetched_at"]
                ))
        
        conn.commit()
        conn.close()
        
        # Create a temporary wrangler config
        with tempfile.NamedTemporaryFile(suffix='.json', mode='w') as config_file:
            wrangler_config = {
                "d1_databases": [
                    {
                        "binding": "DB",
                        "database_name": DATABASE_NAME,
                        "database_id": f"{CLOUDFLARE_ACCOUNT_ID}/{DATABASE_NAME}"
                    }
                ]
            }
            json.dump(wrangler_config, config_file)
            config_file.flush()
            
            # Use Wrangler to upload the SQLite database to D1
            try:
                command = [
                    "wrangler",
                    "d1",
                    "execute",
                    DATABASE_NAME,
                    "--file", temp_db.name,
                    "--config", config_file.name
                ]
                
                env = os.environ.copy()
                env["CLOUDFLARE_API_TOKEN"] = CLOUDFLARE_API_TOKEN
                env["CLOUDFLARE_ACCOUNT_ID"] = CLOUDFLARE_ACCOUNT_ID
                
                result = subprocess.run(command, env=env, check=True)
                print(f"Successfully stored {len(transcripts)} transcripts in D1 database.")
                
            except subprocess.CalledProcessError as e:
                print(f"Error storing transcripts in D1: {str(e)}")

def main():
    try:
        # Create D1 database table if it doesn't exist
        with tempfile.NamedTemporaryFile(suffix='.json', mode='w') as config_file:
            wrangler_config = {
                "d1_databases": [
                    {
                        "binding": "DB",
                        "database_name": DATABASE_NAME,
                        "database_id": f"{CLOUDFLARE_ACCOUNT_ID}/{DATABASE_NAME}"
                    }
                ]
            }
            json.dump(wrangler_config, config_file)
            config_file.flush()
            
            command = [
                "wrangler",
                "d1",
                "execute",
                DATABASE_NAME,
                "--command", "CREATE TABLE IF NOT EXISTS transcripts (video_id TEXT PRIMARY KEY, transcript TEXT, fetched_at TEXT)",
                "--config", config_file.name
            ]
            
            env = os.environ.copy()
            env["CLOUDFLARE_API_TOKEN"] = CLOUDFLARE_API_TOKEN
            env["CLOUDFLARE_ACCOUNT_ID"] = CLOUDFLARE_ACCOUNT_ID
            
            subprocess.run(command, env=env, check=True)
        
        # Fetch all YouTube videos from API
        print("Fetching YouTube videos from API...")
        videos = fetch_all_youtube_videos()
        print(f"Found {len(videos)} YouTube videos")
        
        # Get list of videos already in database to avoid duplicates
        print("Getting list of videos already in database...")
        existing_video_ids = get_existing_video_ids()
        print(f"Found {len(existing_video_ids)} existing videos in database")
        
        # Filter out videos that already have transcripts
        new_videos = [video for video in videos if video["video_id"] not in existing_video_ids]
        print(f"Found {len(new_videos)} new videos to process")
        
        # Fetch transcripts for each video
        transcripts = []
        for i, video in enumerate(new_videos):
            video_id = video["video_id"]
            print(f"Fetching transcript for video {i+1}/{len(new_videos)}: {video_id} - {video['title']}")
            transcript = fetch_transcript(video_id)
            if transcript:
                transcripts.append(transcript)
                print(f"Successfully fetched transcript for {video_id}")
            else:
                print(f"Failed to fetch transcript for {video_id}")
            
            # Avoid rate limiting
            if i < len(new_videos) - 1:  # Don't sleep after the last video
                time.sleep(2)
        
        # Store transcripts in D1 database
        print(f"Storing {len(transcripts)} transcripts in D1 database...")
        store_in_d1_database(transcripts)
        print("Done!")
        
    except Exception as e:
        print(f"Error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main()