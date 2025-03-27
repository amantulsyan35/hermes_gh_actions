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
API_ENDPOINT = os.environ.get("API_ENDPOINT", "https://open-source-content.xyz/v1/youtube")
DB_FILE = "content.sqlite"  # Updated database file name
RATE_LIMIT_SLEEP = 20  # Sleep time in seconds when rate limited
MAX_VIDEOS_TO_PROCESS = 5  # Only process the first 5 videos for testing

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

def fetch_youtube_videos():
    """
    Fetch YouTube videos from the API.
    Returns a list of video data.
    """
    try:
        response = requests.get(API_ENDPOINT)
        
        # Handle rate limiting
        if response.status_code == 429:
            print(f"Rate limited. Sleeping for {RATE_LIMIT_SLEEP} seconds...")
            time.sleep(RATE_LIMIT_SLEEP)
            return fetch_youtube_videos()
        
        if response.status_code != 200:
            print(f"API request failed with status code {response.status_code}")
            print(f"Response: {response.text}")
            return []
        
        data = response.json()
        
        # Handle the format {"data": []}
        videos_data = data.get("data", [])
        
        # Process all videos (no need to filter for YouTube since all are YouTube)
        youtube_videos = []
        for entry in videos_data:
            url = entry.get("url", "")
            video_id = extract_youtube_id(url)
            if video_id:  # Ensure we have a valid YouTube ID
                youtube_videos.append({
                    "video_id": video_id,
                    "title": entry.get("title", ""),
                    "url": url,
                    "created_time": entry.get("createdTime", ""),
                    "content_type": "youtube",
                    # Additional metadata if available
                    "og_title": entry.get("ogTitle", ""),
                    "og_description": entry.get("ogDescription", ""),
                    "og_image": entry.get("ogImage", ""),
                    "keywords": entry.get("keywords", "")
                })
        
        return youtube_videos
    
    except requests.RequestException as e:
        print(f"Error fetching from API: {str(e)}")
        return []

def get_all_youtube_videos():
    """Fetch all YouTube videos from API with testing limit."""
    print(f"Fetching videos from YouTube API endpoint: {API_ENDPOINT}")
    videos = fetch_youtube_videos()
    
    # For testing, limit the number of videos to process
    if len(videos) > MAX_VIDEOS_TO_PROCESS:
        print(f"Limiting to first {MAX_VIDEOS_TO_PROCESS} videos for testing")
        return videos[:MAX_VIDEOS_TO_PROCESS]
    
    return videos

def get_existing_content_urls(conn):
    """Get list of content URLs already in the database."""
    try:
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='content'")
        if not cursor.fetchone():
            return set()
        
        # Get URLs
        cursor.execute("SELECT url FROM content WHERE content_type = 'youtube'")
        urls = set(row[0] for row in cursor.fetchall())
        
        return urls
    except Exception as e:
        print(f"Error getting existing content URLs: {str(e)}")
        return set()

def fetch_youtube_metadata(video_id):
    """Fetch additional metadata for a YouTube video using yt-dlp."""
    try:
        # Create temporary directory for output
        with tempfile.TemporaryDirectory() as temp_dir:
            # Set up cookies path
            cookies_path = os.path.expanduser("~/.config/yt-dlp/cookies.txt")
            
            # Build the command to get video info
            command = [
                "yt-dlp",
                f"https://www.youtube.com/watch?v={video_id}",
                "--skip-download",
                "--dump-json",
                "-o", os.path.join(temp_dir, "%(id)s")
            ]
            
            # Add cookies if they exist
            if os.path.exists(cookies_path):
                command.extend(["--cookies", cookies_path])
            
            # Run the command and capture output
            result = subprocess.run(command, capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"yt-dlp metadata command failed with exit code {result.returncode}")
                if result.stderr:
                    print(f"Error output: {result.stderr}")
                return {}
            
            # Parse JSON response
            try:
                metadata = json.loads(result.stdout)
                
                # Extract relevant fields
                return {
                    "channel_name": metadata.get("channel", ""),
                    "channel_id": metadata.get("channel_id", ""),
                    "description": metadata.get("description", ""),
                    "duration": metadata.get("duration_string", ""),
                    "view_count": metadata.get("view_count", 0),
                    "like_count": metadata.get("like_count", 0),
                    "dislike_count": metadata.get("dislike_count", 0),
                    "comment_count": metadata.get("comment_count", 0),
                    "publish_date": metadata.get("upload_date", ""),
                    "keywords": metadata.get("tags", []),
                }
            except json.JSONDecodeError:
                print(f"Error parsing metadata JSON for {video_id}")
                return {}
                
    except Exception as e:
        print(f"Error fetching metadata for {video_id}: {str(e)}")
        return {}

def fetch_transcript_with_timestamps(video_id):
    """Fetch timestamped transcript for a YouTube video using yt-dlp."""
    try:
        # Create temporary directory for output
        with tempfile.TemporaryDirectory() as temp_dir:
            # Set up cookies path
            cookies_path = os.path.expanduser("~/.config/yt-dlp/cookies.txt")
            
            # Build the command
            command = [
                "yt-dlp",
                f"https://www.youtube.com/watch?v={video_id}",
                "--skip-download",
                "--write-auto-subs",
                "--sub-langs", "en.*",
                "--sub-format", "vtt",
                "--convert-subs", "vtt",
                "-o", os.path.join(temp_dir, "%(id)s"),
                "-v"  # Verbose output for debugging
            ]
            
            # Add cookies if they exist
            if os.path.exists(cookies_path):
                command.extend(["--cookies", cookies_path])
                print(f"Using cookies from: {cookies_path}")
            else:
                print("No cookies file found. YouTube might block the request.")
            
            # Run the command and capture output
            result = subprocess.run(command, capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"yt-dlp command failed with exit code {result.returncode}")
                if result.stderr:
                    print(f"Error output: {result.stderr}")
                return None
            
            # Find the subtitle file
            subtitle_file = None
            for file in os.listdir(temp_dir):
                if file.endswith(".vtt") and video_id in file:
                    subtitle_file = os.path.join(temp_dir, file)
                    break
            
            if not subtitle_file:
                print(f"No subtitle file found for {video_id}")
                return None
            
            # Parse VTT file into text and timestamped format
            plain_transcript, timestamped_segments = parse_vtt_file_with_timestamps(subtitle_file)
            
            # Get duration from last segment if available
            duration = 0
            if timestamped_segments:
                last_segment = timestamped_segments[-1]
                # Convert timestamp (HH:MM:SS.mmm) to seconds
                time_parts = last_segment["end_time"].split(':')
                if len(time_parts) == 3:
                    hours, minutes, seconds = time_parts
                    seconds = float(seconds)
                    duration = int(hours) * 3600 + int(minutes) * 60 + seconds
            
            return {
                "video_id": video_id,
                "full_text": plain_transcript,
                "timestamped_segments": timestamped_segments,
                "duration": duration,
                "language": "en",  # Assuming English
                "fetched_at": datetime.now(timezone.utc).isoformat()
            }
    except Exception as e:
        print(f"Error fetching transcript for {video_id}: {str(e)}")
        return None

def parse_vtt_file_with_timestamps(vtt_file):
    """
    Parse a VTT file into plain text and a timestamped format.
    Returns a tuple of (plain_transcript, timestamped_segments)
    """
    with open(vtt_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    plain_transcript = ""
    timestamped_segments = []
    
    current_start = None
    current_end = None
    current_text = ""
    
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        
        # Skip header lines
        if line == "WEBVTT" or line.startswith("Kind:") or line.startswith("Language:"):
            i += 1
            continue
        
        # Look for timestamp lines
        timestamp_match = re.match(r'(\d{2}:\d{2}:\d{2}\.\d{3}) --> (\d{2}:\d{2}:\d{2}\.\d{3})', line)
        if timestamp_match:
            # If we have collected text from previous timestamp, save it
            if current_start and current_text:
                # Clean the current text
                clean_text = clean_vtt_text(current_text)
                if clean_text:
                    timestamped_segments.append({
                        "start_time": current_start,
                        "end_time": current_end,
                        "text": clean_text
                    })
                    plain_transcript += clean_text + " "
            
            # Set new timestamp info
            current_start = timestamp_match.group(1)
            current_end = timestamp_match.group(2)
            current_text = ""
            
            # Move to next line
            i += 1
            
            # Skip empty lines after timestamp
            while i < len(lines) and not lines[i].strip():
                i += 1
                
            # Collect all text until next timestamp or empty line
            while i < len(lines) and lines[i].strip() and not re.match(r'\d{2}:\d{2}:\d{2}\.\d{3} -->', lines[i]):
                if not lines[i].strip().isdigit():  # Skip cue identifiers
                    current_text += lines[i].strip() + " "
                i += 1
        else:
            i += 1
    
    # Add the last segment if there is one
    if current_start and current_text:
        clean_text = clean_vtt_text(current_text)
        if clean_text:
            timestamped_segments.append({
                "start_time": current_start,
                "end_time": current_end,
                "text": clean_text
            })
            plain_transcript += clean_text + " "
    
    # Clean up the plain transcript
    plain_transcript = re.sub(r'\s+', ' ', plain_transcript).strip()
    
    # Deduplicate segments (YouTube VTT often has overlapping segments)
    timestamped_segments = deduplicate_segments(timestamped_segments)
    
    return plain_transcript, timestamped_segments

def clean_vtt_text(text):
    """Clean VTT text by removing formatting tags and extra whitespace."""
    # Remove timestamp and formatting tags
    text = re.sub(r'<\d{2}:\d{2}:\d{2}\.\d{3}>', '', text)
    text = re.sub(r'</?c>', '', text)
    text = re.sub(r'align:start position:0%', '', text)
    
    # Clean up whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def deduplicate_segments(segments):
    """Remove duplicate or highly overlapping segments."""
    if not segments:
        return []
        
    # Sort by start time
    sorted_segments = sorted(segments, key=lambda x: x["start_time"])
    
    # Deduplicate
    unique_segments = [sorted_segments[0]]
    
    for segment in sorted_segments[1:]:
        last_segment = unique_segments[-1]
        
        # If this segment starts at the same time as the last one, skip it
        if segment["start_time"] == last_segment["start_time"]:
            continue
            
        # If this segment has the exact same text as the last one, skip it
        if segment["text"] == last_segment["text"]:
            continue
            
        # Add this segment
        unique_segments.append(segment)
    
    return unique_segments

def init_database():
    """Initialize the database with the new schema."""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Create tables if they don't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS content (
            url TEXT PRIMARY KEY,
            content_type TEXT,
            title TEXT,
            created_at TEXT,
            consumed_at TEXT,
            last_updated TEXT,
            scrape_at TEXT
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS metadata (
            url TEXT PRIMARY KEY,
            og_title TEXT,
            og_description TEXT,
            og_image TEXT,
            keywords TEXT,
            FOREIGN KEY (url) REFERENCES content(url)
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS youtube (
            url TEXT PRIMARY KEY,
            video_id TEXT,
            channel_name TEXT,
            channel_id TEXT,
            description TEXT,
            duration TEXT,
            view_count INTEGER,
            like_count INTEGER,
            dislike_count INTEGER,
            comment_count INTEGER,
            publish_date TEXT,
            FOREIGN KEY (url) REFERENCES content(url)
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS transcript (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT,
            full_text TEXT,
            language TEXT,
            duration REAL,
            fetched_at TEXT,
            FOREIGN KEY (url) REFERENCES youtube(url)
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS transcript_segments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transcript_id INTEGER,
            start_time TEXT,
            end_time TEXT,
            text TEXT,
            FOREIGN KEY (transcript_id) REFERENCES transcript(id)
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sync_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sync_time TEXT,
            entries_added INTEGER,
            entries_updated INTEGER,
            entries_scraped INTEGER,
            scrape_errors INTEGER,
            sync_type TEXT
        )
        ''')
        
        # Create indices for performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_content_type ON content(content_type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_youtube_video_id ON youtube(video_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_transcript_url ON transcript(url)')
        
        conn.commit()
        return conn
    
    except Exception as e:
        print(f"Error initializing database: {str(e)}")
        if 'conn' in locals():
            conn.close()
        return None

def store_video_data(conn, video_data, metadata, transcript_data):
    """Store all video data in the database using the new schema."""
    try:
        cursor = conn.cursor()
        
        # Insert into content table
        cursor.execute('''
        INSERT OR REPLACE INTO content (
            url, content_type, title, created_at, last_updated, scrape_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            video_data["url"],
            video_data["content_type"],
            video_data["title"],
            video_data["created_time"],
            datetime.now(timezone.utc).isoformat(),
            datetime.now(timezone.utc).isoformat()
        ))
        
        # Insert into metadata table if we have metadata
        if video_data.get("og_title") or video_data.get("og_description") or video_data.get("og_image") or video_data.get("keywords"):
            cursor.execute('''
            INSERT OR REPLACE INTO metadata (
                url, og_title, og_description, og_image, keywords
            ) VALUES (?, ?, ?, ?, ?)
            ''', (
                video_data["url"],
                video_data.get("og_title", ""),
                video_data.get("og_description", ""),
                video_data.get("og_image", ""),
                json.dumps(video_data.get("keywords", "")) if isinstance(video_data.get("keywords"), (list, dict)) else video_data.get("keywords", "")
            ))
        
        # Insert into youtube table
        cursor.execute('''
        INSERT OR REPLACE INTO youtube (
            url, video_id, channel_name, channel_id, description, 
            duration, view_count, like_count, dislike_count, comment_count, publish_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            video_data["url"],
            video_data["video_id"],
            metadata.get("channel_name", ""),
            metadata.get("channel_id", ""),
            metadata.get("description", ""),
            metadata.get("duration", ""),
            metadata.get("view_count", 0),
            metadata.get("like_count", 0),
            metadata.get("dislike_count", 0),
            metadata.get("comment_count", 0),
            metadata.get("publish_date", "")
        ))
        
        # Insert transcript if we have it
        if transcript_data:
            cursor.execute('''
            INSERT INTO transcript (
                url, full_text, language, duration, fetched_at
            ) VALUES (?, ?, ?, ?, ?)
            ''', (
                video_data["url"],
                transcript_data["full_text"],
                transcript_data["language"],
                transcript_data["duration"],
                transcript_data["fetched_at"]
            ))
            
            # Get the transcript ID
            transcript_id = cursor.lastrowid
            
            # Insert transcript segments
            for segment in transcript_data["timestamped_segments"]:
                cursor.execute('''
                INSERT INTO transcript_segments (
                    transcript_id, start_time, end_time, text
                ) VALUES (?, ?, ?, ?)
                ''', (
                    transcript_id,
                    segment["start_time"],
                    segment["end_time"],
                    segment["text"]
                ))
        
        conn.commit()
        return True
        
    except Exception as e:
        print(f"Error storing video data: {str(e)}")
        conn.rollback()
        return False

def update_sync_history(conn, added, updated, scraped, errors):
    """Update the sync history table."""
    try:
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO sync_history (
            sync_time, entries_added, entries_updated, entries_scraped, scrape_errors, sync_type
        ) VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            datetime.now(timezone.utc).isoformat(),
            added,
            updated,
            scraped,
            errors,
            "youtube"
        ))
        
        conn.commit()
    except Exception as e:
        print(f"Error updating sync history: {str(e)}")
        conn.rollback()

def export_to_json(videos_processed):
    """Export processing results to a JSON file for easier viewing."""
    try:
        os.makedirs("data", exist_ok=True)
        
        # Create JSON file with the processed videos
        with open(os.path.join("data", "processed_videos.json"), 'w', encoding='utf-8') as f:
            json.dump(
                {
                    "count": len(videos_processed),
                    "videos": videos_processed,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }, 
                f, 
                indent=2, 
                ensure_ascii=False
            )
        
        print(f"Exported {len(videos_processed)} processed videos to data/processed_videos.json")
    except Exception as e:
        print(f"Error exporting to JSON: {str(e)}")

def main():
    try:
        print(f"*** TESTING MODE: Processing only first {MAX_VIDEOS_TO_PROCESS} videos ***")
        
        # Initialize database with new schema
        conn = init_database()
        if not conn:
            raise Exception("Failed to initialize database")
        
        # Fetch YouTube videos from API
        print("Fetching YouTube videos from API...")
        videos = get_all_youtube_videos()
        print(f"Found {len(videos)} YouTube videos for testing")
        
        # Get list of content URLs already in database
        print("Getting list of content already in database...")
        existing_urls = get_existing_content_urls(conn)
        print(f"Found {len(existing_urls)} existing content items in database")
        
        # Filter out videos that are already in the database
        new_videos = [video for video in videos if video["url"] not in existing_urls]
        updating_videos = [video for video in videos if video["url"] in existing_urls]
        
        print(f"Found {len(new_videos)} new videos to process")
        print(f"Found {len(updating_videos)} existing videos that might need updates")
        
        # Check if cookies file exists
        cookies_path = os.path.expanduser("~/.config/yt-dlp/cookies.txt")
        if os.path.exists(cookies_path):
            print(f"Found cookies file at {cookies_path}")
        else:
            print("WARNING: No cookies file found. YouTube might block the request.")
        
        # Process videos
        videos_to_process = new_videos + updating_videos
        
        # Initialize counters
        added_count = 0
        updated_count = 0
        scraped_count = 0
        error_count = 0
        processed_videos = []
        
        for i, video in enumerate(videos_to_process):
            video_id = video["video_id"]
            is_new = video["url"] not in existing_urls
            
            print(f"Processing video {i+1}/{len(videos_to_process)}: {video_id} - {video['title']}")
            
            try:
                # Fetch additional metadata
                print(f"Fetching metadata for {video_id}...")
                metadata = fetch_youtube_metadata(video_id)
                
                # Fetch transcript
                print(f"Fetching transcript for {video_id}...")
                transcript_data = fetch_transcript_with_timestamps(video_id)
                
                # Store in database
                success = store_video_data(conn, video, metadata, transcript_data)
                
                if success:
                    if is_new:
                        added_count += 1
                    else:
                        updated_count += 1
                    
                    if transcript_data:
                        scraped_count += 1
                    
                    # Add to processed videos
                    processed_videos.append({
                        "video_id": video_id,
                        "url": video["url"],
                        "title": video["title"],
                        "transcript_fetched": transcript_data is not None,
                        "processed_at": datetime.now(timezone.utc).isoformat()
                    })
                    
                    print(f"Successfully processed {video_id}")
                else:
                    error_count += 1
                    print(f"Failed to store data for {video_id}")
            
            except Exception as e:
                error_count += 1
                print(f"Error processing {video_id}: {str(e)}")
            
            # Avoid rate limiting
            if i < len(videos_to_process) - 1:  # Don't sleep after the last video
                time.sleep(2)
        
        # Update sync history
        update_sync_history(conn, added_count, updated_count, scraped_count, error_count)
        
        # Export results
        if processed_videos:
            export_to_json(processed_videos)
        
        # Close database connection
        conn.close()
        
        print("\nProcessing complete!")
        print(f"Added: {added_count}, Updated: {updated_count}, Scraped: {scraped_count}, Errors: {error_count}")
        
    except Exception as e:
        print(f"Error in main function: {str(e)}")
        if 'conn' in locals() and conn:
            conn.close()
        raise

if __name__ == "__main__":
    main()