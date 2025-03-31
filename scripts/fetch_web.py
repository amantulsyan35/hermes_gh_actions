import os
import sqlite3
import libsql_experimental as libsql
import json
from datetime import datetime
import sys
import requests
from bs4 import BeautifulSoup

def create_turso_tables(conn):
    """
    Create the necessary tables in Turso database
    """
    print("Creating tables in Turso database...")
    
    # Create content table
    conn.execute('''
    CREATE TABLE IF NOT EXISTS content (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT UNIQUE,
        content_type TEXT,
        title TEXT,
        created_at TEXT,
        consumed_at TEXT,
        last_updated_at TEXT,
        scraped_at TEXT,
        is_scraped INTEGER DEFAULT 0
    )
    ''')
    
    # Create metadata table
    conn.execute('''
    CREATE TABLE IF NOT EXISTS metadata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        content_id INTEGER,
        og_title TEXT,
        og_description TEXT,
        og_image TEXT,
        keywords TEXT,
        FOREIGN KEY (content_id) REFERENCES content(id)
    )
    ''')
    
    # Create web table
    conn.execute('''
    CREATE TABLE IF NOT EXISTS web (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        content_id INTEGER,
        url TEXT,
        published_at TEXT,
        full_content TEXT,
        FOREIGN KEY (content_id) REFERENCES content(id)
    )
    ''')
    
    # Create YouTube tables - for future use
    conn.execute('''
    CREATE TABLE IF NOT EXISTS youtube (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        content_id INTEGER,
        url TEXT,
        video_id TEXT,
        channel_name TEXT,
        description TEXT,
        duration TEXT,
        FOREIGN KEY (content_id) REFERENCES content(id)
    )
    ''')
    
    conn.execute('''
    CREATE TABLE IF NOT EXISTS transcript (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        youtube_id INTEGER,
        full_text TEXT,
        language TEXT,
        duration REAL,
        fetched_at TEXT,
        FOREIGN KEY (youtube_id) REFERENCES youtube(id)
    )
    ''')
    
    conn.execute('''
    CREATE TABLE IF NOT EXISTS transcript_segments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        transcript_id INTEGER,
        start_time TEXT,
        end_time TEXT,
        text TEXT,
        FOREIGN KEY (transcript_id) REFERENCES transcript(id)
    )
    ''')
    
    # Create sync_history table
    conn.execute('''
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
    
    print("Tables created successfully.")

def fetch_web_content(api_endpoint):
    """
    Fetch web content from the API
    """
    print(f"Fetching web content from {api_endpoint}...")
    try:
        response = requests.get(api_endpoint)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching content: {str(e)}")
        return []

def process_and_store_content(content_data, turso_conn):
    """
    Process fetched content and store in Turso
    """
    entries_added = 0
    scrape_errors = 0
    
    for item in content_data:
        try:
            url = item.get('url')
            if not url:
                continue
                
            # Check if URL already exists
            existing = turso_conn.execute("SELECT id FROM content WHERE url = ?", (url,)).fetchone()
            if existing:
                # Update existing content if needed
                continue
                
            # Add new content entry
            current_time = datetime.now().isoformat()
            turso_conn.execute('''
            INSERT INTO content (url, content_type, title, created_at)
            VALUES (?, ?, ?, ?)
            ''', (url, 'web', item.get('title', ''), current_time))
            
            # Get the content_id
            content_id = turso_conn.execute("SELECT id FROM content WHERE url = ?", (url,)).fetchone()[0]
            
            # Add to web table
            turso_conn.execute('''
            INSERT INTO web (content_id, url, full_content)
            VALUES (?, ?, ?)
            ''', (content_id, url, item.get('content', '')))
            
            # Add metadata if available
            if 'metadata' in item and isinstance(item['metadata'], dict):
                meta = item['metadata']
                turso_conn.execute('''
                INSERT INTO metadata (content_id, og_title, og_description, og_image, keywords)
                VALUES (?, ?, ?, ?, ?)
                ''', (
                    content_id,
                    meta.get('og_title', ''),
                    meta.get('og_description', ''),
                    meta.get('og_image', ''),
                    json.dumps(meta.get('keywords', []))
                ))
            
            entries_added += 1
            
        except Exception as e:
            print(f"Error processing item {url}: {str(e)}")
            scrape_errors += 1
    
    # Add to sync history
    current_time = datetime.now().isoformat()
    turso_conn.execute('''
    INSERT INTO sync_history (
        sync_time, entries_added, entries_updated, entries_scraped, scrape_errors, sync_type
    ) VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        current_time,
        entries_added,
        0,
        0,
        scrape_errors,
        "api_fetch"
    ))
    
    return entries_added, scrape_errors

def main():
    # Check environment variables or use defaults
    API_ENDPOINT = os.environ.get("API_ENDPOINT", "https://open-source-content.xyz/v1/web")
    TURSO_URL = os.environ.get("TURSO_URL", "libsql://context-amantulsyan35.aws-us-east-1.turso.io")
    TURSO_AUTH_TOKEN = os.environ.get("TURSO_AUTH_TOKEN")
    
    # Validate that we have the Turso auth token
    if not TURSO_AUTH_TOKEN:
        print("Error: TURSO_AUTH_TOKEN environment variable is required")
        sys.exit(1)
    
    # Connect to Turso 
    print(f"Connecting to Turso database: {TURSO_URL}")
    # The local.db is just a local cache file
    turso_conn = libsql.connect("local.db", sync_url=TURSO_URL, auth_token=TURSO_AUTH_TOKEN)
    
    try:
        # Create Turso tables if they don't exist
        create_turso_tables(turso_conn)
        
        # Fetch web content
        content_data = fetch_web_content(API_ENDPOINT)
        
        # Process and store content
        if content_data:
            entries_added, scrape_errors = process_and_store_content(content_data, turso_conn)
            print(f"Added {entries_added} new entries with {scrape_errors} errors")
        else:
            print("No content fetched from API")
        
        # Sync changes to Turso
        print("Syncing changes to Turso...")
        turso_conn.sync()
        print("Sync completed successfully.")
        
        # Validate by counting rows
        content_count = turso_conn.execute("SELECT COUNT(*) FROM content").fetchone()[0]
        print(f"Current content entries in database: {content_count}")
            
    except Exception as e:
        print(f"Error during execution: {str(e)}")
        sys.exit(1)
    finally:
        # Close connection
        turso_conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()