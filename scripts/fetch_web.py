import os
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

def process_and_store_content(content_data, turso_conn):
    """
    Process fetched content and store in Turso
    """
    entries_added = 0
    scrape_errors = 0
    
    # Handle different data formats
    if not content_data:
        print("No content data to process")
        return 0, 0
    
    # Extract URLs from {"data": [urls]} format if needed
    if isinstance(content_data, dict) and 'data' in content_data and isinstance(content_data['data'], list):
        content_data = [{"url": url} for url in content_data['data'] if isinstance(url, str)]
        
    # Check if content_data is a list or dict
    if isinstance(content_data, str):
        # Handle string data (possibly a URL list)
        try:
            # Try to parse as JSON string first
            content_data = json.loads(content_data)
        except json.JSONDecodeError:
            # Split by lines if it's a text list of URLs
            content_data = [{"url": line.strip()} for line in content_data.split('\n') if line.strip()]
    
    # Ensure we have a list to iterate
    if not isinstance(content_data, list):
        content_data = [content_data]
    
    for item in content_data:
        url = None
        try:
            # Handle different item formats
            if isinstance(item, dict):
                url = item.get('url')
            elif isinstance(item, str):
                url = item
                
            if not url:
                continue
                
            print(f"Processing URL: {url}")
                
            # Check if URL already exists
            existing = turso_conn.execute("SELECT id FROM content WHERE url = ?", (url,)).fetchone()
            if existing:
                print(f"URL already exists: {url}")
                continue
                
            # Fetch the content if needed
            try:
                content = ""
                title = ""
                if isinstance(item, str) or (isinstance(item, dict) and 'content' not in item):
                    print(f"Fetching content for {url}")
                    page_response = requests.get(url, timeout=10)
                    if page_response.status_code == 200:
                        soup = BeautifulSoup(page_response.text, 'html.parser')
                        # Fix the NoneType error for title
                        if soup.title and soup.title.string:
                            title = soup.title.string
                        else:
                            title = url  # Use URL as fallback title
                        content = page_response.text
                else:
                    title = item.get('title', '')
                    content = item.get('content', '')
            except Exception as e:
                print(f"Error fetching content for {url}: {e}")
                content = ""
                title = url  # Use URL as fallback title
            
            # Add new content entry
            current_time = datetime.now().isoformat()
            turso_conn.execute('''
            INSERT INTO content (url, content_type, title, created_at)
            VALUES (?, ?, ?, ?)
            ''', (url, 'web', title, current_time))
            
            # Commit after each insert to avoid transaction timeouts
            try:
                turso_conn.sync()
            except Exception as e:
                print(f"Warning: Sync after content insert failed: {e}")
            
            # Get the content_id
            content_id = turso_conn.execute("SELECT id FROM content WHERE url = ?", (url,)).fetchone()[0]
            
            # Add to web table
            turso_conn.execute('''
            INSERT INTO web (content_id, url, full_content)
            VALUES (?, ?, ?)
            ''', (content_id, url, content))
            
            # Commit after each web insert
            try:
                turso_conn.sync()
            except Exception as e:
                print(f"Warning: Sync after web insert failed: {e}")
            
            # Add metadata if available
            meta = {}
            if isinstance(item, dict) and 'metadata' in item:
                meta = item['metadata']
            
            turso_conn.execute('''
            INSERT INTO metadata (content_id, og_title, og_description, og_image, keywords)
            VALUES (?, ?, ?, ?, ?)
            ''', (
                content_id,
                meta.get('og_title', '') if isinstance(meta, dict) else '',
                meta.get('og_description', '') if isinstance(meta, dict) else '',
                meta.get('og_image', '') if isinstance(meta, dict) else '',
                json.dumps(meta.get('keywords', [])) if isinstance(meta, dict) else '[]'
            ))
            
            # Commit after each metadata insert
            try:
                turso_conn.sync()
            except Exception as e:
                print(f"Warning: Sync after metadata insert failed: {e}")
            
            entries_added += 1
            print(f"Added entry for {url}")
            
        except Exception as e:
            print(f"Error processing item {url if url else 'unknown'}: {str(e)}")
            scrape_errors += 1
            
            # Try to reconnect to Turso if we get a specific error
            if "stream not found" in str(e):
                print("Attempting to reconnect to Turso database...")
                try:
                    # Recreate the connection
                    turso_conn = libsql.connect("local.db", 
                                             sync_url=os.environ.get("TURSO_URL"), 
                                             auth_token=os.environ.get("TURSO_AUTH_TOKEN"))
                    print("Successfully reconnected to Turso database")
                except Exception as conn_err:
                    print(f"Failed to reconnect: {conn_err}")
    
    # Add to sync history
    try:
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
        
        # Final sync
        turso_conn.sync()
    except Exception as e:
        print(f"Error adding sync history: {e}")
    
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
    turso_conn = None
    
    try:
        turso_conn = libsql.connect("local.db", sync_url=TURSO_URL, auth_token=TURSO_AUTH_TOKEN)
        
        # Create Turso tables if they don't exist
        create_turso_tables(turso_conn)
        
        # Fetch web content
        response = requests.get(API_ENDPOINT)
        print(f"Fetching web content from {API_ENDPOINT}...")
        print(f"API Response (first 100 chars): {response.text[:100]}...")
        
        # Parse the response
        try:
            content_data = response.json()
        except:
            print("Failed to parse JSON response")
            content_data = []
        
        # Process and store content
        if content_data:
            entries_added, scrape_errors = process_and_store_content(content_data, turso_conn)
            print(f"Added {entries_added} new entries with {scrape_errors} errors")
        else:
            print("No content fetched from API")
        
        # Final sync
        try:
            print("Syncing changes to Turso...")
            turso_conn.sync()
            print("Sync completed successfully.")
        except Exception as e:
            print(f"Error during final sync: {e}")
        
        # Validate by counting rows
        try:
            content_count = turso_conn.execute("SELECT COUNT(*) FROM content").fetchone()[0]
            print(f"Current content entries in database: {content_count}")
        except Exception as e:
            print(f"Error counting entries: {e}")
            
    except Exception as e:
        print(f"Error during execution: {str(e)}")
        if turso_conn:
            try:
                turso_conn.sync()
            except:
                pass
        sys.exit(1)
    finally:
        print("Database operation completed.")

if __name__ == "__main__":
    main()