import os
import json
import sqlite3
import requests
import time
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import re

# Configuration
API_ENDPOINT = os.environ.get("API_ENDPOINT", "https://open-source-content.xyz/v1/web")
DB_FILE = "content.sqlite"  # Same database file as YouTube script
RATE_LIMIT_SLEEP = 10  # Sleep time in seconds when rate limited
MAX_LINKS_TO_PROCESS = 10  # Limit for testing

def fetch_web_links():
    """
    Fetch web links from the API.
    The API returns {"data": [url1, url2, ...]} where each URL is a string.
    """
    try:
        print(f"Fetching from API endpoint: {API_ENDPOINT}")
        response = requests.get(API_ENDPOINT)
        
        # Handle rate limiting
        if response.status_code == 429:
            print(f"Rate limited. Sleeping for {RATE_LIMIT_SLEEP} seconds...")
            time.sleep(RATE_LIMIT_SLEEP)
            return fetch_web_links()
        
        if response.status_code != 200:
            print(f"API request failed with status code {response.status_code}")
            print(f"Response: {response.text}")
            return []
        
        # Parse JSON response
        data = response.json()
        
        # Get the array of web URLs
        web_urls = data.get("data", [])
        print(f"Received {len(web_urls)} web URLs from API")
        
        # Process each URL to extract basic information
        web_links = []
        
        for url in web_urls:
            if url and is_valid_url(url) and not is_youtube_url(url):
                web_links.append({
                    "url": url,
                    "title": f"Web page: {url}",  # Default title, will be replaced
                    "created_time": datetime.now(timezone.utc).isoformat(),
                    "content_type": "web"
                })
        
        print(f"Extracted {len(web_links)} valid web links")
        return web_links
    
    except requests.RequestException as e:
        print(f"Error fetching from API: {str(e)}")
        return []
    except json.JSONDecodeError as e:
        print(f"Error parsing API response as JSON: {str(e)}")
        print(f"Raw response: {response.text[:200]}...")  # Print first 200 chars of response
        return []

def is_valid_url(url):
    """Check if a URL is valid."""
    if not isinstance(url, str):
        return False
    
    # Simple URL validation with regex
    pattern = re.compile(
        r'^(?:http|https)://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|' # domain
        r'localhost|' # localhost
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # or IP
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    
    return bool(pattern.match(url))

def is_youtube_url(url):
    """Check if a URL is a YouTube URL."""
    return 'youtube.com' in url or 'youtu.be' in url

def get_all_web_links():
    """Fetch all web links from API with testing limit."""
    print(f"Fetching links from web API endpoint: {API_ENDPOINT}")
    links = fetch_web_links()
    
    # For testing, limit the number of links to process
    if len(links) > MAX_LINKS_TO_PROCESS:
        print(f"Limiting to first {MAX_LINKS_TO_PROCESS} links for testing")
        return links[:MAX_LINKS_TO_PROCESS]
    
    return links

def get_existing_content_urls(conn):
    """Get list of content URLs already in the database."""
    try:
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='content'")
        if not cursor.fetchone():
            return set()
        
        # Get URLs
        cursor.execute("SELECT url FROM content WHERE content_type = 'web'")
        urls = set(row[0] for row in cursor.fetchall())
        
        return urls
    except Exception as e:
        print(f"Error getting existing content URLs: {str(e)}")
        return set()

def scrape_web_page(url):
    """Scrape content from a web page."""
    try:
        print(f"Scraping web page: {url}")
        
        # Use a browser-like user agent
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract metadata
        metadata = {
            "og_title": "",
            "og_description": "",
            "og_image": "",
            "keywords": ""
        }
        
        # Get Open Graph title
        og_title_tag = soup.find('meta', property='og:title')
        if og_title_tag and og_title_tag.get('content'):
            metadata["og_title"] = og_title_tag.get('content')
        
        # Get Open Graph description
        og_desc_tag = soup.find('meta', property='og:description')
        if og_desc_tag and og_desc_tag.get('content'):
            metadata["og_description"] = og_desc_tag.get('content')
        
        # Get Open Graph image
        og_image_tag = soup.find('meta', property='og:image')
        if og_image_tag and og_image_tag.get('content'):
            metadata["og_image"] = og_image_tag.get('content')
        
        # Get keywords
        keywords_tag = soup.find('meta', attrs={'name': 'keywords'})
        if keywords_tag and keywords_tag.get('content'):
            metadata["keywords"] = keywords_tag.get('content')
        
        # Get title
        title = soup.title.string if soup.title else ""
        if not title and soup.find('h1'):
            title = soup.find('h1').text.strip()
        
        if not title:
            title = metadata.get("og_title", url)
        
        # Get published date
        published_at = None
        date_meta = soup.find('meta', property='article:published_time')
        if date_meta and date_meta.get('content'):
            published_at = date_meta.get('content')
        
        if not published_at:
            date_meta = soup.find('meta', attrs={'name': 'date'})
            if date_meta and date_meta.get('content'):
                published_at = date_meta.get('content')
        
        if not published_at and soup.find('time'):
            time_tag = soup.find('time')
            if time_tag.get('datetime'):
                published_at = time_tag.get('datetime')
        
        # Extract full content (meaningful text)
        # Remove scripts, styles, and navigation elements
        for script in soup(["script", "style", "nav", "footer", "header", "aside"]):
            script.decompose()
        
        # Get all text content with some structure
        full_content = ""
        
        # Extract headings
        for heading in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            tag_name = heading.name
            level = int(tag_name[1])
            prefix = '#' * level + ' '
            full_content += prefix + heading.get_text().strip() + '\n\n'
        
        # Extract paragraphs and other content elements
        for elem in soup.find_all(['p', 'article', 'section', 'div', 'main', 'li', 'blockquote']):
            text = elem.get_text().strip()
            if text and len(text) > 10:  # Skip very short elements
                full_content += text + '\n\n'
        
        # Clean up the content
        full_content = re.sub(r'\n{3,}', '\n\n', full_content)  # Replace 3+ newlines with 2
        full_content = re.sub(r'\s{2,}', ' ', full_content)  # Replace multiple spaces with one
        full_content = full_content.strip()
        
        result = {
            "url": url,
            "title": title,
            "published_at": published_at,
            "full_content": full_content,
            "metadata": metadata
        }
        
        return result
    
    except Exception as e:
        print(f"Error scraping web page {url}: {str(e)}")
        # Return a basic structure even on error
        return {
            "url": url,
            "title": f"Error: {url}",
            "published_at": None,
            "full_content": f"Failed to retrieve content from this page. Error: {str(e)}",
            "metadata": {
                "og_title": "",
                "og_description": "",
                "og_image": "",
                "keywords": ""
            }
        }

def init_database():
    """Initialize the database with all required tables."""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Create content table if it doesn't exist
        cursor.execute('''
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
        
        # Create metadata table if it doesn't exist
        cursor.execute('''
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
        
        # Create web table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS web (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content_id INTEGER,
            url TEXT,
            published_at TEXT,
            full_content TEXT,
            FOREIGN KEY (content_id) REFERENCES content(id)
        )
        ''')
        
        # Create sync_history table if it doesn't exist
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
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_content_url ON content(url)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_web_content_id ON web(content_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_web_url ON web(url)')
        
        conn.commit()
        print("Database initialized with all required tables and indices")
        return conn
    
    except Exception as e:
        print(f"Error initializing database: {str(e)}")
        if 'conn' in locals():
            conn.close()
        return None

def store_web_data(conn, web_data):
    """Store web data in the database."""
    try:
        cursor = conn.cursor()
        
        # Check if URL already exists in the database
        cursor.execute("SELECT id FROM content WHERE url = ?", (web_data["url"],))
        existing_content = cursor.fetchone()
        
        # Mark as scraped if we have content
        is_scraped = 1 if web_data["full_content"] and "Failed to retrieve content" not in web_data["full_content"] else 0
        
        # Get the current time in ISO format
        current_time = datetime.now(timezone.utc).isoformat()
        
        if existing_content:
            content_id = existing_content[0]
            # Update existing content
            cursor.execute('''
            UPDATE content SET
                content_type = ?,
                title = ?,
                last_updated_at = ?,
                scraped_at = ?,
                is_scraped = ?
            WHERE id = ?
            ''', (
                "web",
                web_data["title"],
                current_time,
                current_time,
                is_scraped,
                content_id
            ))
        else:
            # Insert new content
            cursor.execute('''
            INSERT INTO content (
                url, content_type, title, created_at, last_updated_at, scraped_at, is_scraped
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                web_data["url"],
                "web",
                web_data["title"],
                web_data.get("created_time", current_time),
                current_time,
                current_time,
                is_scraped
            ))
            content_id = cursor.lastrowid
        
        # Handle metadata
        if web_data["metadata"]:
            # Check if metadata exists
            cursor.execute("SELECT id FROM metadata WHERE content_id = ?", (content_id,))
            existing_metadata = cursor.fetchone()
            
            # Convert keywords to string if it's not already
            metadata = web_data["metadata"]
            keywords = metadata.get("keywords", "")
            if isinstance(keywords, (list, dict)):
                keywords = json.dumps(keywords)
            
            if existing_metadata:
                # Update existing metadata
                cursor.execute('''
                UPDATE metadata SET
                    og_title = ?,
                    og_description = ?,
                    og_image = ?,
                    keywords = ?
                WHERE content_id = ?
                ''', (
                    metadata.get("og_title", ""),
                    metadata.get("og_description", ""),
                    metadata.get("og_image", ""),
                    keywords,
                    content_id
                ))
            else:
                # Insert new metadata
                cursor.execute('''
                INSERT INTO metadata (
                    content_id, og_title, og_description, og_image, keywords
                ) VALUES (?, ?, ?, ?, ?)
                ''', (
                    content_id,
                    metadata.get("og_title", ""),
                    metadata.get("og_description", ""),
                    metadata.get("og_image", ""),
                    keywords
                ))
        
        # Handle web data
        # Check if web entry exists for this content
        cursor.execute("SELECT id FROM web WHERE content_id = ?", (content_id,))
        existing_web = cursor.fetchone()
        
        if existing_web:
            # Update existing web data
            cursor.execute('''
            UPDATE web SET
                url = ?,
                published_at = ?,
                full_content = ?
            WHERE content_id = ?
            ''', (
                web_data["url"],
                web_data["published_at"] or "",
                web_data["full_content"],
                content_id
            ))
        else:
            # Insert new web data
            cursor.execute('''
            INSERT INTO web (
                content_id, url, published_at, full_content
            ) VALUES (?, ?, ?, ?)
            ''', (
                content_id,
                web_data["url"],
                web_data["published_at"] or "",
                web_data["full_content"]
            ))
        
        conn.commit()
        return True
        
    except Exception as e:
        print(f"Error storing web data: {str(e)}")
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
            "web"
        ))
        
        conn.commit()
    except Exception as e:
        print(f"Error updating sync history: {str(e)}")
        conn.rollback()

def export_to_json(links_processed):
    """Export processing results to a JSON file for easier viewing."""
    try:
        os.makedirs("data", exist_ok=True)
        
        # Create JSON file with the processed links
        with open(os.path.join("data", "processed_weblinks.json"), 'w', encoding='utf-8') as f:
            json.dump(
                {
                    "count": len(links_processed),
                    "links": links_processed,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }, 
                f, 
                indent=2, 
                ensure_ascii=False
            )
        
        print(f"Exported {len(links_processed)} processed web links to data/processed_weblinks.json")
    except Exception as e:
        print(f"Error exporting to JSON: {str(e)}")

def main():
    try:
        print(f"*** TESTING MODE: Processing only first {MAX_LINKS_TO_PROCESS} web links ***")
        
        # Initialize database
        conn = init_database()
        if not conn:
            raise Exception("Failed to initialize database")
        
        # Fetch web links from API
        print("Fetching web links from API...")
        links = get_all_web_links()
        print(f"Found {len(links)} web links for testing")
        
        # Get list of content URLs already in database
        print("Getting list of content already in database...")
        existing_urls = get_existing_content_urls(conn)
        print(f"Found {len(existing_urls)} existing web items in database")
        
        # Filter out links that are already in the database
        new_links = [link for link in links if link["url"] not in existing_urls]
        updating_links = [link for link in links if link["url"] in existing_urls]
        
        print(f"Found {len(new_links)} new links to process")
        print(f"Found {len(updating_links)} existing links that might need updates")
        
        # Process links
        links_to_process = new_links + updating_links
        
        # Initialize counters
        added_count = 0
        updated_count = 0
        scraped_count = 0
        error_count = 0
        processed_links = []
        
        for i, link in enumerate(links_to_process):
            url = link["url"]
            is_new = url not in existing_urls
            
            print(f"\n{'='*80}\nProcessing link {i+1}/{len(links_to_process)}: {url}\n{'='*80}")
            
            try:
                # Scrape the web page
                scraped_data = scrape_web_page(url)
                
                # Update link title if we have a better one
                if scraped_data and scraped_data["title"] and "Error" not in scraped_data["title"]:
                    link["title"] = scraped_data["title"]
                    print(f"Updated title to: {link['title']}")
                
                # Set is_scraped flag based on scraping success
                is_scraped = 1 if scraped_data and "Failed to retrieve content" not in scraped_data["full_content"] else 0
                
                if is_scraped:
                    print(f"Successfully scraped content for {url} ({len(scraped_data['full_content'])} chars)")
                    scraped_count += 1
                
                # Store in database
                success = store_web_data(conn, scraped_data)
                
                if success:
                    if is_new:
                        added_count += 1
                    else:
                        updated_count += 1
                    
                    # Add to processed links
                    processed_links.append({
                        "url": url,
                        "title": link["title"],
                        "content_scraped": is_scraped == 1,
                        "content_length": len(scraped_data["full_content"]) if scraped_data["full_content"] else 0,
                        "processed_at": datetime.now(timezone.utc).isoformat()
                    })
                    
                    print(f"Successfully processed {url}")
                else:
                    error_count += 1
                    print(f"Failed to store data for {url}")
            
            except Exception as e:
                error_count += 1
                print(f"Error processing {url}: {str(e)}")
            
            # Avoid rate limiting
            if i < len(links_to_process) - 1:  # Don't sleep after the last link
                sleep_time = 2  # Sleep to avoid rate limiting
                print(f"Sleeping for {sleep_time} seconds to avoid rate limiting...")
                time.sleep(sleep_time)
        
        # Update sync history
        update_sync_history(conn, added_count, updated_count, scraped_count, error_count)
        
        # Export results
        if processed_links:
            export_to_json(processed_links)
        
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