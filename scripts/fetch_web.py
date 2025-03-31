import os
import sqlite3
import libsql_experimental as libsql
import json
from datetime import datetime
import sys

def create_turso_tables(conn):
    """
    Create the necessary tables in Turso that mirror our SQLite schema
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

def copy_data(sqlite_conn, turso_conn):
    """
    Copy data from SQLite to Turso
    """
    print("Copying data from SQLite to Turso...")
    
    # Set SQLite to return rows as dictionaries
    sqlite_conn.row_factory = sqlite3.Row
    
    # Copy content table
    print("Copying content table...")
    content_cursor = sqlite_conn.cursor()
    content_cursor.execute("SELECT * FROM content")
    content_rows = content_cursor.fetchall()
    
    # Clear existing data to prevent duplicates
    turso_conn.execute("DELETE FROM content")
    
    for row in content_rows:
        # Convert row to dictionary
        row_dict = dict(row)
        
        # Build placeholders for SQL query
        placeholders = ", ".join(["?" for _ in row_dict])
        columns = ", ".join(row_dict.keys())
        
        # Build SQL statement
        sql = f"INSERT INTO content ({columns}) VALUES ({placeholders})"
        
        # Execute query
        turso_conn.execute(sql, list(row_dict.values()))
    
    # Copy metadata table
    print("Copying metadata table...")
    metadata_cursor = sqlite_conn.cursor()
    metadata_cursor.execute("SELECT * FROM metadata")
    metadata_rows = metadata_cursor.fetchall()
    
    # Clear existing data
    turso_conn.execute("DELETE FROM metadata")
    
    for row in metadata_rows:
        row_dict = dict(row)
        placeholders = ", ".join(["?" for _ in row_dict])
        columns = ", ".join(row_dict.keys())
        sql = f"INSERT INTO metadata ({columns}) VALUES ({placeholders})"
        turso_conn.execute(sql, list(row_dict.values()))
    
    # Copy web table - this might contain large text fields
    print("Copying web table...")
    web_cursor = sqlite_conn.cursor()
    web_cursor.execute("SELECT * FROM web")
    web_rows = web_cursor.fetchall()
    
    # Clear existing data
    turso_conn.execute("DELETE FROM web")
    
    for row in web_rows:
        row_dict = dict(row)
        placeholders = ", ".join(["?" for _ in row_dict])
        columns = ", ".join(row_dict.keys())
        sql = f"INSERT INTO web ({columns}) VALUES ({placeholders})"
        turso_conn.execute(sql, list(row_dict.values()))
    
    # Check if YouTube-related tables exist in SQLite and copy them if they do
    tables_to_check = ['youtube', 'transcript', 'transcript_segments']
    for table in tables_to_check:
        # Check if table exists in SQLite
        check_cursor = sqlite_conn.cursor()
        check_cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
        if check_cursor.fetchone():
            print(f"Copying {table} table...")
            
            # Clear existing data
            turso_conn.execute(f"DELETE FROM {table}")
            
            # Get and copy data
            data_cursor = sqlite_conn.cursor()
            data_cursor.execute(f"SELECT * FROM {table}")
            rows = data_cursor.fetchall()
            
            for row in rows:
                row_dict = dict(row)
                placeholders = ", ".join(["?" for _ in row_dict])
                columns = ", ".join(row_dict.keys())
                sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
                turso_conn.execute(sql, list(row_dict.values()))
    
    # Copy sync_history table
    print("Copying sync_history table...")
    sync_cursor = sqlite_conn.cursor()
    sync_cursor.execute("SELECT * FROM sync_history")
    sync_rows = sync_cursor.fetchall()
    
    # Clear existing data
    turso_conn.execute("DELETE FROM sync_history")
    
    for row in sync_rows:
        row_dict = dict(row)
        placeholders = ", ".join(["?" for _ in row_dict])
        columns = ", ".join(row_dict.keys())
        sql = f"INSERT INTO sync_history ({columns}) VALUES ({placeholders})"
        turso_conn.execute(sql, list(row_dict.values()))
    
    # Add a migration record to sync_history
    current_time = datetime.now().isoformat()
    turso_conn.execute('''
    INSERT INTO sync_history (
        sync_time, entries_added, entries_updated, entries_scraped, scrape_errors, sync_type
    ) VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        current_time,
        len(content_rows),
        0,
        0,
        0,
        "sqlite_migration"
    ))
    
    print(f"Successfully copied {len(content_rows)} entries to Turso.")

def main():
    # Check environment variables or use defaults
    SQLITE_DB = os.environ.get("SQLITE_DB", "content.sqlite")
    TURSO_URL = os.environ.get("TURSO_URL", "libsql://context-amantulsyan35.aws-us-east-1.turso.io")
    TURSO_AUTH_TOKEN = os.environ.get("TURSO_AUTH_TOKEN")
    
    # Validate that we have the Turso auth token
    if not TURSO_AUTH_TOKEN:
        print("Error: TURSO_AUTH_TOKEN environment variable is required")
        sys.exit(1)
    
    print(f"Connecting to SQLite database: {SQLITE_DB}")
    # Check if SQLite file exists
    if not os.path.exists(SQLITE_DB):
        print(f"Error: SQLite database file {SQLITE_DB} not found")
        sys.exit(1)
    
    # Connect to the SQLite database
    sqlite_conn = sqlite3.connect(SQLITE_DB)
    
    # Connect to Turso 
    print(f"Connecting to Turso database: {TURSO_URL}")
    # The local.db is just a local cache file
    turso_conn = libsql.connect("local.db", sync_url=TURSO_URL, auth_token=TURSO_AUTH_TOKEN)
    
    try:
        # Create Turso tables if they don't exist
        create_turso_tables(turso_conn)
        
        # Copy data from SQLite to Turso
        copy_data(sqlite_conn, turso_conn)
        
        # Sync changes to Turso
        print("Syncing changes to Turso...")
        turso_conn.sync()
        print("Sync completed successfully.")
        
        # Validate by counting rows
        sqlite_cursor = sqlite_conn.cursor()
        sqlite_cursor.execute("SELECT COUNT(*) FROM content")
        sqlite_count = sqlite_cursor.fetchone()[0]
        
        turso_count = turso_conn.execute("SELECT COUNT(*) FROM content").fetchone()[0]
        
        print(f"Validation - SQLite content rows: {sqlite_count}, Turso content rows: {turso_count}")
        
        if sqlite_count == turso_count:
            print("✅ Migration successful! Row counts match.")
        else:
            print("⚠️ Warning: Row counts don't match. Some data may not have been transferred.")
            
    except Exception as e:
        print(f"Error during migration: {str(e)}")
        sys.exit(1)
    finally:
        # Close connections
        sqlite_conn.close()
        turso_conn.close()
        print("Database connections closed.")

if __name__ == "__main__":
    main()