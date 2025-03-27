import sqlite3
import json
import os

def query_database():
    # Connect to database
    conn = sqlite3.connect("data/transcripts.sqlite")
    cursor = conn.cursor()
    
    # Get table info
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print(f"Tables in database: {[table[0] for table in tables]}")
    
    # Count records
    cursor.execute("SELECT COUNT(*) FROM transcripts")
    count = cursor.fetchone()[0]
    print(f"Total transcripts: {count}")
    
    # Get sample data
    print("\nSample transcripts (first 5):")
    cursor.execute("""
        SELECT video_id, substr(transcript, 1, 100) || '...' AS preview, fetched_at 
        FROM transcripts LIMIT 5
    """)
    
    for row in cursor.fetchall():
        print(f"\nVideo ID: {row[0]}")
        print(f"Fetched at: {row[2]}")
        print(f"Transcript preview: {row[1]}")
    
    conn.close()

if __name__ == "__main__":
    query_database()