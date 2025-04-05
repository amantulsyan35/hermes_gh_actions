// sql-queries.ts - Contains all SQL statements used in the application

export const SQL = {
  // Table creation queries
  CREATE_CONTENT_TABLE: `
    CREATE TABLE IF NOT EXISTS content (
      id SERIAL PRIMARY KEY,
      url TEXT UNIQUE,
      content_type TEXT,
      title TEXT,
      created_at TIMESTAMP WITH TIME ZONE,
      consumed_at TIMESTAMP WITH TIME ZONE,
      last_updated_at TIMESTAMP WITH TIME ZONE,
      scraped_at TIMESTAMP WITH TIME ZONE,
      is_scraped BOOLEAN DEFAULT FALSE
    )
  `,

  CREATE_METADATA_TABLE: `
    CREATE TABLE IF NOT EXISTS metadata (
      id SERIAL PRIMARY KEY,
      content_id INTEGER REFERENCES content(id),
      og_title TEXT,
      og_description TEXT,
      og_image TEXT,
      keywords TEXT
    )
  `,

  CREATE_WEB_TABLE: `
    CREATE TABLE IF NOT EXISTS web (
      id SERIAL PRIMARY KEY,
      content_id INTEGER REFERENCES content(id),
      url TEXT,
      published_at TIMESTAMP WITH TIME ZONE,
      full_content TEXT,
      embedding vector(1536)
    )
  `,

  CREATE_SYNC_HISTORY_TABLE: `
    CREATE TABLE IF NOT EXISTS sync_history (
      id SERIAL PRIMARY KEY,
      sync_time TIMESTAMP WITH TIME ZONE,
      entries_added INTEGER,
      entries_updated INTEGER,
      entries_scraped INTEGER,
      scrape_errors INTEGER,
      sync_type TEXT
    )
  `,

  // Data manipulation queries
  CHECK_URL_EXISTS: "SELECT id FROM content WHERE url = $1",

  INSERT_CONTENT: `
    INSERT INTO content 
    (url, content_type, title, created_at, consumed_at, scraped_at, is_scraped)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    RETURNING id
  `,

  INSERT_WEB: `
    INSERT INTO web 
    (content_id, url, published_at, full_content, embedding)
    VALUES ($1, $2, $3, $4, $5)
  `,

  INSERT_METADATA: `
    INSERT INTO metadata 
    (content_id, og_title, og_description, og_image, keywords)
    VALUES ($1, $2, $3, $4, $5)
  `,

  INSERT_SYNC_HISTORY: `
    INSERT INTO sync_history (
      sync_time, entries_added, entries_updated, entries_scraped, scrape_errors, sync_type
    ) VALUES ($1, $2, $3, $4, $5, $6)
  `,

  // Reporting queries
  GET_CONTENT_COUNT: "SELECT COUNT(*) as count FROM content",
};
