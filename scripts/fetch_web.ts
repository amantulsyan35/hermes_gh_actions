import { load } from "cheerio";
import { createClient, Client } from "@libsql/client";

// Types
interface ExtractedContent {
  title: string;
  url: string;
  publishedAt: string | null;
  fullContent: string;
  metaData: {
    ogTitle: string;
    ogDescription: string;
    ogImage: string;
    keywords: string;
  };
}

interface ContentItem {
  url: string;
  title?: string;
  content?: string;
  metadata?: any;
}

async function extractWebPageContent(link: string): Promise<ExtractedContent> {
  try {
    console.log(`Fetching content for ${link}`);

    // Set up AbortController for timeout
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 10000);

    try {
      // Fetch the HTML content from the link with headers
      const response = await fetch(link, {
        headers: {
          "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        },
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        throw new Error(
          `Failed to fetch ${link}: Status code ${response.status}`
        );
      }

      const html = await response.text();

      // Load the HTML content into cheerio
      const $ = load(html);

      // Extract the page title
      const title =
        $("title").text().trim() || $("h1").first().text().trim() || link;

      // Collect metadata
      const metaData = {
        ogTitle: $('meta[property="og:title"]').attr("content") || "",
        ogDescription:
          $('meta[property="og:description"]').attr("content") || "",
        ogImage: $('meta[property="og:image"]').attr("content") || "",
        keywords: $('meta[name="keywords"]').attr("content") || "",
      };

      let publishedAt: any;

      // Try to find date in common meta tags
      let publishedTimeRaw =
        $('meta[property="article:published_time"]').attr("content") ||
        $('meta[name="date"]').attr("content") ||
        $("time").attr("datetime") ||
        "";

      if (publishedTimeRaw) {
        publishedAt = publishedTimeRaw;
      }

      // Extract full page content, focusing on meaningful text elements
      // Remove script, style, and other non-content elements
      $(
        "script, style, noscript, iframe, img, svg, path, head, nav, footer, aside"
      ).remove();

      // Get all text content, preserving some structure
      let fullContent = "";

      // Get heading elements with their hierarchy
      $("h1, h2, h3, h4, h5, h6").each((_, element) => {
        const tagName = element.tagName.toLowerCase();
        const level = parseInt(tagName.substring(1));
        const prefix = "#".repeat(level) + " ";
        fullContent += prefix + $(element).text().trim() + "\n\n";
      });

      // Extract paragraphs
      $(
        "p, article, section, div, main, span, li, td, th, blockquote, pre, code, figcaption"
      ).each((_, element) => {
        const text = $(element).text().trim();
        if (text && text.length > 10) {
          // Skip very short elements that might just be styling
          fullContent += text + "\n\n";
        }
      });

      // Remove excessive whitespace
      fullContent = fullContent
        .replace(/(\n\s*){3,}/g, "\n\n") // Replace 3+ line breaks with 2
        .replace(/\s{2,}/g, " ") // Replace multiple spaces with a single space
        .trim();

      return {
        title,
        url: link,
        publishedAt,
        fullContent,
        metaData,
      };
    } catch (fetchError: any) {
      clearTimeout(timeoutId);
      if (fetchError.name === "AbortError") {
        throw new Error(`Request timeout for ${link}`);
      }
      throw fetchError;
    }
  } catch (error) {
    console.error(`Error scraping ${link}:`, error);

    // Return default values in case of error
    return {
      title: link, // Using the URL as the title for failed pages
      url: link,
      publishedAt: "",
      fullContent: `Failed to retrieve content from ${link}.`,
      metaData: {
        ogTitle: "",
        ogDescription: "",
        ogImage: "",
        keywords: "",
      },
    };
  }
}

async function createTursoTables(client: Client): Promise<void> {
  try {
    console.log("Creating tables in Turso database...");

    // Create content table
    await client.execute(`
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
    `);

    // Create metadata table
    await client.execute(`
      CREATE TABLE IF NOT EXISTS metadata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        content_id INTEGER,
        og_title TEXT,
        og_description TEXT,
        og_image TEXT,
        keywords TEXT,
        FOREIGN KEY (content_id) REFERENCES content(id)
      )
    `);

    // Create web table
    await client.execute(`
      CREATE TABLE IF NOT EXISTS web (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        content_id INTEGER,
        url TEXT,
        published_at TEXT,
        full_content TEXT,
        FOREIGN KEY (content_id) REFERENCES content(id)
      )
    `);

    // Create YouTube tables - for future use
    await client.execute(`
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
    `);

    await client.execute(`
      CREATE TABLE IF NOT EXISTS transcript (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        youtube_id INTEGER,
        full_text TEXT,
        language TEXT,
        duration REAL,
        fetched_at TEXT,
        FOREIGN KEY (youtube_id) REFERENCES youtube(id)
      )
    `);

    await client.execute(`
      CREATE TABLE IF NOT EXISTS transcript_segments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        transcript_id INTEGER,
        start_time TEXT,
        end_time TEXT,
        text TEXT,
        FOREIGN KEY (transcript_id) REFERENCES transcript(id)
      )
    `);

    // Create sync_history table
    await client.execute(`
      CREATE TABLE IF NOT EXISTS sync_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sync_time TEXT,
        entries_added INTEGER,
        entries_updated INTEGER,
        entries_scraped INTEGER,
        scrape_errors INTEGER,
        sync_type TEXT
      )
    `);

    console.log("Tables created successfully.");
  } catch (error) {
    console.error("Error creating tables:", error);
    throw error;
  }
}

async function storeContentInDatabase(
  client: Client,
  content: ExtractedContent
): Promise<boolean> {
  try {
    const currentTime = new Date().toISOString();

    // Insert into content table
    const contentResult = await client.execute({
      sql: `
        INSERT INTO content (url, content_type, title, created_at, scraped_at, is_scraped)
        VALUES (?, ?, ?, ?, ?, ?)
      `,
      args: [content.url, "web", content.title, currentTime, currentTime, 1],
    });

    if (!contentResult.lastInsertRowid) {
      console.error(`Failed to insert content for ${content.url}`);
      return false;
    }

    const contentId = Number(contentResult.lastInsertRowid);

    // Insert into web table
    await client.execute({
      sql: `
        INSERT INTO web (content_id, url, published_at, full_content)
        VALUES (?, ?, ?, ?)
      `,
      args: [
        contentId,
        content.url,
        content.publishedAt || null,
        content.fullContent,
      ],
    });

    // Insert into metadata table
    await client.execute({
      sql: `
        INSERT INTO metadata (content_id, og_title, og_description, og_image, keywords)
        VALUES (?, ?, ?, ?, ?)
      `,
      args: [
        contentId,
        content.metaData.ogTitle,
        content.metaData.ogDescription,
        content.metaData.ogImage,
        content.metaData.keywords,
      ],
    });

    console.log(`Successfully added entry for ${content.url}`);
    return true;
  } catch (error) {
    console.error(`Database error for ${content.url}:`, error);
    return false;
  }
}

async function processURLs(
  urls: string[],
  limit: number = 10
): Promise<{ added: number; errors: number }> {
  // Limit to first N URLs for testing if needed
  const urlsToProcess = urls.slice(0, limit);
  console.log(`Processing ${urlsToProcess.length} URLs (limit: ${limit})`);

  let addedCount = 0;
  let errorCount = 0;

  // Create Turso client
  const tursoUrl = process.env.TURSO_URL as string;
  const tursoAuthToken = process.env.TURSO_AUTH_TOKEN;

  if (!tursoAuthToken) {
    throw new Error("TURSO_AUTH_TOKEN environment variable is required");
  }

  console.log(`Connecting to Turso database: ${tursoUrl}`);

  const client = createClient({
    url: tursoUrl,
    authToken: tursoAuthToken,
  });

  try {
    // Create tables if they don't exist
    await createTursoTables(client);

    // Process each URL
    for (const url of urlsToProcess) {
      try {
        // Check if URL already exists
        const existingResult = await client.execute({
          sql: "SELECT id FROM content WHERE url = ?",
          args: [url],
        });

        if (existingResult.rows.length > 0) {
          console.log(`URL already exists: ${url}`);
          continue;
        }

        // Extract content
        const content = await extractWebPageContent(url);

        // Store in database
        const success = await storeContentInDatabase(client, content);

        if (success) {
          addedCount++;
        } else {
          errorCount++;
        }
      } catch (error) {
        console.error(`Error processing ${url}:`, error);
        errorCount++;
      }
    }

    // Add entry to sync_history
    const currentTime = new Date().toISOString();
    await client.execute({
      sql: `
        INSERT INTO sync_history (
          sync_time, entries_added, entries_updated, entries_scraped, scrape_errors, sync_type
        ) VALUES (?, ?, ?, ?, ?, ?)
      `,
      args: [currentTime, addedCount, 0, addedCount, errorCount, "api_fetch"],
    });

    // Get the count of content entries
    const countResult = await client.execute(
      "SELECT COUNT(*) as count FROM content"
    );
    const contentCount = countResult.rows[0].count;
    console.log(`Current content entries in database: ${contentCount}`);
  } catch (error) {
    console.error("Error during execution:", error);
    throw error;
  }

  return { added: addedCount, errors: errorCount };
}

// Main function to fetch and process content
async function main() {
  try {
    const apiEndpoint =
      process.env.API_ENDPOINT || "https://open-source-content.xyz/v1/web";
    console.log(`Fetching web content from ${apiEndpoint}...`);

    // Fetch the list of URLs from the API
    const response = await fetch(apiEndpoint);
    const data = await response.json();

    // Extract URLs from the data
    let urls: string[] = [];
    if (data && data.data && Array.isArray(data.data)) {
      urls = data.data.filter((url: string) => typeof url === "string");
    }

    if (urls.length === 0) {
      console.log("No URLs found in API response");
      return;
    }

    console.log(`Found ${urls.length} URLs`);

    // Process the URLs (limit to 10 for testing)
    const result = await processURLs(urls, 10);

    console.log(
      `Added ${result.added} new entries with ${result.errors} errors`
    );
  } catch (error) {
    console.error("Error in main function:", error);
    process.exit(1);
  }
}

// Run the script
main().catch((error) => {
  console.error("Unhandled error:", error);
  process.exit(1);
});
