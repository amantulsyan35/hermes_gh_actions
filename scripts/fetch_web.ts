import { load } from "cheerio";
import { Pool } from "pg";
import OpenAI from "openai";
import { SQL } from "../sql-queries";
import { OpenSourceEntry } from "../types";

// Initialize OpenAI client
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

// Initialize PostgreSQL pool
const pool = new Pool({
  host: process.env.POSTGRES_HOST,
  port: 5432,
  database: process.env.POSTGRES_DB,
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  ssl: {
    rejectUnauthorized: false,
  },
});

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

interface ContentWithEmbedding extends ExtractedContent {
  embedding: number[];
}

function formatEmbeddingForPostgres(embedding: number[]): string {
  // Format the array for PostgreSQL vector type - should start with [
  return `[${embedding.join(",")}]`;
}

async function extractPageContent(link: string): Promise<ExtractedContent> {
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
        const tagName = element.tagName?.toLowerCase?.();
        const level = parseInt(tagName?.substring(1) || "0");
        const prefix = "#".repeat(level || 1) + " ";
        const headingText = $(element).text().trim();
        fullContent += prefix + headingText + "\n\n";
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
      publishedAt: null,
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

async function generateEmbedding(text: string): Promise<number[]> {
  try {
    // Truncate text if it's too long (OpenAI has token limits)
    const truncatedText = text.substring(0, 8000);

    // Generate embedding using OpenAI API
    const response = await openai.embeddings.create({
      model: "text-embedding-ada-002",
      input: truncatedText,
    });

    return response.data[0].embedding;
  } catch (error) {
    console.error("Error generating embedding:", error);
    throw error;
  }
}

async function storeContentInDatabase(
  content: ContentWithEmbedding,
  consumedAt: string
): Promise<boolean> {
  const client = await pool.connect();

  try {
    const currentTime = new Date().toISOString();
    const consumedTimestamp = consumedAt || null;

    // Begin transaction
    await client.query("BEGIN");

    // Check if URL already exists
    const existingResult = await client.query(SQL.CHECK_URL_EXISTS, [
      content.url,
    ]);

    if (existingResult.rows.length > 0) {
      console.log(`URL already exists: ${content.url} - skipping`);
      await client.query("COMMIT");
      return false;
    }

    // Insert into content table
    const contentResult = await client.query(
      `INSERT INTO content
      (url, content_type, title, created_at, consumed_at, scraped_at, is_scraped)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING id`,
      [
        content.url,
        "web",
        content.title,
        currentTime,
        consumedTimestamp, // Store the createdAt as consumedAt
        currentTime,
        true,
      ]
    );

    const contentId = contentResult.rows[0].id;

    // Format the embedding properly for PostgreSQL
    const formattedEmbedding = formatEmbeddingForPostgres(content.embedding);

    // Insert into web table with properly formatted embedding
    await client.query(SQL.INSERT_WEB, [
      contentId,
      content.url,
      content.publishedAt,
      content.fullContent,
      formattedEmbedding, // Use the formatted embedding
    ]);

    // Insert into metadata table
    await client.query(SQL.INSERT_METADATA, [
      contentId,
      content.metaData.ogTitle,
      content.metaData.ogDescription,
      content.metaData.ogImage,
      content.metaData.keywords,
    ]);

    // Commit transaction
    await client.query("COMMIT");

    console.log(`Successfully added entry for ${content.url}`);
    return true;
  } catch (error) {
    // Rollback in case of error
    await client.query("ROLLBACK");
    console.error(`Database error for ${content.url}:`, error);
    return false;
  } finally {
    client.release();
  }
}

async function processURLs(
  entries: OpenSourceEntry[],
  limit: number = 10
): Promise<{ added: number; errors: number; skipped: number }> {
  // Limit to first N entries if needed
  const entriesToProcess = entries.slice(0, limit);
  console.log(`Processing ${entriesToProcess.length} URLs (limit: ${limit})`);

  let addedCount = 0;
  let errorCount = 0;
  let skippedCount = 0;

  try {
    // Process each entry
    for (const entry of entriesToProcess) {
      try {
        const url = entry.url;

        // Check if URL already exists
        const client = await pool.connect();
        const existingResult = await client.query(SQL.CHECK_URL_EXISTS, [url]);
        client.release();

        if (existingResult.rows.length > 0) {
          console.log(`URL already exists: ${url}`);
          skippedCount++;
          continue;
        }

        // 1. Extract content
        const content = await extractPageContent(url);

        // 2. Generate embedding from content
        const embedding = await generateEmbedding(content.fullContent);

        // 3. Store in database with embedding
        const contentWithEmbedding: ContentWithEmbedding = {
          ...content,
          embedding,
        };

        // Pass the createdAt timestamp to be used as consumedAt
        const success = await storeContentInDatabase(
          contentWithEmbedding,
          entry.createdAt
        );

        if (success) {
          addedCount++;
        } else {
          skippedCount++;
        }
      } catch (error) {
        console.error(`Error processing ${entry.url}:`, error);
        errorCount++;
      }
    }

    // Add entry to sync_history
    const client = await pool.connect();
    const currentTime = new Date().toISOString();
    await client.query(SQL.INSERT_SYNC_HISTORY, [
      currentTime,
      addedCount,
      0,
      addedCount,
      errorCount,
      "api_fetch",
    ]);

    // Get the count of content entries
    const countResult = await client.query(SQL.GET_CONTENT_COUNT);
    const contentCount = countResult.rows[0].count;
    console.log(`Current content entries in database: ${contentCount}`);
    client.release();
  } catch (error) {
    console.error("Error during execution:", error);
    throw error;
  }

  return { added: addedCount, errors: errorCount, skipped: skippedCount };
}

// Main function to fetch and process content
async function main() {
  try {
    const apiEndpoint =
      process.env.API_ENDPOINT || "https://open-source-content.xyz/v1/web";
    console.log(`Fetching web content from ${apiEndpoint}...`);

    // Fetch the list of entries from the API
    const response = await fetch(apiEndpoint);
    const data = await response.json();

    // Extract entries from the data
    let entries: OpenSourceEntry[] = [];
    if (data && data.data && Array.isArray(data.data)) {
      // Filter to ensure we have valid entries (with url field)
      entries = data.data.filter(
        (entry: any) =>
          typeof entry === "object" &&
          entry !== null &&
          typeof entry.url === "string"
      );

      // Filter out YouTube URLs
      entries = entries.filter(
        (entry: OpenSourceEntry) =>
          !entry.url.includes("youtube.com") && !entry.url.includes("youtu.be")
      );
    }

    if (entries.length === 0) {
      console.log("No valid entries found in API response");
      return;
    }

    console.log(`Found ${entries.length} valid entries`);

    // Process the entries (limit to desired number)
    const processLimit = process.env.PROCESS_LIMIT
      ? parseInt(process.env.PROCESS_LIMIT)
      : 10;

    const result = await processURLs(entries, processLimit);

    console.log(
      `Added ${result.added} new entries, skipped ${result.skipped}, with ${result.errors} errors`
    );
  } catch (error) {
    console.error("Error in main function:", error);
    process.exit(1);
  } finally {
    // Close the pool when done
    await pool.end();
  }
}

// Run the script
main().catch((error) => {
  console.error("Unhandled error:", error);
  process.exit(1);
});
