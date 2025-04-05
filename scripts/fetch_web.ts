import { load } from "cheerio";
import { Pool } from "pg";
import OpenAI from "openai";
import { SQL } from "../sql-queries";
import { OpenSourceEntry } from "../types";

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

const pool = new Pool({
  host: process.env.POSTGRES_HOST,
  port: 5432,
  database: process.env.POSTGRES_DB,
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  ssl: { rejectUnauthorized: false },
});

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
  return `[${embedding.join(",")}]`;
}

async function extractPageContent(
  link: string
): Promise<ExtractedContent | null> {
  try {
    console.log(`🧠 Starting scrape for: ${link}`);
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 10000);

    try {
      const response = await fetch(link, {
        headers: {
          "User-Agent": "Mozilla/5.0",
        },
        signal: controller.signal,
      });
      clearTimeout(timeoutId);
      if (!response.ok) throw new Error(`Status code ${response.status}`);

      const html = await response.text();
      const $ = load(html);

      const title =
        $("title").text().trim() || $("h1").first().text().trim() || link;
      const metaData = {
        ogTitle: $('meta[property="og:title"]').attr("content") || "",
        ogDescription:
          $('meta[property="og:description"]').attr("content") || "",
        ogImage: $('meta[property="og:image"]').attr("content") || "",
        keywords: $('meta[name="keywords"]').attr("content") || "",
      };

      const publishedAt =
        $('meta[property="article:published_time"]').attr("content") ||
        $('meta[name="date"]').attr("content") ||
        $("time").attr("datetime") ||
        null;

      $(
        "script, style, noscript, iframe, img, svg, path, head, nav, footer, aside"
      ).remove();

      let fullContent = "";
      $("h1, h2, h3, h4, h5, h6").each((_, el) => {
        if (el.type === "tag") {
          const tag = el.tagName.toLowerCase();
          const level = parseInt(tag.substring(1));
          const headingText = $(el).text().trim();
          fullContent += "#".repeat(level) + " " + headingText + "\n\n";
        }
      });

      $(
        "p, article, section, div, main, span, li, td, th, blockquote, pre, code, figcaption"
      ).each((_, element) => {
        const text = $(element).text().trim();
        if (text && text.length > 10) fullContent += text + "\n\n";
      });

      fullContent = fullContent
        .replace(/(\n\s*){3,}/g, "\n\n")
        .replace(/\s{2,}/g, " ")
        .trim();

      console.log(`✅ Scraped content from: ${link}`);
      return { title, url: link, publishedAt, fullContent, metaData };
    } catch (fetchError: any) {
      clearTimeout(timeoutId);
      if (fetchError.name === "AbortError")
        throw new Error(`Timeout for ${link}`);
      throw fetchError;
    }
  } catch (error) {
    console.error(`❌ Error scraping ${link}:`, error);
    return null;
  }
}

async function extractPageContentWithRetry(
  url: string,
  retries = 2
): Promise<ExtractedContent | null> {
  for (let attempt = 1; attempt <= retries; attempt++) {
    const result = await extractPageContent(url);
    if (result) return result;
    console.warn(`Retrying (${attempt}/${retries}) for ${url}`);
  }
  return null;
}

async function generateEmbedding(text: string): Promise<number[]> {
  try {
    const truncatedText = text.substring(0, 8000);
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

    await client.query("BEGIN");

    const existingResult = await client.query(SQL.CHECK_URL_EXISTS, [
      content.url,
    ]);
    if (existingResult.rows.length > 0) {
      await client.query("COMMIT");
      return false;
    }

    const contentResult = await client.query(SQL.INSERT_CONTENT, [
      content.url,
      "web",
      content.title,
      currentTime,
      consumedTimestamp,
      currentTime,
      true,
    ]);
    const contentId = contentResult.rows[0].id;
    const formattedEmbedding = formatEmbeddingForPostgres(content.embedding);

    await client.query(SQL.INSERT_WEB, [
      contentId,
      content.url,
      content.publishedAt,
      content.fullContent,
      formattedEmbedding,
    ]);

    await client.query(SQL.INSERT_METADATA, [
      contentId,
      content.metaData.ogTitle,
      content.metaData.ogDescription,
      content.metaData.ogImage,
      content.metaData.keywords,
    ]);

    await client.query("COMMIT");
    console.log(`📝 Successfully added to database: ${content.url}`);
    return true;
  } catch (error) {
    await client.query("ROLLBACK");
    console.error(`❌ Database error for ${content.url}:`, error);
    return false;
  } finally {
    client.release();
  }
}

async function processURLs(
  entries: OpenSourceEntry[],
  limit = 10
): Promise<{ added: number; errors: number; skipped: number }> {
  let addedCount = 0;
  let errorCount = 0;
  let skippedCount = 0;

  for (const entry of entries) {
    if (addedCount >= limit) break;

    try {
      const url = entry.url;
      const client = await pool.connect();
      const existingResult = await client.query(SQL.CHECK_URL_EXISTS, [url]);
      client.release();

      if (existingResult.rows.length > 0) {
        skippedCount++;
        continue;
      }

      const content = await extractPageContentWithRetry(url);
      if (!content) {
        skippedCount++;
        continue;
      }

      const embedding = await generateEmbedding(content.fullContent);
      const contentWithEmbedding: ContentWithEmbedding = {
        ...content,
        embedding,
      };

      const success = await storeContentInDatabase(
        contentWithEmbedding,
        entry.createdAt
      );
      if (success) addedCount++;
      else skippedCount++;
    } catch (error) {
      console.error(`Error processing ${entry.url}:`, error);
      errorCount++;
    }
  }

  return { added: addedCount, errors: errorCount, skipped: skippedCount };
}

async function retryFailedScrapes(
  limit = 5
): Promise<{ retried: number; errors: number }> {
  const client = await pool.connect();
  let retriedCount = 0;
  let errorCount = 0;

  try {
    const result = await client.query(
      `
      SELECT id, url, consumed_at 
      FROM content 
      WHERE is_scraped = false AND (retry_count IS NULL OR retry_count < 3) 
      ORDER BY consumed_at DESC 
      LIMIT $1
    `,
      [limit]
    );
    const failedEntries = result.rows;

    for (const entry of failedEntries) {
      try {
        const content = await extractPageContentWithRetry(entry.url);
        if (!content) {
          await pool.query(
            `UPDATE content SET retry_count = COALESCE(retry_count, 0) + 1 WHERE id = $1`,
            [entry.id]
          );
          errorCount++;
          continue;
        }

        const embedding = await generateEmbedding(content.fullContent);
        const contentWithEmbedding: ContentWithEmbedding = {
          ...content,
          embedding,
        };
        const success = await storeContentInDatabase(
          contentWithEmbedding,
          entry.consumed_at
        );

        if (success) {
          await pool.query(
            `UPDATE content SET is_scraped = true, scraped_at = $1 WHERE id = $2`,
            [new Date().toISOString(), entry.id]
          );
          retriedCount++;
        } else {
          await pool.query(
            `UPDATE content SET retry_count = COALESCE(retry_count, 0) + 1 WHERE id = $1`,
            [entry.id]
          );
          errorCount++;
        }
      } catch (err) {
        await pool.query(
          `UPDATE content SET retry_count = COALESCE(retry_count, 0) + 1 WHERE id = $1`,
          [entry.id]
        );
        errorCount++;
      }
    }

    return { retried: retriedCount, errors: errorCount };
  } catch (err) {
    return { retried: 0, errors: 1 };
  } finally {
    client.release();
  }
}

async function main() {
  const requiredEnv = [
    "OPENAI_API_KEY",
    "POSTGRES_HOST",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_DB",
  ];

  requiredEnv.forEach((key) => {
    if (!process.env[key]) {
      console.error(`❌ Missing required environment variable: ${key}`);
      process.exit(1);
    }
  });

  try {
    const apiEndpoint =
      process.env.API_ENDPOINT || "https://open-source-content.xyz/v1/web";
    const response = await fetch(apiEndpoint);
    const data = await response.json();

    const entries: OpenSourceEntry[] = Array.isArray(data?.data)
      ? data.data.filter((entry: any) => typeof entry?.url === "string")
      : [];

    const processLimit = process.env.PROCESS_LIMIT
      ? parseInt(process.env.PROCESS_LIMIT)
      : 10;
    const result = await processURLs(entries, processLimit);
    console.log(
      `Added ${result.added}, Skipped ${result.skipped}, Errors ${result.errors}`
    );

    const retryLimit = 5;
    const retryResult = await retryFailedScrapes(retryLimit);
    console.log(
      `Retried ${retryResult.retried}, Retry Errors ${retryResult.errors}`
    );

    const client = await pool.connect();
    await client.query(SQL.INSERT_SYNC_HISTORY, [
      new Date().toISOString(),
      result.added,
      0,
      result.added + retryResult.retried,
      result.errors + retryResult.errors,
      "api_fetch",
    ]);

    const countResult = await client.query(SQL.GET_CONTENT_COUNT);
    console.log(
      `Current content entries in database: ${countResult.rows[0].count}`
    );
    client.release();
  } catch (error) {
    console.error("Error in main function:", error);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error("Unhandled error:", error);
  process.exit(1);
});
