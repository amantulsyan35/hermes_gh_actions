"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g = Object.create((typeof Iterator === "function" ? Iterator : Object).prototype);
    return g.next = verb(0), g["throw"] = verb(1), g["return"] = verb(2), typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
var cheerio_1 = require("cheerio");
var client_1 = require("@libsql/client");
function extractWebPageContent(link) {
    return __awaiter(this, void 0, void 0, function () {
        var controller_1, timeoutId, response, html, $_1, title, metaData, publishedAt, publishedTimeRaw, fullContent_1, fetchError_1, error_1;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    _a.trys.push([0, 6, , 7]);
                    console.log("Fetching content for ".concat(link));
                    controller_1 = new AbortController();
                    timeoutId = setTimeout(function () { return controller_1.abort(); }, 10000);
                    _a.label = 1;
                case 1:
                    _a.trys.push([1, 4, , 5]);
                    return [4 /*yield*/, fetch(link, {
                            headers: {
                                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                            },
                            signal: controller_1.signal,
                        })];
                case 2:
                    response = _a.sent();
                    clearTimeout(timeoutId);
                    if (!response.ok) {
                        throw new Error("Failed to fetch ".concat(link, ": Status code ").concat(response.status));
                    }
                    return [4 /*yield*/, response.text()];
                case 3:
                    html = _a.sent();
                    $_1 = (0, cheerio_1.load)(html);
                    title = $_1("title").text().trim() || $_1("h1").first().text().trim() || link;
                    metaData = {
                        ogTitle: $_1('meta[property="og:title"]').attr("content") || "",
                        ogDescription: $_1('meta[property="og:description"]').attr("content") || "",
                        ogImage: $_1('meta[property="og:image"]').attr("content") || "",
                        keywords: $_1('meta[name="keywords"]').attr("content") || "",
                    };
                    publishedAt = void 0;
                    publishedTimeRaw = $_1('meta[property="article:published_time"]').attr("content") ||
                        $_1('meta[name="date"]').attr("content") ||
                        $_1("time").attr("datetime") ||
                        "";
                    if (publishedTimeRaw) {
                        publishedAt = publishedTimeRaw;
                    }
                    // Extract full page content, focusing on meaningful text elements
                    // Remove script, style, and other non-content elements
                    $_1("script, style, noscript, iframe, img, svg, path, head, nav, footer, aside").remove();
                    fullContent_1 = "";
                    // Get heading elements with their hierarchy
                    $_1("h1, h2, h3, h4, h5, h6").each(function (_, element) {
                        var tagName = element.tagName.toLowerCase();
                        var level = parseInt(tagName.substring(1));
                        var prefix = "#".repeat(level) + " ";
                        fullContent_1 += prefix + $_1(element).text().trim() + "\n\n";
                    });
                    // Extract paragraphs
                    $_1("p, article, section, div, main, span, li, td, th, blockquote, pre, code, figcaption").each(function (_, element) {
                        var text = $_1(element).text().trim();
                        if (text && text.length > 10) {
                            // Skip very short elements that might just be styling
                            fullContent_1 += text + "\n\n";
                        }
                    });
                    // Remove excessive whitespace
                    fullContent_1 = fullContent_1
                        .replace(/(\n\s*){3,}/g, "\n\n") // Replace 3+ line breaks with 2
                        .replace(/\s{2,}/g, " ") // Replace multiple spaces with a single space
                        .trim();
                    return [2 /*return*/, {
                            title: title,
                            url: link,
                            publishedAt: publishedAt,
                            fullContent: fullContent_1,
                            metaData: metaData,
                        }];
                case 4:
                    fetchError_1 = _a.sent();
                    clearTimeout(timeoutId);
                    if (fetchError_1.name === "AbortError") {
                        throw new Error("Request timeout for ".concat(link));
                    }
                    throw fetchError_1;
                case 5: return [3 /*break*/, 7];
                case 6:
                    error_1 = _a.sent();
                    console.error("Error scraping ".concat(link, ":"), error_1);
                    // Return default values in case of error
                    return [2 /*return*/, {
                            title: link, // Using the URL as the title for failed pages
                            url: link,
                            publishedAt: null,
                            fullContent: "Failed to retrieve content from ".concat(link, "."),
                            metaData: {
                                ogTitle: "",
                                ogDescription: "",
                                ogImage: "",
                                keywords: "",
                            },
                        }];
                case 7: return [2 /*return*/];
            }
        });
    });
}
function createTursoTables(client) {
    return __awaiter(this, void 0, void 0, function () {
        var error_2;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    _a.trys.push([0, 8, , 9]);
                    console.log("Creating tables in Turso database...");
                    // Create content table
                    return [4 /*yield*/, client.execute("\n      CREATE TABLE IF NOT EXISTS content (\n        id INTEGER PRIMARY KEY AUTOINCREMENT,\n        url TEXT UNIQUE,\n        content_type TEXT,\n        title TEXT,\n        created_at TEXT,\n        consumed_at TEXT,\n        last_updated_at TEXT,\n        scraped_at TEXT,\n        is_scraped INTEGER DEFAULT 0\n      )\n    ")];
                case 1:
                    // Create content table
                    _a.sent();
                    // Create metadata table
                    return [4 /*yield*/, client.execute("\n      CREATE TABLE IF NOT EXISTS metadata (\n        id INTEGER PRIMARY KEY AUTOINCREMENT,\n        content_id INTEGER,\n        og_title TEXT,\n        og_description TEXT,\n        og_image TEXT,\n        keywords TEXT,\n        FOREIGN KEY (content_id) REFERENCES content(id)\n      )\n    ")];
                case 2:
                    // Create metadata table
                    _a.sent();
                    // Create web table
                    return [4 /*yield*/, client.execute("\n      CREATE TABLE IF NOT EXISTS web (\n        id INTEGER PRIMARY KEY AUTOINCREMENT,\n        content_id INTEGER,\n        url TEXT,\n        published_at TEXT,\n        full_content TEXT,\n        FOREIGN KEY (content_id) REFERENCES content(id)\n      )\n    ")];
                case 3:
                    // Create web table
                    _a.sent();
                    // Create YouTube tables - for future use
                    return [4 /*yield*/, client.execute("\n      CREATE TABLE IF NOT EXISTS youtube (\n        id INTEGER PRIMARY KEY AUTOINCREMENT,\n        content_id INTEGER,\n        url TEXT,\n        video_id TEXT,\n        channel_name TEXT,\n        description TEXT,\n        duration TEXT,\n        FOREIGN KEY (content_id) REFERENCES content(id)\n      )\n    ")];
                case 4:
                    // Create YouTube tables - for future use
                    _a.sent();
                    return [4 /*yield*/, client.execute("\n      CREATE TABLE IF NOT EXISTS transcript (\n        id INTEGER PRIMARY KEY AUTOINCREMENT,\n        youtube_id INTEGER,\n        full_text TEXT,\n        language TEXT,\n        duration REAL,\n        fetched_at TEXT,\n        FOREIGN KEY (youtube_id) REFERENCES youtube(id)\n      )\n    ")];
                case 5:
                    _a.sent();
                    return [4 /*yield*/, client.execute("\n      CREATE TABLE IF NOT EXISTS transcript_segments (\n        id INTEGER PRIMARY KEY AUTOINCREMENT,\n        transcript_id INTEGER,\n        start_time TEXT,\n        end_time TEXT,\n        text TEXT,\n        FOREIGN KEY (transcript_id) REFERENCES transcript(id)\n      )\n    ")];
                case 6:
                    _a.sent();
                    // Create sync_history table
                    return [4 /*yield*/, client.execute("\n      CREATE TABLE IF NOT EXISTS sync_history (\n        id INTEGER PRIMARY KEY AUTOINCREMENT,\n        sync_time TEXT,\n        entries_added INTEGER,\n        entries_updated INTEGER,\n        entries_scraped INTEGER,\n        scrape_errors INTEGER,\n        sync_type TEXT\n      )\n    ")];
                case 7:
                    // Create sync_history table
                    _a.sent();
                    console.log("Tables created successfully.");
                    return [3 /*break*/, 9];
                case 8:
                    error_2 = _a.sent();
                    console.error("Error creating tables:", error_2);
                    throw error_2;
                case 9: return [2 /*return*/];
            }
        });
    });
}
function storeContentInDatabase(client, content) {
    return __awaiter(this, void 0, void 0, function () {
        var currentTime, contentResult, contentId, error_3;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    _a.trys.push([0, 4, , 5]);
                    currentTime = new Date().toISOString();
                    return [4 /*yield*/, client.execute({
                            sql: "\n        INSERT INTO content (url, content_type, title, created_at, scraped_at, is_scraped)\n        VALUES (?, ?, ?, ?, ?, ?)\n      ",
                            args: [content.url, "web", content.title, currentTime, currentTime, 1],
                        })];
                case 1:
                    contentResult = _a.sent();
                    if (!contentResult.lastInsertRowid) {
                        console.error("Failed to insert content for ".concat(content.url));
                        return [2 /*return*/, false];
                    }
                    contentId = Number(contentResult.lastInsertRowid);
                    // Insert into web table
                    return [4 /*yield*/, client.execute({
                            sql: "\n        INSERT INTO web (content_id, url, published_at, full_content)\n        VALUES (?, ?, ?, ?)\n      ",
                            args: [
                                contentId,
                                content.url,
                                content.publishedAt || null,
                                content.fullContent,
                            ],
                        })];
                case 2:
                    // Insert into web table
                    _a.sent();
                    // Insert into metadata table
                    return [4 /*yield*/, client.execute({
                            sql: "\n        INSERT INTO metadata (content_id, og_title, og_description, og_image, keywords)\n        VALUES (?, ?, ?, ?, ?)\n      ",
                            args: [
                                contentId,
                                content.metaData.ogTitle,
                                content.metaData.ogDescription,
                                content.metaData.ogImage,
                                content.metaData.keywords,
                            ],
                        })];
                case 3:
                    // Insert into metadata table
                    _a.sent();
                    console.log("Successfully added entry for ".concat(content.url));
                    return [2 /*return*/, true];
                case 4:
                    error_3 = _a.sent();
                    console.error("Database error for ".concat(content.url, ":"), error_3);
                    return [2 /*return*/, false];
                case 5: return [2 /*return*/];
            }
        });
    });
}
function processURLs(urls_1) {
    return __awaiter(this, arguments, void 0, function (urls, limit) {
        var urlsToProcess, addedCount, errorCount, tursoUrl, tursoAuthToken, client, _i, urlsToProcess_1, url, existingResult, content, success, error_4, currentTime, countResult, contentCount, error_5;
        if (limit === void 0) { limit = 10; }
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    urlsToProcess = urls.slice(0, limit);
                    console.log("Processing ".concat(urlsToProcess.length, " URLs (limit: ").concat(limit, ")"));
                    addedCount = 0;
                    errorCount = 0;
                    tursoUrl = process.env.TURSO_URL ||
                        "libsql://context-amantulsyan35.aws-us-east-1.turso.io";
                    tursoAuthToken = process.env.TURSO_AUTH_TOKEN ||
                        "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJnaWQiOiI1YTA1ODFmZC03Y2E2LTRjMGUtOGU4ZS00M2ExN2VmMTY1MTQiLCJpYXQiOjE3NDM0MjI2NzQsInJpZCI6IjIwNmFmMWI2LWMyMGUtNGU5Yy1hMDJlLTQ4MTdhYjU4OWZlZiJ9.xHYDOLpY0f__LnsgVUAmE8GNJYMxla-_TLm94elavupw9p3Bbe5UQAFcWg7jqmHF_pJ_nt40FpFzFE9E3m-oDw";
                    if (!tursoAuthToken) {
                        throw new Error("TURSO_AUTH_TOKEN environment variable is required");
                    }
                    console.log("Connecting to Turso database: ".concat(tursoUrl));
                    client = (0, client_1.createClient)({
                        url: tursoUrl,
                        authToken: tursoAuthToken,
                    });
                    _a.label = 1;
                case 1:
                    _a.trys.push([1, 13, , 14]);
                    // Create tables if they don't exist
                    return [4 /*yield*/, createTursoTables(client)];
                case 2:
                    // Create tables if they don't exist
                    _a.sent();
                    _i = 0, urlsToProcess_1 = urlsToProcess;
                    _a.label = 3;
                case 3:
                    if (!(_i < urlsToProcess_1.length)) return [3 /*break*/, 10];
                    url = urlsToProcess_1[_i];
                    _a.label = 4;
                case 4:
                    _a.trys.push([4, 8, , 9]);
                    return [4 /*yield*/, client.execute({
                            sql: "SELECT id FROM content WHERE url = ?",
                            args: [url],
                        })];
                case 5:
                    existingResult = _a.sent();
                    if (existingResult.rows.length > 0) {
                        console.log("URL already exists: ".concat(url));
                        return [3 /*break*/, 9];
                    }
                    return [4 /*yield*/, extractWebPageContent(url)];
                case 6:
                    content = _a.sent();
                    return [4 /*yield*/, storeContentInDatabase(client, content)];
                case 7:
                    success = _a.sent();
                    if (success) {
                        addedCount++;
                    }
                    else {
                        errorCount++;
                    }
                    return [3 /*break*/, 9];
                case 8:
                    error_4 = _a.sent();
                    console.error("Error processing ".concat(url, ":"), error_4);
                    errorCount++;
                    return [3 /*break*/, 9];
                case 9:
                    _i++;
                    return [3 /*break*/, 3];
                case 10:
                    currentTime = new Date().toISOString();
                    return [4 /*yield*/, client.execute({
                            sql: "\n        INSERT INTO sync_history (\n          sync_time, entries_added, entries_updated, entries_scraped, scrape_errors, sync_type\n        ) VALUES (?, ?, ?, ?, ?, ?)\n      ",
                            args: [currentTime, addedCount, 0, addedCount, errorCount, "api_fetch"],
                        })];
                case 11:
                    _a.sent();
                    return [4 /*yield*/, client.execute("SELECT COUNT(*) as count FROM content")];
                case 12:
                    countResult = _a.sent();
                    contentCount = countResult.rows[0].count;
                    console.log("Current content entries in database: ".concat(contentCount));
                    return [3 /*break*/, 14];
                case 13:
                    error_5 = _a.sent();
                    console.error("Error during execution:", error_5);
                    throw error_5;
                case 14: return [2 /*return*/, { added: addedCount, errors: errorCount }];
            }
        });
    });
}
// Main function to fetch and process content
function main() {
    return __awaiter(this, void 0, void 0, function () {
        var apiEndpoint, response, data, urls, result, error_6;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    _a.trys.push([0, 4, , 5]);
                    apiEndpoint = process.env.API_ENDPOINT || "https://open-source-content.xyz/v1/web";
                    console.log("Fetching web content from ".concat(apiEndpoint, "..."));
                    return [4 /*yield*/, fetch(apiEndpoint)];
                case 1:
                    response = _a.sent();
                    return [4 /*yield*/, response.json()];
                case 2:
                    data = _a.sent();
                    urls = [];
                    if (data && data.data && Array.isArray(data.data)) {
                        urls = data.data.filter(function (url) { return typeof url === "string"; });
                    }
                    if (urls.length === 0) {
                        console.log("No URLs found in API response");
                        return [2 /*return*/];
                    }
                    console.log("Found ".concat(urls.length, " URLs"));
                    return [4 /*yield*/, processURLs(urls, 10)];
                case 3:
                    result = _a.sent();
                    console.log("Added ".concat(result.added, " new entries with ").concat(result.errors, " errors"));
                    return [3 /*break*/, 5];
                case 4:
                    error_6 = _a.sent();
                    console.error("Error in main function:", error_6);
                    process.exit(1);
                    return [3 /*break*/, 5];
                case 5: return [2 /*return*/];
            }
        });
    });
}
// Run the script
main().catch(function (error) {
    console.error("Unhandled error:", error);
    process.exit(1);
});
