// import { Pool } from "pg";
// import { SQL } from "../sql-queries";

// // Initialize PostgreSQL pool
// const pool = new Pool({
//   host: process.env.POSTGRES_HOST,
//   port: 5432,
//   database: process.env.POSTGRES_DB,
//   user: process.env.POSTGRES_USER,
//   password: process.env.POSTGRES_PASSWORD,
//   ssl: {
//     rejectUnauthorized: false,
//   },
// });

// async function initializeDatabase(): Promise<void> {
//   const client = await pool.connect();

//   try {
//     console.log("Creating tables in PostgreSQL database...");

//     // Begin transaction
//     await client.query("BEGIN");

//     // Create all necessary tables
//     console.log("Creating content table...");
//     await client.query(SQL.CREATE_CONTENT_TABLE);

//     console.log("Creating metadata table...");
//     await client.query(SQL.CREATE_METADATA_TABLE);

//     console.log("Creating web table with vector embeddings...");
//     await client.query(SQL.CREATE_WEB_TABLE);

//     console.log("Creating sync_history table...");
//     await client.query(SQL.CREATE_SYNC_HISTORY_TABLE);

//     // Commit transaction
//     await client.query("COMMIT");

//     console.log("Database initialization completed successfully.");
//   } catch (error) {
//     // Rollback in case of error
//     await client.query("ROLLBACK");
//     console.error("Error initializing database:", error);
//     throw error;
//   } finally {
//     client.release();
//   }
// }

// // Run the initialization script
// async function main() {
//   try {
//     await initializeDatabase();
//   } catch (error) {
//     console.error("Database initialization failed:", error);
//     process.exit(1);
//   } finally {
//     // Close the pool when done
//     await pool.end();
//   }
// }

// main().catch((error) => {
//   console.error("Unhandled error:", error);
//   process.exit(1);
// });
