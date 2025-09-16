import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import { loadCSV } from "../utils/csvloader.js";

// const router = express.Router();

// Resolve correct path to CSV (works regardless of where server is started from)
// const __filename = fileURLToPath(import.meta.url);
// const __dirname = path.dirname(__filename);
// const SENTIMENT_CSV = path.resolve(__dirname, "../news_sentiment/daily_summary.csv");

const router = express.Router();
const SENTIMENT_CSV = "../news_sentiment/daily_summary.csv";

// GET /api/sentiment?category=business&start=2025-09-01&end=2025-09-05
router.get("/", async (req, res) => {
  try {
    const { category, start, end } = req.query;
    let rows = await loadCSV(SENTIMENT_CSV);

    // Filter by category
    if (category) {
      rows = rows.filter(r => r.category && r.category.toLowerCase() === category.toLowerCase());
    }

    // Filter by date range
    if (start) {
      const startDate = new Date(start);
      rows = rows.filter(r => new Date(r.date) >= startDate);
    }
    if (end) {
      const endDate = new Date(end);
      rows = rows.filter(r => new Date(r.date) <= endDate);
    }

    res.json(rows);
  } catch (err) {
    console.error("Error in /api/sentiment:", err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
