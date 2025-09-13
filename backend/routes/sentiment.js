import express from "express";
import { loadCSV } from "../utils/csvLoader.js";

const router = express.Router();
const SENTIMENT_CSV = "../news_sentiment/daily_summary.csv";

// GET /api/sentiment?category=business&start=2025-09-01&end=2025-09-05
router.get("/", async (req, res) => {
  console.log("router");
  try {
    const { category, start, end } = req.query;
    let rows = await loadCSV(SENTIMENT_CSV);

    // Filter by category
    if (category) {
      rows = rows.filter(r => r.category.toLowerCase() === category.toLowerCase());
    }

    // Filter by date range
    if (start) {
      rows = rows.filter(r => new Date(r.date) >= new Date(start));
    }
    if (end) {
      rows = rows.filter(r => new Date(r.date) <= new Date(end));
    }

    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
