import express from "express";
import { loadCSV } from "../utils/csvloader.js";

const router = express.Router();
const MARKET_CSV = "../news_sentiment/market_sentiment_results.csv";

// GET /api/market?ticker=^GSPC&lag=1
router.get("/", async (req, res) => {
  try {
    const { ticker, lag } = req.query;
    let rows = await loadCSV(MARKET_CSV);

    if (ticker) {
      rows = rows.filter(r => r.Ticker === ticker);
    }
    if (lag) {
      rows = rows.filter(r => parseInt(r["Lag (days)"], 10) === parseInt(lag, 10));
    }

    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
