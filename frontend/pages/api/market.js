// pages/api/market.js
import { loadCSV } from "./csvLoader.js";

const MARKET_CSV = "news_sentiment/market_sentiment_results.csv";

export default async function handler(req, res) {
  try {
    const { ticker, lag } = req.query;
    let rows = await loadCSV(MARKET_CSV);

    if (ticker) {
      rows = rows.filter(r => r.Ticker === ticker);
    }
    if (lag) {
      rows = rows.filter(r => parseInt(r["Lag (days)"], 10) === parseInt(lag, 10));
    }

    res.status(200).json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}
