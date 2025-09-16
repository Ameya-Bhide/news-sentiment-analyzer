// pages/api/sentiment.js
import { loadCSV } from "../../../utils/csvLoader.js";

const SENTIMENT_CSV = "news_sentiment/daily_summary.csv";

export default async function handler(req, res) {
  try {
    const { category, start, end } = req.query;
    let rows = await loadCSV(SENTIMENT_CSV);

    if (category) {
      rows = rows.filter(r => r.category.toLowerCase() === category.toLowerCase());
    }
    if (start) {
      rows = rows.filter(r => new Date(r.date) >= new Date(start));
    }
    if (end) {
      rows = rows.filter(r => new Date(r.date) <= new Date(end));
    }

    res.status(200).json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}
