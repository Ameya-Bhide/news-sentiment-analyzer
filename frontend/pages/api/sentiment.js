// pages/api/sentiment.js
import { loadRemoteCSV } from "./csvLoader.js";

const SENTIMENT_CSV = "https://wutelclgudapnuyrucfn.supabase.co/storage/v1/object/public/sentiment-data/daily_summary.csv";

export default async function handler(req, res) {
  try {
    const { category, start, end } = req.query;
    let rows = await loadRemoteCSV(SENTIMENT_CSV);

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
