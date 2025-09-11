import fs from "fs";
import path from "path";
import Papa from "papaparse";

export default function handler(req, res) {
  const filePath = path.join(process.cwd(), "data", "daily_summary.csv");
  if (!fs.existsSync(filePath)) {
    return res.status(404).json({ error: "daily_summary.csv not found" });
  }

  const file = fs.readFileSync(filePath, "utf8");
  const parsed = Papa.parse(file, { header: true, dynamicTyping: true });
  res.status(200).json(parsed.data.filter((row) => row.date)); // filter out blanks
}
