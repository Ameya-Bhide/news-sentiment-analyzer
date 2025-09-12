import fs from "fs";
import path from "path";
import csv from "csv-parser";

export function loadCSV(filePath) {
  return new Promise((resolve, reject) => {
    const rows = [];
    const fullPath = path.resolve(filePath);

    if (!fs.existsSync(fullPath)) {
      return reject(new Error(`CSV file not found: ${fullPath}`));
    }

    fs.createReadStream(fullPath)
      .pipe(csv())
      .on("data", (row) => rows.push(row))
      .on("end", () => resolve(rows))
      .on("error", (err) => reject(err));
  });
}
