import fs from "fs";
import path from "path";
import csv from "csv-parser";
import { Readable } from "stream";


/*
export function loadCSV(filePath) {
  return new Promise((resolve, reject) => {
    console.log("promise")
    const rows = [];
    const fullPath = path.resolve(filePath);
    console.log(fullPath)
    console.log(filePath)
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
  */

export async function loadRemoteCSV(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch CSV: ${res.status}`);
  const text = await res.text();

  return new Promise((resolve, reject) => {
    const rows = [];
    Readable.from(text)
      .pipe(csv())
      .on("data", (row) => rows.push(row))
      .on("end", () => resolve(rows))
      .on("error", reject);
  });
}

