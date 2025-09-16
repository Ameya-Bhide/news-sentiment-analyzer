import { useState, useEffect } from "react";
import {
  LineChart, Line, CartesianGrid, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer
} from "recharts";

export default function SentimentChart() {
  const [data, setData] = useState([]);
  const [category, setCategory] = useState("");
  const [start, setStart] = useState("");
  const [end, setEnd] = useState("");

  useEffect(() => {
    const fetchData = async () => {
      try {
        let url = "/api/sentiment"; // ✅ relative path for Next.js/Vercel
        const params = [];
        if (category) params.push(`category=${category}`);
        if (start) params.push(`start=${start}`);
        if (end) params.push(`end=${end}`);
        if (params.length > 0) url += `?${params.join("&")}`;

        const res = await fetch(url);
        if (!res.ok) throw new Error(`API error: ${res.status}`);
        const json = await res.json();

        const parsed = json.map(d => ({
          ...d,
          avg_vader: parseFloat(d.avg_vader),
          finbert_pos: parseFloat(d.finbert_pos),
          finbert_neg: parseFloat(d.finbert_neg),
          finbert_neu: parseFloat(d.finbert_neu),
        }));

        setData(parsed);
      } catch (err) {
        console.error("Failed to fetch sentiment data:", err);
      }
    };

    fetchData();
  }, [category, start, end]);

  return (
    <div style={{ padding: "1rem" }}>
      {/* Controls */}
      <div style={{ marginBottom: "1rem", display: "flex", gap: "1rem", alignItems: "center" }}>
        <label>
          Category:{" "}
          <select value={category} onChange={e => setCategory(e.target.value)}>
            <option value="">All</option>
            <option value="business">Business</option>
            <option value="world">World</option>
            <option value="technology">Technology</option>
          </select>
        </label>

        <label>
          Start Date:{" "}
          <input type="date" value={start} onChange={e => setStart(e.target.value)} />
        </label>

        <label>
          End Date:{" "}
          <input type="date" value={end} onChange={e => setEnd(e.target.value)} />
        </label>
      </div>

      {/* Chart */}
      <ResponsiveContainer width="100%" height={400}>
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="date" />
          <YAxis />
          <Tooltip />
          <Legend />
          <Line type="monotone" dataKey="avg_vader" stroke="#8884d8" name="VADER Avg" />
          <Line type="monotone" dataKey="finbert_pos" stroke="#82ca9d" name="FinBERT Positive (%)" />
          <Line type="monotone" dataKey="finbert_neg" stroke="#ff6961" name="FinBERT Negative (%)" />
          <Line type="monotone" dataKey="finbert_neu" stroke="#ffc658" name="FinBERT Neutral (%)" />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
