// components/MarketChart.js
import { useEffect, useState } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer } from "recharts";

export default function MarketChart({ ticker }) {
  const [data, setData] = useState([]);

  useEffect(() => {
    async function fetchData() {
      try {
        const res = await fetch(`http://localhost:4000/api/market?ticker=${encodeURIComponent(ticker)}`);
        const json = await res.json();

        // Example: filter correlations for this ticker
        const parsed = json.map(d => ({
          metric: `${d.Metric} (lag ${d["Lag (days)"]})`,
          correlation: parseFloat(d.Correlation),
        }));

        setData(parsed);
      } catch (err) {
        console.error("Failed to fetch market data", err);
      }
    }
    fetchData();
  }, [ticker]);

  if (!data.length) return <p>No data for {ticker}</p>;

  return (
    <ResponsiveContainer width="100%" height={300}>
      <BarChart data={data}>
        <XAxis dataKey="metric" angle={-30} textAnchor="end" interval={0} />
        <YAxis domain={[-1, 1]} />
        <Tooltip />
        <Legend content={<CustomLegend />} />
        <Bar dataKey="correlation" fill="#8884d8" />
      </BarChart>
    </ResponsiveContainer>
  );
}

const CustomLegend = () => {
  return (
    <div style={{ textAlign: "center", marginTop: "4rem" }}>
      <span style={{ color: "#8884d8", fontWeight: "bold" }}>■ correlation</span>
    </div>
  );
};

