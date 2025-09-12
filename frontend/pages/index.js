import { useState, useEffect } from "react";
import SentimentChart from "../components/SentimentChart";
import Filters from "../components/Filters";

export default function Home() {
  const [data, setData] = useState([]);

  const fetchData = async (filters = {}) => {
    const params = new URLSearchParams(filters).toString();
    const res = await fetch(`http://localhost:4000/api/sentiment?${params}`);
    const json = await res.json();

    // convert date to readable format
    const formatted = json.map(d => ({
      ...d,
      date: new Date(d.date).toISOString().split("T")[0],
      avg_vader: parseFloat(d.avg_vader),
      finbert_pos: parseFloat(d.finbert_pos),
      finbert_neg: parseFloat(d.finbert_neg),
      finbert_neu: parseFloat(d.finbert_neu),
    }));

    setData(formatted);
  };

  useEffect(() => {
    fetchData();
  }, []);

  return (
    <div style={{ padding: "2rem" }}>
      <h1>📊 News Sentiment Trends</h1>
      <Filters onFilter={fetchData} />
      <SentimentChart data={data} />
    </div>
  );
}
