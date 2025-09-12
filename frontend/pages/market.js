import { useState, useEffect } from "react";
import MarketChart from "../components/MarketChart";

export default function Market() {
  const [data, setData] = useState([]);

  const fetchData = async () => {
    const res = await fetch("http://localhost:4000/api/market?ticker=^GSPC");
    const json = await res.json();
    setData(json);
  };

  useEffect(() => {
    fetchData();
  }, []);

  return (
    <div style={{ padding: "2rem" }}>
      <h1>📈 Sentiment vs Market Index</h1>
      <MarketChart data={data} />
    </div>
  );
}
