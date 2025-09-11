import { useEffect, useState } from "react";
import Papa from "papaparse";
import { Line } from "react-chartjs-2";
import {
  Chart as ChartJS,
  LineElement,
  CategoryScale,
  LinearScale,
  PointElement,
  Legend,
  Tooltip,
} from "chart.js";

ChartJS.register(LineElement, CategoryScale, LinearScale, PointElement, Legend, Tooltip);

export default function Home() {
  const [data, setData] = useState(null);

  useEffect(() => {
    fetch("/api/summary")
      .then((res) => res.json())
      .then((json) => setData(json));
  }, []);

  if (!data) return <p>Loading...</p>;

  const chartData = {
    labels: data.map((row) => row.date),
    datasets: [
      {
        label: "Positive",
        data: data.map((row) => parseFloat(row.finbert_pos)),
        borderColor: "green",
        fill: false,
      },
      {
        label: "Negative",
        data: data.map((row) => parseFloat(row.finbert_neg)),
        borderColor: "red",
        fill: false,
      },
      {
        label: "Neutral",
        data: data.map((row) => parseFloat(row.finbert_neu)),
        borderColor: "gray",
        fill: false,
      },
    ],
  };

  return (
    <div style={{ padding: "2rem" }}>
      <h1>📊 News Sentiment Dashboard</h1>
      <Line data={chartData} />
    </div>
  );
}
