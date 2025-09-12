import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts";

export default function SentimentChart({ data }) {
  return (
    <ResponsiveContainer width="100%" height={400}>
      <LineChart data={data}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="date" />
        <YAxis />
        <Tooltip />
        <Legend />
        <Line type="monotone" dataKey="avg_vader" stroke="#8884d8" name="VADER Avg" />
        <Line type="monotone" dataKey="finbert_pos" stroke="#82ca9d" name="FinBERT Positive %" />
        <Line type="monotone" dataKey="finbert_neg" stroke="#ff7300" name="FinBERT Negative %" />
        <Line type="monotone" dataKey="finbert_neu" stroke="#999999" name="FinBERT Neutral %" />
      </LineChart>
    </ResponsiveContainer>
  );
}
