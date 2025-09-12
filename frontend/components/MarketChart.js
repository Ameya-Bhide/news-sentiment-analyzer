import { ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts";

export default function MarketChart({ data }) {
  return (
    <ResponsiveContainer width="100%" height={400}>
      <ScatterChart>
        <CartesianGrid />
        <XAxis dataKey="Metric" type="category" />
        <YAxis dataKey="Correlation" type="number" />
        <Tooltip cursor={{ strokeDasharray: "3 3" }} />
        <Legend />
        <Scatter name="Lag Analysis" data={data} fill="#8884d8" />
      </ScatterChart>
    </ResponsiveContainer>
  );
}
