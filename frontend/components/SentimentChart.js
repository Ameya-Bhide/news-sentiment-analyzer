"use client";
import React, { useEffect, useState } from "react";
import {
  LineChart, Line, XAxis, YAxis, Tooltip, Legend, CartesianGrid, ResponsiveContainer
} from "recharts";

export default function SentimentChart({ category }) {
  const [data, setData] = useState([]);

  useEffect(() => {
    const fetchData = async () => {
      let url = "http://localhost:4000/api/sentiment"; // backend API
      if (category) {
        url += `?category=${category}`;
      }
      const res = await fetch(url);
      
      const json = await res.json();
      console.log(json)
      // Parse numeric values
      const parsed = json.map(d => ({
        ...d,
        avg_vader: parseFloat(d.avg_vader),
        finbert_pos: parseFloat(d.finbert_pos),
        finbert_neg: parseFloat(d.finbert_neg),
        finbert_neu: parseFloat(d.finbert_neu),
      }));
      setData(parsed);
    };
    fetchData();
  }, [category]);

  return (
    <div style={{ width: "100%", height: 400 }}>
      <ResponsiveContainer>
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="date" />
          <YAxis />
          <Tooltip />
          <Legend />
          <Line type="monotone" dataKey="avg_vader" stroke="#8884d8" name="VADER Avg" />
          <Line type="monotone" dataKey="finbert_pos" stroke="#82ca9d" name="FinBERT Positive %" />
          <Line type="monotone" dataKey="finbert_neg" stroke="#ff6961" name="FinBERT Negative %" />
          <Line type="monotone" dataKey="finbert_neu" stroke="#ffc658" name="FinBERT Neutral %" />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
