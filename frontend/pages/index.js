import SentimentChart from "../components/SentimentChart";

export default function Home() {
  return (
    <main style={{ padding: "2rem" }}>
      <h1 className="text-3x1 font-bold underline">
        Hello World
      </h1>
      <h1>📈 News Sentiment Dashboard</h1>
      <h2>Overall</h2>
      <SentimentChart />

      <h2>Business</h2>
      <SentimentChart category="business" />

      <h2>World</h2>
      <SentimentChart category="world" />

      <h2>Technology</h2>
      <SentimentChart category="technology" />
    </main>
  );
}
