import SentimentChart from "../components/SentimentChart";

export default function Home() {
  return (
    <div className="space-y-10">
      {/* Page header */}
      <div className="text-center space-y-2">
        <h1 className="text-3xl font-extrabold text-gray-900">
          📈 News Sentiment Dashboard
        </h1>
        <p className="text-gray-600">
          Visualizing daily sentiment trends across categories
        </p>
      </div>

      {/* Charts section */}
      <div className="grid gap-10">
        <section>
          <h2 className="text-xl font-semibold mb-4 text-gray-800">Overall</h2>
          <div className="bg-white shadow rounded-lg p-4">
            <SentimentChart />
          </div>
        </section>

        <section>
          <h2 className="text-xl font-semibold mb-4 text-gray-800">Business</h2>
          <div className="bg-white shadow rounded-lg p-4">
            <SentimentChart defaultCategory="business" />
          </div>
        </section>

        <section>
          <h2 className="text-xl font-semibold mb-4 text-gray-800">World</h2>
          <div className="bg-white shadow rounded-lg p-4">
            <SentimentChart defaultCategory="world" />
          </div>
        </section>

        <section>
          <h2 className="text-xl font-semibold mb-4 text-gray-800">Technology</h2>
          <div className="bg-white shadow rounded-lg p-4">
            <SentimentChart defaultCategory="technology" />
          </div>
        </section>
      </div>
    </div>
  );
}
