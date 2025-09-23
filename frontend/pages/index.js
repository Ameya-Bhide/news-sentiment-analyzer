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

      <div className="bg-white shadow-md rounded-xl p-4 mb-6">
        <h2 className="text-lg font-semibold mb-2">📊 Sentiment Trends</h2>
        <p className="text-gray-600 text-sm">
          The charts show average daily sentiment of news headlines, using two models: 
          <span className="font-semibold"> VADER</span> (general sentiment) and 
          <span className="font-semibold"> FinBERT</span> (finance-specific, with % positive/negative/neutral).
          Use the filters below to explore by category and date range.
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
