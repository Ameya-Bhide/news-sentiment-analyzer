// pages/market.js
import MarketChart from "../components/MarketChart";

export default function Market() {
  return (
    <div className="space-y-10">
      {/* Page header */}
      <div className="text-center space-y-2">
        <h1 className="text-3xl font-extrabold text-gray-900">
          📊 Market Sentiment Correlation
        </h1>
        <p className="text-gray-600">
          Exploring how news sentiment trends relate to market returns
        </p>
      </div>

      {/* Charts section */}
      <div className="grid gap-10">
        <section>
          <h2 className="text-xl font-semibold mb-4 text-gray-800">S&amp;P 500</h2>
          <div className="bg-white shadow rounded-lg p-4">
            <MarketChart ticker="^GSPC" />
          </div>
        </section>

        <section>
          <h2 className="text-xl font-semibold mb-4 text-gray-800">NASDAQ</h2>
          <div className="bg-white shadow rounded-lg p-4">
            <MarketChart ticker="^IXIC" />
          </div>
        </section>

        <section>
          <h2 className="text-xl font-semibold mb-4 text-gray-800">Dow Jones</h2>
          <div className="bg-white shadow rounded-lg p-4">
            <MarketChart ticker="^DJI" />
          </div>
        </section>
      </div>
    </div>
  );
}
