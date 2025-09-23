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

      <div className="bg-white shadow-md rounded-xl p-4 mb-6">
        <h2 className="text-lg font-semibold mb-2">📈 Market Correlations</h2>
        <p className="text-gray-600 text-sm">
          The charts show correlations between news sentiment metrics and 
          index returns.
          Bars represent correlation values (from -1 to 1) for same-day and lagged returns.
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
