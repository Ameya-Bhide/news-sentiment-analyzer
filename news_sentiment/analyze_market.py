# news_sentiment/analyze_market.py

import pandas as pd
import yfinance as yf
import matplotlib.pyplot as plt
from scipy.stats import pearsonr
import os

def analyze_sentiment_vs_indexes(
    sentiment_csv="daily_summary.csv",
    tickers={"^GSPC": "S&P 500", "^IXIC": "NASDAQ", "^DJI": "Dow Jones"}
):
    # Load daily sentiment summary
    daily = pd.read_csv(sentiment_csv)
    daily["date"] = pd.to_datetime(daily["date"])

    results = []

    for ticker, name in tickers.items():
        print(f"\n=== {name} ({ticker}) ===")
        market = yf.download(
            ticker,
            start=daily["date"].min(),
            end=daily["date"].max(),
            progress=False
        )

        if market.empty:
            print(f"⚠️ No market data for {ticker}")
            continue

        # Flatten column names if MultiIndex
        if isinstance(market.columns, pd.MultiIndex):
            market.columns = [c[0] if isinstance(c, tuple) else c for c in market.columns]

        # Ensure close column exists
        if "Close" not in market.columns:
            print(f"⚠️ Could not find a 'Close' column in market data for {ticker}. Columns were: {market.columns.tolist()}")
            continue

        # Prepare market data
        market = market[["Close"]].rename(columns={"Close": "close"})
        market["date"] = market.index.normalize()
        market["returns"] = market["close"].pct_change()

        # Lead-lag returns (1, 2, 3 days ahead)
        for lag in [1, 2, 3]:
            market[f"returns_t+{lag}"] = market["returns"].shift(-lag)

        # Merge with sentiment data
        merged = pd.merge(daily, market, on="date", how="inner")

        if merged.empty:
            print(f"⚠️ No overlap between sentiment and market for {ticker}")
            continue

        print(f"✅ Merged dataset for {ticker}, {len(merged)} days of overlap")

        # --- Correlations ---
        # Same-day
        for metric in ["avg_vader", "finbert_pos"]:
            sub = merged.dropna(subset=[metric, "returns"])
            if len(sub) > 1:
                r, p = pearsonr(sub[metric], sub["returns"])
                print(f"{metric} vs {ticker} returns: r = {r:.3f}, p = {p:.4f}")
                results.append([ticker, name, 0, metric, r, p])
            else:
                print(f"⚠️ Not enough data for {metric} vs {ticker} returns (same-day)")

        # Lead-lag
        for lag, col in [(1, "returns_t+1"), (2, "returns_t+2"), (3, "returns_t+3")]:
            for metric in ["avg_vader", "finbert_pos"]:
                if col in merged:
                    sub = merged.dropna(subset=[metric, col])
                    if len(sub) > 1:
                        r, p = pearsonr(sub[metric], sub[col])
                        print(f"{metric} vs {ticker} returns +{lag} days: r = {r:.3f}, p = {p:.4f}")
                        results.append([ticker, name, lag, metric, r, p])
                    else:
                        print(f"⚠️ Not enough data for {metric} vs {ticker} returns +{lag} days")

        # --- Plot sentiment vs returns ---
        plt.figure(figsize=(10, 5))
        plt.scatter(merged["avg_vader"], merged["returns"], label="VADER vs Returns", alpha=0.7)
        plt.scatter(merged["finbert_pos"], merged["returns"], label="FinBERT % Positive vs Returns", alpha=0.7)
        plt.axhline(0, color="gray", linestyle="--")
        plt.title(f"Sentiment vs {name} Returns")
        plt.xlabel("Sentiment Metric")
        plt.ylabel("Market Returns")
        plt.legend()
        plt.tight_layout()
        os.makedirs("market_plots", exist_ok=True)
        out_path = f"market_plots/sentiment_vs_{ticker.replace('^', '')}.png"
        plt.savefig(out_path)
        plt.close()
        print(f"📈 Plot saved: {out_path}")

    # Save results table
    if results:
        res_df = pd.DataFrame(results, columns=["Ticker", "Index", "Lag (days)", "Metric", "Correlation", "p-value"])
        res_df.to_csv("market_sentiment_results.csv", index=False)
        print("✅ Results saved to market_sentiment_results.csv")


if __name__ == "__main__":
    analyze_sentiment_vs_indexes()
