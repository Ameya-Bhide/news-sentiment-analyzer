# news_sentiment/analyze_market.py

import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf
from scipy.stats import pearsonr
import os

def analyze_sentiment_vs_market(
    sentiment_csv="daily_summary.csv",
    tickers=["^GSPC", "^NDX", "^URTH"],
    start="2024-01-01",
    end=None
):
    # --- Load sentiment data ---
    sentiment = pd.read_csv(sentiment_csv)

    # Only business and technology categories
    sentiment = sentiment[sentiment["category"].str.lower().isin(["business", "technology"])]

    # Parse date
    sentiment["date"] = pd.to_datetime(sentiment["date"])
    daily = sentiment.groupby("date").agg(
        avg_vader=("avg_vader", "mean"),
        finbert_pos=("finbert_pos", "mean"),
        finbert_neg=("finbert_neg", "mean"),
        finbert_neu=("finbert_neu", "mean"),
    ).reset_index()

    results = []

    os.makedirs("market_plots", exist_ok=True)

    for ticker in tickers:
        print(f"\n📊 Analyzing {ticker}...")

        # --- Load market data ---
        market = yf.download(ticker, start=start, end=end, progress=False)
        if market.empty:
            print(f"⚠️ No market data for {ticker}, skipping.")
            continue

        # Flatten multi-index (if needed)
        if isinstance(market.columns, pd.MultiIndex):
            market.columns = [col[0] for col in market.columns]

        market = market[["Close"]].reset_index()
        market["date"] = pd.to_datetime(market["Date"]).dt.date
        market = market.drop(columns=["Date"])
        market["return"] = market["Close"].pct_change()

        # --- Merge with sentiment ---
        daily["date"] = pd.to_datetime(daily["date"]).dt.date
        merged = pd.merge(daily, market, on="date", how="inner")

        if merged.empty:
            print(f"⚠️ No overlap between sentiment and market for {ticker}.")
            continue

        print(f"✅ Merged dataset for {ticker}, {len(merged)} days of overlap")

        # --- Same-day correlations ---
        for col in ["avg_vader", "finbert_pos"]:
            r, p = pearsonr(merged[col].fillna(0), merged["return"].fillna(0))
            print(f"Same-day: {col} vs {ticker} returns: r={r:.3f}, p={p:.4f}")
            results.append([ticker, "same_day", col, r, p])

        # --- Next-day correlations ---
        merged["next_return"] = merged["return"].shift(-1)
        for col in ["avg_vader", "finbert_pos"]:
            r, p = pearsonr(merged[col].fillna(0), merged["next_return"].fillna(0))
            print(f"Next-day: {col} vs {ticker} returns: r={r:.3f}, p={p:.4f}")
            results.append([ticker, "next_day", col, r, p])

        # --- Plot ---
        plt.figure(figsize=(10, 5))
        plt.plot(merged["date"], merged["return"], label=f"{ticker} Returns", color="black")
        plt.plot(merged["date"], merged["avg_vader"], label="VADER Avg Sentiment", color="blue")
        plt.plot(merged["date"], merged["finbert_pos"]/100, label="FinBERT % Positive (scaled)", color="green")
        plt.axhline(0, color="gray", linestyle="--")
        plt.title(f"Sentiment vs {ticker} Returns (Business + Tech News)")
        plt.xlabel("Date")
        plt.ylabel("Returns / Sentiment")
        plt.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
        out_path = f"market_plots/sentiment_vs_{ticker.replace('^','')}.png"
        plt.savefig(out_path)
        plt.close()
        print(f"📈 Plot saved: {out_path}")

    # --- Save correlation results ---
    results_df = pd.DataFrame(results, columns=["ticker", "alignment", "sentiment", "r", "p"])
    results_df.to_csv("market_sentiment_correlations.csv", index=False)
    print("\n✅ Correlation results saved to market_sentiment_correlations.csv")


if __name__ == "__main__":
    analyze_sentiment_vs_market()
