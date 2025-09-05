# news_sentiment/analyze_market.py

import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf
import os

def analyze_sentiment_vs_market(
    sentiment_csv="daily_summary.csv",
    ticker="AAPL",
    start="2025-01-01",
    end=None
):
    # Load sentiment summary
    daily = pd.read_csv(sentiment_csv)
    daily["date"] = pd.to_datetime(daily["date"])

    # Download market data
    print(f"📥 Downloading market data for {ticker}...")
    market = yf.download(ticker, start=start, end=end)

    # Flatten MultiIndex if present
    if isinstance(market.columns, pd.MultiIndex):
        market.columns = ["_".join([str(c) for c in col if c]).strip()
                          for col in market.columns.values]
        print("flattened:", market.columns.tolist())

    # Try to select close price
    close_col = None
    for candidate in ["Adj Close", "Close", f"Adj Close_{ticker}", f"Close_{ticker}"]:
        if candidate in market.columns:
            close_col = candidate
            break

    if close_col is None:
        raise KeyError(f"❌ Could not find close price column in: {market.columns.tolist()}")

    market = market[[close_col]].rename(columns={close_col: "close"})
    market = market.reset_index().rename(columns={"Date": "date"})
    market["return"] = market["close"].pct_change() * 100

    # Merge with sentiment (overall only)
    merged = pd.merge(daily, market, on="date", how="inner")

    # Save merged CSV
    out_csv = f"market_sentiment_{ticker}.csv"
    merged.to_csv(out_csv, index=False)
    print(f"✅ Saved merged sentiment + market data to {out_csv}")

    # Plot correlation
    plt.figure(figsize=(10, 5))
    plt.scatter(merged["avg_vader"], merged["return"], alpha=0.6, label="VADER vs Returns")
    plt.scatter(merged["finbert_pos"], merged["return"], alpha=0.6, label="FinBERT % Pos vs Returns")
    plt.axhline(0, color="gray", linestyle="--")
    plt.title(f"Sentiment vs {ticker} Daily Returns")
    plt.xlabel("Sentiment Score / % Positive Headlines")
    plt.ylabel("Stock Return (%)")
    plt.legend()
    os.makedirs("market_plots", exist_ok=True)
    out_plot = f"market_plots/sentiment_vs_{ticker}.png"
    plt.savefig(out_plot)
    plt.close()
    print(f"📈 Plot saved: {out_plot}")

if __name__ == "__main__":
    analyze_sentiment_vs_market()
