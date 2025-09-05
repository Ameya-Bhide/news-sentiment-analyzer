# analyze_market.py

import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf
from scipy.stats import pearsonr
import os

def analyze_sentiment_vs_indexes(daily_csv="daily_summary.csv"):
    # Load sentiment summary
    daily = pd.read_csv(daily_csv)
    daily["date"] = pd.to_datetime(daily["date"])

    # Only keep business + technology categories
    daily = daily[daily["category"].str.lower().isin(["business", "technology"])]

    # Aggregate across those categories per day
    daily = (
        daily.groupby("date")
        .agg(
            avg_vader=("avg_vader", "mean"),
            finbert_pos=("finbert_pos", "mean"),
            finbert_neg=("finbert_neg", "mean"),
            finbert_neu=("finbert_neu", "mean"),
        )
        .reset_index()
    )

    # Choose indexes to analyze
    indexes = {
        "S&P 500": "^GSPC",
        "NASDAQ": "^IXIC",
        "Dow Jones": "^DJI",
    }

    os.makedirs("market_plots", exist_ok=True)

    for name, ticker in indexes.items():
        print(f"\n=== {name} ({ticker}) ===")

        # Download market data
        market = yf.download(ticker, start=daily["date"].min(), end=daily["date"].max(), progress=False)

        if market.empty:
            print(f"⚠️ No market data for {ticker}, skipping.")
            continue

        # --- Flatten MultiIndex if present ---
        if isinstance(market.columns, pd.MultiIndex):
            market.columns = ["_".join([c for c in col if c]).strip() for col in market.columns.values]

        # Find the close column (handles Close_^GSPC etc.)
        close_col = None
        for col in market.columns:
            if col.lower().startswith("close"):
                close_col = col
                break

        if close_col is None:
            print(f"⚠️ Could not find a close column in market data for {ticker}. Columns were: {market.columns.tolist()}")
            continue

        # Reset index and rename
        market = market.reset_index()[["Date", close_col]].rename(
            columns={"Date": "date", close_col: "close"}
        )
        market["date"] = pd.to_datetime(market["date"])

        # Compute daily returns (% change)
        market["returns"] = market["close"].pct_change() * 100

        # Merge with sentiment
        merged = pd.merge(daily, market, on="date", how="inner")

        if merged.empty:
            print(f"⚠️ No overlapping dates for {ticker}, skipping.")
            continue

        print(f"✅ Merged dataset for {ticker}, {len(merged)} days of overlap")

        # --- Correlations ---
        for var in ["avg_vader", "finbert_pos"]:
            r, p = pearsonr(merged[var].fillna(0), merged["returns"].fillna(0))
            print(f"{var} vs {ticker} returns: r = {r:.3f}, p = {p:.4f}")

        # --- Plot ---
        plt.figure(figsize=(10, 5))
        plt.plot(merged["date"], merged["returns"], marker="o", label=f"{ticker} Daily Returns (%)")
        plt.plot(merged["date"], merged["avg_vader"], marker="o", label="VADER Avg Sentiment")
        plt.plot(merged["date"], merged["finbert_pos"], marker="o", label="FinBERT % Positive")
        plt.axhline(0, color="gray", linestyle="--")
        plt.title(f"Sentiment vs {name} Returns")
        plt.xlabel("Date")
        plt.ylabel("Returns / Sentiment")
        plt.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()

        out_path = f"market_plots/sentiment_vs_{ticker.replace('^','')}.png"
        plt.savefig(out_path)
        plt.close()
        print(f"📈 Plot saved: {out_path}")


if __name__ == "__main__":
    analyze_sentiment_vs_indexes()
