import pandas as pd
import matplotlib.pyplot as plt
import os

def summarize_headlines(csv_path="headlines.csv", out_csv="daily_summary.csv"):
    # Load CSV
    df = pd.read_csv(csv_path)

    # Parse datetime
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
    df["date"] = df["scraped_at"].dt.date

    # Ensure numeric sentiment
    df["vader_sentiment"] = pd.to_numeric(df.get("vader_sentiment"), errors="coerce")

    # FinBERT labels if present
    if "finbert_label" in df.columns:
        df["finbert_label"] = df["finbert_label"].fillna("neutral")

    # --- Category-level summary ---
    summary = (
        df.groupby(["date", "category"])
        .agg(
            headlines=("headline", "count"),
            avg_vader=("vader_sentiment", "mean"),
            finbert_pos=("finbert_label", lambda x: (x == "positive").mean() * 100),
            finbert_neg=("finbert_label", lambda x: (x == "negative").mean() * 100),
            finbert_neu=("finbert_label", lambda x: (x == "neutral").mean() * 100),
        )
        .reset_index()
    )

    # --- Overall summary ---
    overall = (
        df.groupby("date")
        .agg(
            headlines=("headline", "count"),
            avg_vader=("vader_sentiment", "mean"),
            finbert_pos=("finbert_label", lambda x: (x == "positive").mean() * 100),
            finbert_neg=("finbert_label", lambda x: (x == "negative").mean() * 100),
            finbert_neu=("finbert_label", lambda x: (x == "neutral").mean() * 100),
        )
        .reset_index()
    )

    # Save CSV
    summary.to_csv(out_csv, index=False)
    print(f"✅ Daily summary saved to {out_csv}")

    os.makedirs("sentiment_plots", exist_ok=True)

    # --- Console summary (averages across dataset) ---
    print("\n📊 Averages across entire dataset:")
    print(f"Overall avg VADER: {df['vader_sentiment'].mean():.4f}")
    print(
        f"FinBERT -> Positive: {(df['finbert_label'] == 'positive').mean() * 100:.2f}% | "
        f"Negative: {(df['finbert_label'] == 'negative').mean() * 100:.2f}% | "
        f"Neutral: {(df['finbert_label'] == 'neutral').mean() * 100:.2f}%"
    )

    for cat in ["business", "world", "technology"]:
        sub = df[df["category"].str.lower() == cat]
        if sub.empty:
            continue
        print(f"\n[{cat.capitalize()}]")
        print(f"  Avg VADER: {sub['vader_sentiment'].mean():.4f}")
        print(
            f"  FinBERT -> Positive: {(sub['finbert_label'] == 'positive').mean() * 100:.2f}% | "
            f"Negative: {(sub['finbert_label'] == 'negative').mean() * 100:.2f}% | "
            f"Neutral: {(sub['finbert_label'] == 'neutral').mean() * 100:.2f}%"
        )

    # --- Function to plot sentiment trends ---
    def plot_sentiment(data, title, out_path):
        plt.figure(figsize=(10, 5))
        plt.plot(data["date"], data["avg_vader"].fillna(0), marker="o", label="VADER Avg Sentiment")
        plt.plot(data["date"], data["finbert_pos"].fillna(0), marker="o", label="FinBERT % Positive")
        plt.plot(data["date"], data["finbert_neg"].fillna(0), marker="o", label="FinBERT % Negative")
        plt.plot(data["date"], data["finbert_neu"].fillna(0), marker="o", label="FinBERT % Neutral")
        plt.axhline(0, color="gray", linestyle="--")
        plt.title(title)
        plt.xlabel("Date")
        plt.ylabel("Score / % of Headlines")
        plt.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(out_path)
        plt.close()
        print(f"📈 Plot saved: {out_path}")

    # --- Overall plot ---
    plot_sentiment(overall, "Daily Sentiment Trends (Overall)", "sentiment_plots/sentiment_trend.png")

    # --- Per-category plots ---
    for cat in ["business", "world", "technology"]:
        sub = summary[summary["category"].str.lower() == cat]
        if sub.empty:
            print(f"⚠️ No data for {cat}, skipping plot.")
            continue
        out_path = f"sentiment_plots/sentiment_trend_{cat}.png"
        plot_sentiment(sub, f"Daily Sentiment Trends ({cat.capitalize()} News)", out_path)


if __name__ == "__main__":
    summarize_headlines()
