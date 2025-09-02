import csv
import os
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from transformers import pipeline
import torch


class NewsSentimentPipeline:
    CSV_PATH = "headlines.csv"
    HEADER = [
        "scraped_at",
        "headline",
        "source",
        "category",
        "url",
        "published",
        "vader_sentiment",
        "finbert_label",
        "finbert_score",
    ]

    def __init__(self):
        # VADER is lightweight
        self.analyzer = SentimentIntensityAnalyzer()
        self.seen_urls = set()

        # Preload FinBERT sentiment model
        device = 0 if torch.cuda.is_available() else -1
        self.finbert = pipeline(
            "sentiment-analysis",
            model="ProsusAI/finbert",
            tokenizer="ProsusAI/finbert",
            device=device,
        )

    def open_spider(self, spider):
        file_exists = os.path.isfile(self.CSV_PATH)
        self.file = open(self.CSV_PATH, "a", newline="", encoding="utf-8")
        self.writer = csv.writer(self.file)
        if not file_exists:
            self.writer.writerow(self.HEADER)

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        # de-dup within a run
        url = (item.get("url") or "").strip()
        if url and url in self.seen_urls:
            return item
        if url:
            self.seen_urls.add(url)

        # normalize published to UTC
        published_raw = item.get("published")
        published_dt = None
        if published_raw:
            try:
                dt = parsedate_to_datetime(published_raw)
                published_dt = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
                if published_dt.tzinfo is not timezone.utc:
                    published_dt = published_dt.astimezone(timezone.utc)
            except Exception:
                published_dt = None

        # cutoff to filter old items
        cutoff = datetime(2025, 1, 1, tzinfo=timezone.utc)
        if published_dt and published_dt < cutoff:
            return item

        headline = (item.get("headline") or "").strip()
        scraped_at = datetime.now(timezone.utc).isoformat()

        # --- VADER ---
        vader_sentiment = ""
        if headline:
            scores = self.analyzer.polarity_scores(headline)
            vader_sentiment = round(scores["compound"], 4)

        # --- FinBERT ---
        finbert_label, finbert_score = "N/A", "N/A"
        if headline:
            try:
                result = self.finbert(headline[:512])  # truncate if long
                if result and isinstance(result, list):
                    finbert_label = result[0]["label"]
                    finbert_score = round(float(result[0]["score"]), 4)
            except Exception as e:
                spider.logger.warning(f"FinBERT error on '{headline}': {e}")

        # Write CSV row
        self.writer.writerow([
            scraped_at,
            headline,
            item.get("source"),
            item.get("category"),
            url,
            published_raw,
            vader_sentiment,
            finbert_label,
            finbert_score,
        ])
        return item