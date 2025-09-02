# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# news_sentiment/pipelines.py

import csv
import os
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


class NewsSentimentPipeline:
    CSV_PATH = "headlines.csv"
    # Add a "sentiment" column at the end
    HEADER = ["scraped_at", "headline", "source", "category", "url", "published", "sentiment"]

    def __init__(self):
        # VADER analyzer is lightweight; safe to init here
        self.analyzer = SentimentIntensityAnalyzer()
        self.seen_urls = set()

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

        # parse/normalize published → UTC
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

        # cutoff (keeps your “recent only” behavior)
        cutoff = datetime(2025, 1, 1, tzinfo=timezone.utc)
        if published_dt and published_dt < cutoff:
            return item  # skip old items

        # VADER sentiment on headline text
        headline = (item.get("headline") or "").strip()
        if headline:
            scores = self.analyzer.polarity_scores(headline)
            sentiment = round(scores["compound"], 4)  # -1..+1
        else:
            sentiment = ""  # empty headline, leave blank

        scraped_at = datetime.now(timezone.utc).isoformat()

        self.writer.writerow([
            scraped_at,
            headline,
            item.get("source"),
            item.get("category"),
            url,
            published_raw,
            sentiment,
        ])
        return item
