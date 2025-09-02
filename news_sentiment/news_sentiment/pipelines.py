# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import csv
import os
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from itemadapter import ItemAdapter


class NewsSentimentPipeline:
    CSV_PATH = "headlines.csv"
    HEADER = ["scraped_at", "headline", "source", "category", "url", "published"]

    def open_spider(self, spider):
        file_exists = os.path.isfile(self.CSV_PATH)
        self.file = open(self.CSV_PATH, "a", newline="", encoding="utf-8")
        self.writer = csv.writer(self.file)
        if not file_exists:
            self.writer.writerow(self.HEADER)
        self.seen_urls = set()

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        url = (item.get("url") or "").strip()
        if url and url in self.seen_urls:
            return item
        if url:
            self.seen_urls.add(url)

        # parse published date safely
        published_raw = item.get("published")
        published_dt = None
        if published_raw:
            try:
                published_dt = parsedate_to_datetime(published_raw)
                # normalize to UTC and strip tzinfo if missing
                if published_dt.tzinfo is None:
                    published_dt = published_dt.replace(tzinfo=timezone.utc)
                else:
                    published_dt = published_dt.astimezone(timezone.utc)
            except Exception:
                published_dt = None

        # compare against cutoff (in UTC)
        cutoff = datetime(2024, 1, 1, tzinfo=timezone.utc)
        if published_dt and published_dt < cutoff:
            return item  # skip old articles

        scraped_at = datetime.now(timezone.utc).isoformat()

        #placeholder for future sentiment score
        sentiment = "N/A"
        self.writer.writerow([
            scraped_at,
            item.get("headline"),
            item.get("source"),
            item.get("category"),
            url,
            published_raw,
            sentiment,
        ])
        return item
