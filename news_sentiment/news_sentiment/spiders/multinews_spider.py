import os
import json
import scrapy
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode


class MultiNewsSpider(scrapy.Spider):
    name = "multinews"
    custom_settings = {
        "DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
    }

    def __init__(self, feeds_file=None, source=None, category=None, *args, **kwargs):
        """
        Args (all optional):
          feeds_file: path to feeds.json (defaults to ./feeds.json in repo root)
          source: comma-separated filter, e.g. "Reuters,The Guardian"
          category: comma-separated filter, e.g. "business,world"
        """
        super().__init__(*args, **kwargs)
        self.feeds = self._load_feeds(feeds_file)

        # optional filters
        src_set = {s.strip().lower() for s in source.split(",")} if source else None
        cat_set = {c.strip().lower() for c in category.split(",")} if category else None

        if src_set or cat_set:
            before = len(self.feeds)
            self.feeds = [
                f for f in self.feeds
                if (not src_set or f["source"].lower() in src_set)
                and (not cat_set or f["category"].lower() in cat_set)
            ]
            self.logger.info(f"Filtered feeds: {before} -> {len(self.feeds)}")

    def _load_feeds(self, feeds_file):
        candidates = [feeds_file] if feeds_file else []
        candidates += ["feeds.json"]  # repo root default
        for path in candidates:
            if path and os.path.isfile(path):
                self.logger.info(f"Loading feeds from {path}")
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                feeds_list = data.get("feeds") if isinstance(data, dict) else data
                if not isinstance(feeds_list, list):
                    raise ValueError("feeds.json must be a list of {source, category, url} (or {'feeds': [...]})")

                normalized = []
                for i, row in enumerate(feeds_list):
                    try:
                        src = (row.get("source") or "").strip()
                        url = (row.get("url") or "").strip()
                        cat = (row.get("category") or "general").strip()
                        if not src or not url:
                            raise ValueError("missing source or url")
                        normalized.append({"source": src, "category": cat, "url": url})
                    except Exception as e:
                        self.logger.warning(f"Skipping invalid feed row #{i}: {e}")
                if normalized:
                    return normalized
                self.logger.warning("No valid feeds found in config; using built-in fallback.")

        # Minimal fallback so job still runs
        self.logger.warning("feeds.json not found; falling back to Reuters World.")
        return [
            {"source": "Reuters", "category": "world", "url": "https://www.reuters.com/rssFeed/worldNews"}
        ]

    async def start(self):
        headers = {
        "Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/135.0.0.0 Safari/537.36"
        }
        for feed in self.feeds:
            yield scrapy.Request(
                url=feed["url"],
                callback=self.parse_feed,
                cb_kwargs={"source": feed["source"], "category": feed["category"]},
                headers=headers,
            )

    def parse_feed(self, response, source, category):
        # RSS: <item>, Atom: <entry>
        items = response.css("item")
        is_atom = False
        if not items:
            items = response.css("entry")
            is_atom = True

        for it in items:
            title = it.css("title::text").get() or it.css("title *::text").get()

            if is_atom:
                link = it.css("link::attr(href)").get() or it.css("link::text").get()
                pub = it.css("updated::text").get() or it.css("published::text").get() or it.css("dc\\:date::text").get()
            else:
                link = it.css("link::text").get() or it.css("link::attr(href)").get()
                pub = it.css("pubDate::text").get() or it.css("dc\\:date::text").get()

            if not title or not link:
                continue

            yield {
                "headline": title.strip(),
                "source": source,
                "category": category,
                "url": self._strip_tracking_params(link.strip()),
                "published": (pub or "").strip(),
            }

    @staticmethod
    def _strip_tracking_params(url: str) -> str:
        try:
            parsed = urlparse(url)
            if not parsed.query:
                return url
            drop = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
                    "at_medium", "at_campaign", "CMP"}
            clean_qs = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k not in drop]
            return urlunparse(parsed._replace(query=urlencode(clean_qs, doseq=True)))
        except Exception:
            return url
