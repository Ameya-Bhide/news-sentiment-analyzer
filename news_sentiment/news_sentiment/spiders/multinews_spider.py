import scrapy
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

# Feeds you want to aggregate (add more any time)
FEEDS = [
    # BBC
    {"source": "BBC", "category": "business",   "url": "https://feeds.bbci.co.uk/news/business/rss.xml"},
    {"source": "BBC", "category": "technology", "url": "https://feeds.bbci.co.uk/news/technology/rss.xml"},
    #{"source": "BBC", "category": "world",      "url": "https://feeds.bbci.co.uk/news/world/rss.xml"},
    #{"source": "BBC", "category": "sport",      "url": "https://feeds.bbci.co.uk/sport/rss.xml"},

    # CNN
    {"source": "CNN", "category": "business",   "url": "http://rss.cnn.com/rss/edition_business.rss"},
    {"source": "CNN", "category": "technology", "url": "http://rss.cnn.com/rss/edition_technology.rss"},
    #{"source": "CNN", "category": "world",      "url": "http://rss.cnn.com/rss/edition_world.rss"},

    # Reuters
    {"source": "Reuters", "category": "business",   "url": "https://www.reuters.com/rssFeed/businessNews"},
    {"source": "Reuters", "category": "technology", "url": "https://www.reuters.com/rssFeed/technologyNews"},
    #{"source": "Reuters", "category": "world",      "url": "https://www.reuters.com/rssFeed/worldNews"},
]

class MultiNewsSpider(scrapy.Spider):
    name = "multinews"
    allowed_domains = ["bbci.co.uk","bbc.com","rss.cnn.com","cnn.com","reuters.com","feeds.reuters.com"]

    custom_settings = {
        # polite + consistent encoding
        "DOWNLOAD_DELAY": 0.5,
        "FEED_EXPORT_ENCODING": "utf-8",
        # enable your pipeline if you want spider-local control (optional if already in settings.py)
        "ITEM_PIPELINES": {"news_sentiment.pipelines.NewsSentimentPipeline": 300}
    }

    def start_requests(self):
        for feed in FEEDS:
            yield scrapy.Request(
                url=feed["url"],
                callback=self.parse_feed,
                meta={"source": feed["source"], "category": feed["category"]},
            )

    def parse_feed(self, response):
        source   = response.meta["source"]
        category = response.meta["category"]

        # Handle both RSS (<item>) and Atom (<entry>)
        items = response.css("item")
        if not items:
            items = response.css("entry")

        for node in items:
            title = node.css("title::text").get() or ""
            title = title.strip()

            # Try multiple ways to get a link (RSS vs Atom differences)
            link = (
                node.css("link::text").get() or
                node.css("link::attr(href)").get() or
                node.css("guid::text").get() or
                ""
            )
            link = link.strip()

            # Optional: strip tracking params like ?at_medium=RSS&...
            link = self._strip_tracking(link, keys_to_remove={"at_medium", "at_campaign", "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content"})

            # Published date (if present)
            published = (
                node.css("pubDate::text").get() or  # RSS 2.0
                node.css("updated::text").get() or  # Atom
                node.css("dc\\:date::text").get()   # some feeds
            )
            if published:
                published = published.strip()

            if title and link:
                yield {
                    "headline": title,
                    "source": source,
                    "category": category,
                    "url": link,
                    "published": published,
                }

    def _strip_tracking(self, url, keys_to_remove):
        if not url:
            return url
        try:
            parts = urlparse(url)
            q = [(k, v) for (k, v) in parse_qsl(parts.query, keep_blank_values=True) if k not in keys_to_remove]
            new_q = urlencode(q, doseq=True)
            return urlunparse(parts._replace(query=new_q))
        except Exception:
            return url
