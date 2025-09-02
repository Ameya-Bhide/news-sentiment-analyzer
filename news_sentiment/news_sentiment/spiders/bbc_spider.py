import scrapy
from news_sentiment.items import NewsSentimentItem

class BBCSpider(scrapy.Spider):
    name = "bbc"
    allowed_domains = ["bbc.com"]
    allowed_domains = ["bbc.com", "bbci.co.uk"]
    start_urls = [
        "https://feeds.bbci.co.uk/news/business/rss.xml",  # finance
        "https://feeds.bbci.co.uk/news/technology/rss.xml" # tech
    ]

    def parse(self, response):
        # Loop through each <item> in the RSS feed
        for item in response.css("item"):
            category = response.url.split("/")[-2] # business or tech
            headline = item.css("title::text").get()
            link = item.css("link::text").get()
            if headline:
                yield {
                    "headline": headline.strip(),
                    "source": "BBC",
                    "category": category,
                    "url": link
                }

        # (commented out for now, since RSS is cleaner)
        # HTML fallback
        """
        start_urls = ["https://www.bbc.com/news"]

        def parse(self, response):
            for sel in response.css("h3, h2"):
                headline = sel.css("a::text").get()
                if headline:
                    item = NewsSentimentItem()
                    item["headline"] = headline.strip()
                    item["source"] = "BBC"
                    item["url"] = response.url
                    yield item
        """
