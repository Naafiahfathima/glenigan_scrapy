import scrapy

class ApplicationItem(scrapy.Item):
    """Defines the structure of the scraped data."""
    ref_no = scrapy.Field()
    link = scrapy.Field()

class HtmlScraperItem(scrapy.Item):
    ref_no = scrapy.Field()
    url = scrapy.Field()
    html_content = scrapy.Field()