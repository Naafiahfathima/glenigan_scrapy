import scrapy

class ApplicationItem(scrapy.Item):
    """Defines the structure of the scraped data."""
    ref_no = scrapy.Field()
    link = scrapy.Field()
