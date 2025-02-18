# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class HtmlScraperItem(scrapy.Item):
    ref_no = scrapy.Field()
    url = scrapy.Field()
    html_content = scrapy.Field()