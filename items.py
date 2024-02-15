# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class Hw2ScraperItem(scrapy.Item):
    # define the fields for your item here like:
    filename = scrapy.Field()
    news_url = scrapy.Field()
    status_code = scrapy.Field()
    response_size = scrapy.Field()
    contentType = scrapy.Field()
    outlinks = scrapy.Field()
    domain = scrapy.Field()
    pass
