
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from hw2_scraper.items import Hw2ScraperItem
from scrapy.loader import ItemLoader
from urllib.parse import urlparse


class MySpider(CrawlSpider):
    name = 'newsSpiderCrawler'
    allowed_domains = ["latimes.com"]
    start_urls = ["https://www.latimes.com/"]
    handle_httpstatus_list=[200,301,302,401,403,404]
    
    custom_settings = {
                        'DEPTH_LIMIT': 16, 
                        'CLOSESPIDER_PAGECOUNT': 20000,
                        'CONCURRENT_REQUESTS_PER_DOMAIN': 16,
                        'CONCURRENT_REQUESTS': 32,
                        'HTTPERROR_ALLOW_ALL' : True, 
                        'HTTPCACHE_ENABLED' : True,
                        'AUTOTHROTTLE_ENABLED' : True, 
                        }
    
    rules = (
         Rule(LinkExtractor(allow = '/*', allow_domains="latimes.com"), callback='parse_train', follow=True),
    )

    def parse_train(self, response):
        l = ItemLoader(item=Hw2ScraperItem(), response=response)
        # print("Parsing "+ response.urls + ": " + response)
        l.add_value('news_url', response.url)
        l.add_value('status_code', response.status)

        if response.status == 200:
          #l.add_value('response_size', response.headers.get("Content-Length",b"").decode("utf-8"))
          l.add_value('response_size',len(response.body))
          l.add_value('outlinks', len(response.css('a::attr(href)').getall()))
          l.add_value('contentType', response.headers.get("Content-Type", b"").decode("utf-8"))
          l.add_value('filename', "visit_ustoday")
        
        else:
          l.add_value('filename', "fetch_ustoday")
        
        if urlparse(response.url).netloc == urlparse("https://www.latimescom/").netloc:
          l.add_value('domain', 'OK')
        else:
          l.add_value('domain', 'N_OK')

        
        yield l.load_item()