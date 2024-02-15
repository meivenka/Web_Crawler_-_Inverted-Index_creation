# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exporters import CsvItemExporter
from scrapy import signals



class Hw2ScraperPipeline(object):
    saveFiles = ['fetch_latimes','visit_latimes','urls_latimes']

    def open_spider(self, spider):
        self.files = dict([ (name, open(name+'.csv','w+b')) for name in self.saveFiles ])
        self.exporters = dict([ (name,CsvItemExporter(self.files[name])) for name in self.saveFiles])
        [e.start_exporting() for e in self.exporters.values()]
    
    def close_spider(self, spider):
        [e.finish_exporting() for e in self.exporters.values()]
        [f.close() for f in self.files.values()]


    
    def process_item(self, item, spider):
        if item['filename'] == "fetch_latimes":
            self.exporters["fetch_latimes"].export_item({'url': item['news_url'], 'status': item['status_code']})
            
        else:
            # if status == 200 -> write in both files
            self.exporters["fetch_latimes"].export_item({'url': item['news_url'], 'status': item['status_code']})
            self.exporters["visit_latimes"].export_item({'url': item['news_url'], 'size': item['response_size'], 'outlink': item['outlinks'], 'Content-Type': item['contentType']})

        # Everything will be here
        self.exporters["urls_latimes"].export_item({'url': item['news_url'], 'domain': item['domain']})
        return item