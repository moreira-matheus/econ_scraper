# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from pydispatch import dispatcher
from datetime import datetime

from scrapy import Item
from scrapy import signals
from scrapy.exceptions import DropItem
from scrapy.exporters import CsvItemExporter

import os

OUTPUT_DIR = './output/'

def create_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

class WebscrapingPipeline:
    def process_item(self, item, spider):
        return item

class CsvPipeline:
    def __init__(self):
        dispatcher.connect(self.spider_opened, signals.spider_opened)
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_opened(self, spider):
        create_output_dir()
        today = datetime.today().strftime('%Y%m%d')
        fname = OUTPUT_DIR + f"output_{spider.name}_{today}.csv"
        self.file = open(fname, 'w+b')
        self.exporter = CsvItemExporter(file=self.file,
                                        join_multivalued=',',
                                        **{'delimiter': ';'})
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        # If item is empty
        if not bool(item):
            raise DropItem
        # If all values are empty
        if all([not bool(v) for v in item.values]):
            raise DropItem

        self.exporter.export_item(item)

        return item
