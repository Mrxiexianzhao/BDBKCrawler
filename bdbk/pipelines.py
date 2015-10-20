# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os

from bdbk.items import CategoryItem
from bdbk.items import PersonItem
from bdbk.items import ImageItem

import pymongo

TBL_CATEGORY_INFO = 'category_info'
TBL_PERSION_INFO = 'person_info'
TBL_IMAGE_INFO = 'image_info'

class StoreDBPipeline(object):
    def __init__(self, mongodb_url, mongodb_dbname):
        self.mongodb_url = mongodb_url
        self.mongodb_dbname = mongodb_dbname
        self.categori_id = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongodb_url = crawler.settings.get('MONGODB_URL'),
            mongodb_dbname = crawler.settings.get('MONGODB_DB', 'bdbk')
        )

    def open_spider(self, spider):
        settings = spider.settings
        self.mongodb_client = pymongo.MongoClient(self.mongodb_url)
        self.mongodb_db = self.mongodb_client[self.mongodb_dbname]

    def close_spider(self, spider):
        self.mongodb_client.close()

    def process_item(self, item, spider):
        if isinstance(item, PersonItem):
            self.mongodb_db[TBL_PERSION_INFO].insert(dict(item))
        elif isinstance(item, ImageItem):
            self.mongodb_db[TBL_IMAGE_INFO].insert(dict(item))
        elif isinstance(item, dict):
            if self.categori_id == None:
              self.categori_id = self.mongodb_db[TBL_CATEGORY_INFO].insert_one(item).inserted_id
            else:
              if item.has_key('_id'):
                del item['_id']
              self.mongodb_db[TBL_CATEGORY_INFO].update_one({'_id': self.categori_id}, {'$set': item})
