# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os

from bdbk.items import PersonItem
from bdbk.items import ImageItem
from bdbk.items import AlbumItem
from bdbk.items import ErrorInfoItem
import pymongo

TBL_CATEGORY_INFO = 'category_info'
TBL_PERSON_INFO = 'person_info'
TBL_IMAGE_INFO = 'image_info'
TBL_ALBUM_INFO = 'album_info'
TBL_ERROR_INFO = 'error_info'

class StoreDBPipeline(object):
    def __init__(self, mongodb_url, mongodb_dbname):
        self.mongodb_url = mongodb_url
        self.mongodb_dbname = mongodb_dbname

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
        self.person_info_collection = self.mongodb_db[TBL_PERSON_INFO]
        self.image_info_collection = self.mongodb_db[TBL_IMAGE_INFO]
        self.album_info_collection = self.mongodb_db[TBL_ALBUM_INFO]

    def close_spider(self, spider):
        self.mongodb_client.close()

    def process_item(self, item, spider):
      try:
        if isinstance(item, ImageItem):
            self.image_info_collection.insert(dict(item))
        elif isinstance(item, AlbumItem):
            self.album_info_collection.insert(dict(item))
        elif isinstance(item, PersonItem):
            self.person_info_collection.insert(dict(item))
        elif isinstance(item, dict):
            category_doc = self.mongodb_db[TBL_CATEGORY_INFO]
            for k,v in item.items():
                category = category_doc.find_one({'name': k})
                if category == None:
                  category_doc.insert_one({'name': k, 'count': v})
                else:
                  category_doc.update_one({'name': k}, {'$set': {'count':v}})
        elif isinstance(item, ErrorInfoItem):
            self.mongodb_db[TBL_ERROR_INFO].insert(dict(item))
      except Exception, e:
        spider.logger.error("process_item error. item: %r, err: %r" % (item, e))

