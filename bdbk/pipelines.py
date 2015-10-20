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
# import MySQLdb

TBL_PERSION_INFO = 'person_info'
TBL_IMAGE_INFO = 'image_info'

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

      """
      try:
        self.mysql_conn = MySQLdb.connect(host=settings["DB_SERVER_HOST"], port=settings["DB_SERVER_PORT"], user=settings["DB_USER"], passwd=settings["DB_PASSWORD"], charset='utf8')
        cur = self.mysql_conn.cursor()
        cur.execute('CREATE DATABASE IF NOT EXISTS {0}'.format(settings["DB_NAME"]))
        self.mysql_conn.select_db(settings["DB_NAME"])

        cur.execute('''CREATE TABLE IF NOT EXISTS `{0}` ( \
            `pi_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT, \
            `pi_name` VARCHAR(100) CHARACTER SET 'utf8mb4' NOT NULL, \
            `pi_url` VARCHAR(255)  CHARACTER SET 'utf8' NOT NULL, \
            `pi_tags` VARCHAR(255)  CHARACTER SET 'utf8mb4' NOT NULL DEFAULT '', \
            `pi_keywords` VARCHAR(255) CHARACTER SET 'utf8mb4' NOT NULL DEFAULT '', \
            `pi_description` VARCHAR(10240) CHARACTER SET 'utf8mb4' NOT NULL DEFAULT '', \
            PRIMARY KEY (`pi_id`), \
            UNIQUE KEY `pi_url_UNIQUE` (`pi_url`), \
            INDEX `pi_name` (`pi_name`) \
        ) '''.format(TBL_PERSION_INFO))

        self.mysql_conn.commit()

        cur.close()
      except MySQLdb.Error, e:
        raise e
      """

    def close_spider(self, spider):
        self.mongodb_client.close()

        """
        try:
            if self.mysql_conn != None:
            self.mysql_conn.close()
            self.mysql_conn = None
        except MySQLdb.Error, e:
            raise e
        """


    def process_item(self, item, spider):
      if isinstance(item, PersonItem):
        self.mongodb_db[TBL_PERSION_INFO].insert(dict(item))
      elif isinstance(item, ImageItem):
        self.mongodb_db[TBL_IMAGE_INFO].insert(dict(item))

      """
      cur = self.mysql_conn.cursor()
      cur.execute('''INSERT INTO `{0}`(`pi_name`, `pi_url`, `pi_tags`, `pi_keywords`, `pi_description`) VALUES(
      '{1}', '{2}', '{3}', '{4}', '{5}')'''.format(
        TBL_PERSION_INFO,
        MySQLdb.escape_string(person_item['name']), 
        MySQLdb.escape_string(person_item['url']), 
        MySQLdb.escape_string(person_item['tags']), 
        MySQLdb.escape_string(person_item['keywords']), 
        MySQLdb.escape_string(person_item['description'])))
      self.mysql_conn.commit()
      cur.close()
      """
