# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class CategoryItem(scrapy.Item):
    # define the fields for your item here like:
    name = scrapy.Field()
    count = scrapy.Field()

    def to_dic(self):
      d = {}
      d['name'] = self['name']
      d['count'] = self['count']
      return d


class PersonItem(scrapy.Item):
    name = scrapy.Field()
    url = scrapy.Field()
    tags = scrapy.Field()
    keywords = scrapy.Field()
    description = scrapy.Field()

    def to_dic(self):
      d = {}
      d['name'] = self['name']
      d['url'] = self['url']
      d['tags'] = self['tags']
      d['keywords'] = self['keywords']
      d['description'] = self['description']
      return d

class ImageItem(scrapy.Item):
    src = scrapy.Field()
    url = scrapy.Field()
    desc = scrapy.Field()
    width = scrapy.Field()
    height = scrapy.Field()
    size = scrapy.Field()
    file_name = scrapy.Field()
    file_path = scrapy.Field()
    person_id = scrapy.Field()
    person_name = scrapy.Field()
    person_url = scrapy.Field()

    def to_dic(self):
      d = {}
      d['src'] = self['src']
      d['url'] = self['url']
      d['desc'] = self['desc']
      d['width'] = self['width']
      d['height'] = self['height']
      d['size'] = self['size']
      d['file_name'] = self['file_name']
      d['file_path'] = self['file_path']
#      d['person_id'] = self['person_id']
      d['person_name'] = self['person_name']
      d['person_url'] = self['person_url']
      return d

