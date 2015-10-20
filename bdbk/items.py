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

class PersonItem(scrapy.Item):
    name = scrapy.Field()
    url = scrapy.Field()
    tags = scrapy.Field()
    keywords = scrapy.Field()
    description = scrapy.Field()
    summary_pic = scrapy.Field()

class ImageItem(scrapy.Item):
    src = scrapy.Field()
    url = scrapy.Field()
    is_cover = scrapy.Field()
    mime = scrapy.Field()
    desc = scrapy.Field()
    width = scrapy.Field()
    height = scrapy.Field()
    size = scrapy.Field()
    file_name = scrapy.Field()
    file_path = scrapy.Field()
    person_id = scrapy.Field()
    person_name = scrapy.Field()
    person_url = scrapy.Field()
