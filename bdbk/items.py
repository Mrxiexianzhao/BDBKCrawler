# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class RerunItem(scrapy.Item):
    rerun = scrapy.Field()

class PersonItem(scrapy.Item):
    name = scrapy.Field()
    url = scrapy.Field()
    tags = scrapy.Field()
    keywords = scrapy.Field()
    description = scrapy.Field()
    summary_pic = scrapy.Field()

class AlbumItem(scrapy.Item):
    url = scrapy.Field()
    description = scrapy.Field()
    total = scrapy.Field()
    cover_pic = scrapy.Field()
    person_name = scrapy.Field()
    person_url = scrapy.Field()

class ImageItem(scrapy.Item):
    src = scrapy.Field()
    url = scrapy.Field()
    is_cover = scrapy.Field()
    album_url = scrapy.Field()
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

class ErrorInfoItem(scrapy.Item):
    time = scrapy.Field()
    url = scrapy.Field()
    error_level = scrapy.Field()
    error_type = scrapy.Field()
    description = scrapy.Field()
