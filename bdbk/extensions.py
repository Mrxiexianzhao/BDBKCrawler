# -*- coding: utf-8 -*-

from scrapy import signals

from bdbk.items import ErrorInfoItem
from bdbk.utils import now_string

class BDBKErrorStore(object):

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(
        )

        crawler.signals.connect(ext.spider_opened, signal = signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal = signals.spider_closed)
        crawler.signals.connect(ext.spider_error, signal = signals.spider_error)

        return ext

    def spider_opened(self, spider):
        now = now_string()
        message = 'bdbk spider start at: {0}'.format(now)
        ei_item = ErrorInfoItem()
        ei_item['time'] = now
        ei_item['url'] = spider.start_page
        ei_item['error_level'] = "I"
        ei_item['error_type'] = "I1"
        ei_item['description'] = message
        spider.logger.info(message)
        return ei_item

    def spider_closed(self, spider):
        now = now_string()
        message = 'bdbk spider end at: {0}'.format(now)
        ei_item = ErrorInfoItem()
        ei_item['time'] = now
        ei_item['url'] = spider.start_page
        ei_item['error_level'] = "I"
        ei_item['error_type'] = "I2"
        ei_item['description'] = message
        spider.logger.info(message)
        return ei_item

    def spider_error(self, failure, response, spider):
        message = 'Error: {0}'.format(failure.getErrorMessage())
        ei_item = ErrorInfoItem()
        ei_item['time'] = now_string()
        ei_item['url'] = response.url
        ei_item['error_level'] = "E"
        ei_item['error_type'] = "E1000"
        ei_item['description'] = message
        spider.logger.error(message)
        return ei_item

