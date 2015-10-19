# -*- coding: utf-8 -*-

import os
import errno 
import re
import json
import traceback

import scrapy
import redis # https://github.com/andymccurdy/redis-py

from bdbk.items import CategoryItem
from bdbk.items import PersonItem
from bdbk.items import ImageItem

# generic settings
BAIDU_DOMAIN = ['baidu.com']

class CategorySpider(scrapy.Spider):
    name = 'bdbk.category'
    allowed_domains = BAIDU_DOMAIN
    data_path = None
    redis_client = None

    def __init__(self, *args, **kwargs):
        super(CategorySpider, self).__init__(*args, **kwargs)

    def start_requests(self):
        self.data_path = os.path.join('.', self.settings["DATA_PATH"])
        try:
            os.makedirs(self.data_path)
        except OSError, err:
            if err.errno == errno.EEXIST and os.path.isdir(self.data_path):
                pass
            else:
                raise
        yield scrapy.Request(self.settings['START_PAGE'], self.parse)

    def parse(self, response):
        try:
            self.redis_client = redis.Redis(host=self.settings["REDIS_SERVER_HOST"], port=self.settings["REDIS_SERVER_PORT"], db=0)
        except redis.RedisError, err:
            raise err

        self.redis_client.flushdb()

        url_re = re.compile('(http[s]?://[^/]+)/.*')
        url = response.url
        base_url = url_re.match(url).groups()[0]
        categories_dic = []
        for sel in response.xpath('//a[contains(@href, "taglist")]'):
            category_item = CategoryItem()
            category_item['name'] = sel.xpath('text()').extract()[0].encode('utf-8', 'ignore')
            category_item['url'] = response.urljoin(sel.xpath('@href').extract()[0])
            categories_dic.append(category_item.to_dic())
            for i in range(0, 750 + 1, 10):
                list_url = category_item['url'] + '&offset={0}'.format(i)
                request = scrapy.Request(list_url, callback = self.parse_category_list)
                request.meta['category_item'] = category_item
                yield request

    def parse_category_list(self, response):
        category_item = response.meta['category_item']
        for sel in response.xpath('//a[contains(@href, "/view/")]'):
            url = response.urljoin(sel.xpath('@href').extract()[0].split('?')[0])
            request = scrapy.Request(url, callback = self.parse_person)
            yield request


    def parse_person(self, response):
        url = response.url.split('?')[0]

        '''
        check if scanned:
        'http://baike.baidu.com/subview/3996/3996.htm'
        will get '3996' as an unique id(uid)
        if got nothing, use the url as uid.
        ''' 
        r = re.compile(r'\D*(\d+)\D*') 
        uid = r.findall(url)
        if len(uid) > 0:
          uid = uid[-1]
        else:
          uid = url

        scann_cnt = self.redis_client.get(uid)
        if scann_cnt != None:
            scann_cnt = int(scann_cnt) + 1
            self.redis_client.set(uid, scann_cnt)
            return

        self.redis_client.set(uid, 1)

        # the 'keywords' meta must contains '人物'
        kwlist = response.xpath('//meta[@name="keywords"]/@content').extract()
        if len(kwlist) == 0:
          return

        keywords = kwlist[0].encode('utf-8', 'ignore')
        if keywords.find('人物') == -1:
            return

        description = response.xpath('//meta[@name="description"]/@content').extract()[0].encode('utf-8', 'ignore')

        person_item = PersonItem()
        person_item['name'] = response.xpath('//h1/text()').extract()[0].encode('utf-8', 'ignore')
        person_item['url'] = url
        person_item['keywords'] = keywords
        person_item['description'] = description

        # get person tags (人物标签)
        person_tags = []
        for sel in response.xpath('//span[@class="taglist"]'):
          tag = sel.xpath('text()').extract()[0].replace('\n', '').encode('utf-8', 'ignore')
          person_tags.append(tag)
        person_item['tags'] = ' '.join(person_tags)

        # for the data pipeline
        yield person_item

        # crawling image gallery (图册)
        image_gallery_urls = response.xpath('//div[@class="summary-pic"]/a/@href').extract()
        if len(image_gallery_urls) > 0:
            image_gallery_url = response.urljoin(image_gallery_urls[0].split('?')[0])
            request = scrapy.Request(image_gallery_url, callback = self.parse_image_gallery)
            request.meta["person_info"] = person_item
            yield request

        # follow link that which url contains |view|(view/subview)
        for sel in response.xpath('//a[contains(@href, "view")]'):
            url = response.urljoin(sel.xpath('@href').extract()[0].split('?')[0])
            request = scrapy.Request(url, callback = self.parse_person)
            yield request

    def parse_image_gallery(self, response):
        person_info = response.meta['person_info']
        self.logger.info('Found image gallery from : %s', response.url)
        album_info_str = "{%s}" % response.xpath('//body/script/text()').re(r'albums:.*lemmaId:')[0].replace('albums', '"albums"').replace(',lemmaId:','')
        album_info_dic = None
        try:
            album_info_dic = json.loads(album_info_str)
            album_info_dic = album_info_dic['albums']
        except Exception, e:
            self.logger.error('json parse album info error. url: %s, err: %r', response.url, e)
            return
        if isinstance(album_info_dic, list):
            album_info_dic = album_info_dic[0]

        pictures = []
        try:
            pictures.append(album_info_dic['pictures'])
        except KeyError, e:
            try:
                for k,v in album_info_dic.items():
                  if v.has_key('pictures'):
                    pictures.append(v['pictures'])
            except Exception, e:
                self.logger.error('parse pictures info error. url: %s, err: %r', 
                      response.url, e)
                return
        except Exception, e:
            self.logger.error('parse pictures info error. url: %s, err: %r', 
                response.url, e)
            return

        for p in pictures:
            for picture_info in p:
              image_item = ImageItem()
              try:
                prefer_index = str(picture_info['type']['oriWithWater'])
                image = picture_info['sizes'][prefer_index]
                image_item['src'] = picture_info['src']
                description = "desc:{%s}, owner: {%s}" % (picture_info['desc'].encode('utf8', 'ignore'), picture_info['owner'].encode('utf8', 'ignore'))
                image_item['desc'] = description
                image_item['url'] = image['url']
                image_item['width'] = image['width']
                image_item['height'] = image['height']
                image_item['size'] = image['size']
                image_item['person_name'] = person_info['name']
                image_item['person_url'] = person_info['url']
              except Exception, e:
                self.logger.error('parse pictures info error. picture: %r, err: %r \n TRACE: %s', 
                  picture_info, e, traceback.format_exc())
                continue

              src = image_item['src']
              scann_cnt = self.redis_client.get(src)
              if scann_cnt != None:
                scann_cnt = int(scann_cnt) + 1
                self.redis_client.set(src, scann_cnt)
                continue 

              self.redis_client.set(src, 1)

              request = scrapy.Request(image_item['url'], callback = self.download_image)
              request.meta["image_info"] = image_item
              yield request

    def download_image(self, response):
        image_info = response.meta['image_info']
        file_name = response.url.split('/')[-1]
        path_part = os.path.join(file_name[0:2], file_name[2:4])
        image_dir = os.path.join('.', self.data_path, 'images', path_part)

        image_info['file_name'] = file_name
        image_info['file_path'] = os.path.join(path_part, file_name)

        try:
            os.makedirs(image_dir)
        except OSError, err:
            if err.errno == errno.EEXIST and os.path.isdir(image_dir):
                pass
            else:
                raise

        try:
          with open(os.path.join(image_dir, file_name), 'wb') as f:
            f.write(response.body)
        except Exception, err:
          this.logger.error("image file write error. file: %s, err: %r", file_name, err)

        yield image_info

class BDBKSpider(CategorySpider):
    name = 'bdbk'
