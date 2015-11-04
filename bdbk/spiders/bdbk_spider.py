# -*- coding: utf-8 -*-

import os
import errno 
import re
import json
import traceback

import scrapy
import redis # https://github.com/andymccurdy/redis-py
import pymongo

from bdbk.items import PersonItem
from bdbk.items import AlbumItem
from bdbk.items import ImageItem
from bdbk.items import ErrorInfoItem

from bdbk.utils import mkdir
from bdbk.utils import now_string

# generic settings
BAIDU_DOMAIN = ['baidu.com']

def getYN(prompt):
    str = raw_input(prompt)
    str = str.lower()
    if str != 'y' and str != 'n':
      return getYN(prompt)
    return str == 'y'

class CategorySpider(scrapy.Spider):
    name = 'bdbk.category'
    allowed_domains = BAIDU_DOMAIN

    def __init__(self, url=None, *args, **kwargs):
        self.start_page = url
        self.rerun = False
        self.follow_link= True
        self.flushRedis = False
        super(CategorySpider, self).__init__(*args, **kwargs)

    def start_requests(self):
        if not getYN("ready for run?(y/n): "):
            return

        # create dir
        self.data_path = os.path.join('.', self.settings["DATA_PATH"])
        self.ignore_tags = self.settings['IGNORE_TAGS']

        self.rerun = self.settings["RERUN"]
        self.follow_link= self.settings["FOLLOW_LINK"]
        self.flushRedis = self.settings["FLUSH_REDIS"]

        try:
            mkdir(self.data_path)
        except OSError, err:
            raise
        
        # redis client
        try:
            redis_server_host = self.settings["REDIS_SERVER_HOST"]
            redis_server_port = int(self.settings["REDIS_SERVER_PORT"])
            redis_server_db = int(self.settings['REDIS_SERVER_DB'])
            redis_server_db_person = int(self.settings['REDIS_SERVER_DB_PERSON'])
            self.redis_client = redis.Redis(host=redis_server_host, port=redis_server_port, db=redis_server_db)
            self.redis_client_person = redis.Redis(host=redis_server_host, port=redis_server_port, db=redis_server_db_person)
            if self.flushRedis:
                self.redis_client.flushdb()
                self.redis_client_person.flushdb()
            self.logger.info('Redis server flushed? [{0}]. server addr: {1}:{2}/{3}'.format(self.flushRedis, redis_server_host,redis_server_port, redis_server_db))
        except redis.RedisError, err:
            raise err

        if self.rerun:
            self.logger.info("Start [RERUN] mode")
            self.prepare_rerun()

        if self.start_page == None:
            self.start_page = self.settings['START_PAGE']
            request = scrapy.Request(self.start_page, self.parse)
        else:
            request = scrapy.Request(self.start_page, self.parse_person)

        self.logger.info("Start crawling url: %s" %self.start_page)

        yield request

    def prepare_rerun(self):
        mongodb_url = self.settings.get('MONGODB_URL'),
        mongodb_dbname = self.settings.get('MONGODB_DB', 'bdbk')
        mongodb_client = pymongo.MongoClient(mongodb_url)
        mongodb_db = mongodb_client[mongodb_dbname]
        person_col = mongodb_db['person_info']
        self.redis_client_person.flushdb()
        for person in person_col.find():
            self.redis_client_person.set(person['url'], 1)
            self.logger.warning("Set person visited. name: {0}, url: {1}".format(person['name'].encode('utf8', 'ignore'), person['url']))
        mongodb_client.close()

    def parse(self, response):
        for sel in response.xpath('//a[contains(@href, "taglist")]'):
            url = response.urljoin(sel.xpath('@href').extract()[0])
            for i in range(0, 750 + 1, 10):
                list_url = url + '&offset={0}'.format(i)
                request = scrapy.Request(list_url, callback = self.parse_category_list)
                yield request
        if response.url.find('taglist') > 0:
            for i in range(0, 750 + 1, 10):
                list_url = url.split('&')[0] + '&offset={0}'.format(i)
                self.logger.info("goto url: {0}.".format(list_url))
                request = scrapy.Request(list_url, callback = self.parse_category_list)
                yield request

    def parse_category_list(self, response):
        for sel in response.xpath('//a[contains(@href, "/view/")]'):
            url = response.urljoin(sel.xpath('@href').extract()[0].split('?')[0])
            request = scrapy.Request(url, callback = self.parse_person)
            yield request

    def check_visited(self, key):
        v = self.redis_client_person.get(key)
        if v != None:
            self.logger.warning("{0} is visited".format(key))
            return True
        return False

    def parse_person(self, response):
        url = response.url.split('?')[0]
        if self.check_visited(url):
          return

        kwlist = response.xpath('//meta[@name="keywords"]/@content').extract()
        if len(kwlist) == 0:
          self.redis_client_person.set(url, 1)
          return

        keywords = kwlist[0].encode('utf-8', 'ignore')
        '''
        # the 'keywords' meta must contains '人物'
        if keywords.find('人物') == -1:
            self.redis_client_person.set(url, 1)
            return
        '''

        description = response.xpath('//meta[@name="description"]/@content').extract()[0].encode('utf-8', 'ignore')
        page_title = response.xpath('//h1/text()').extract()[0].encode('utf-8', 'ignore')

        # get person tags (人物标签)
        person_tags = list()
        categories = dict()
        is_person = False
        for sel in response.xpath('//span[@class="taglist"]'):
            tag = sel.xpath('text()').extract()[0]
            tag = re.sub(r'[\r\n]*', '', tag).encode('utf-8', 'ignore')
            if len(tag) == 0:
                continue
            if tag in self.ignore_tags:
                message = 'In ignore list. name: {0}, tag: {1}'.format(page_title, tag)
                ei_item = ErrorInfoItem()
                ei_item['time'] = now_string()
                ei_item['url'] = url
                ei_item['error_level'] = "W"
                ei_item['error_type'] = 'W1'
                ei_item['description'] = message
                yield ei_item
                self.logger.warning(message)
                self.redis_client_person.set(url, 1)
                return
            if tag.find('人物') != -1:
                is_person = True
            person_tags.append(tag)
            # save to redis
            category_cnt = self.redis_client.get(tag)
            if  str(category_cnt) == 'None':
                category_cnt = 1
            else:
                category_cnt = int(category_cnt) + 1
            self.redis_client.set(tag, category_cnt)

            categories[tag] = category_cnt

        # if tags do not contains |人物|, just follow link
        if is_person == False and self.follow_link == True:
            self.redis_client_person.set(url, 1)
            # follow link that which url contains |view|(view/subview)
            for sel in response.xpath('//a[contains(@href, "view")]'):
                url = response.urljoin(sel.xpath('@href').extract()[0].split('?')[0])
                if self.check_visited(url):
                    return
                request = scrapy.Request(url, callback = self.parse_person)
                yield request
            return

        person_item = PersonItem()
        person_item['name'] = page_title
        person_item['url'] = url
        person_item['description'] = description
        person_item['tags'] = person_tags
        person_item['keywords'] = keywords 

        summary_pic = response.xpath('//div[@class="summary-pic"]/a/img/@src').extract()
        if len(summary_pic) > 0:
            summary_pic = summary_pic[0].split('/')[-1].split('.')[0]
        else:
            summary_pic = ''
        person_item['summary_pic'] = summary_pic

        # for the data pipeline
        yield person_item
        yield categories

        # crawling image gallery (图册)
        # album list
        album_list = response.xpath('//script/text()').re(r'AlbumList\({.*[\n\t]*.*[\n\t]*.*[\n\t]*.*')
        albums = list()
        if len(album_list) > 0:
            album_list = album_list[0]
            album_list = re.sub(r'[\r\n\t]*', '', album_list)
            album_lemma_id = re.findall(r'lemmaId:"([\d]+)"', album_list)[0]
            album_sublemma_id = re.findall(r'subLemmaId:"([\d]+)"', album_list)[0]
            album_data_json = re.sub(r'AlbumList.*data:', '', album_list)
            try:
                album_data_dict = json.loads(album_data_json)
                i = 0
                for d in album_data_dict:
                    if isinstance(album_data_dict, list):
                        cover_pic = d["coverpic"]
                        album_desc= d["desc"]
                        album_total= d["total"]
                        album_url = '/picture/{0}/{1}/{2}/{3}'.format(album_lemma_id, album_sublemma_id, i, cover_pic)
                        i += 1
                    else:
                        cover_pic = album_data_dict[d]["coverpic"]
                        album_desc= album_data_dict[d]["desc"]
                        album_total= album_data_dict[d]["total"]
                        album_url = '/picture/{0}/{1}/{2}/{3}'.format(album_lemma_id, album_sublemma_id, d, cover_pic)
                    album_url = response.urljoin(album_url)

                    # build album_item
                    album_item = AlbumItem()
                    album_item['url'] = album_url
                    album_item['description'] = album_desc.encode('utf8', 'ignore')
                    album_item['total'] = album_total
                    album_item['cover_pic'] = cover_pic
                    album_item['person_name'] = person_item['name']
                    album_item['person_url'] = person_item['url']
                    albums.append(album_item)
            except Exception, e:
                self.logger.error('json parse album list info error. url: %s, err: %r', response.url, e)

        for album_item in albums:
            album_url = album_item['url']
            yield album_item
            self.logger.info('Found album for person %s. desc: %s, url: %s', album_item['person_name'], album_item['description'], album_url)
            request = scrapy.Request(album_url, callback = self.parse_image_gallery)
            request.meta["person_info"] = person_item
            request.meta["album_info"] = album_item
            request.meta["from_url"] = album_url
            yield request

        # if albums is 0 length, follow link contains |picture|
        if len(albums) == 0:
            for url in response.xpath('//a[contains(@href, "/picture/")]/@href').extract():
                image_gallery_url = response.urljoin(url.split('?')[0])
                request = scrapy.Request(image_gallery_url, callback = self.parse_image_gallery)
                request.meta["person_info"] = person_item
                request.meta["album_info"] = None
                request.meta["from_url"] = image_gallery_url
                yield request

        # set visited
        self.redis_client_person.set(person_item['url'], 1)

        if self.follow_link == False:
            return

        # follow link that which url contains |view|(view/subview)
        for sel in response.xpath('//a[contains(@href, "view")]'):
            url = response.urljoin(sel.xpath('@href').extract()[0].split('?')[0])
            if self.check_visited(url):
                return
            request = scrapy.Request(url, callback = self.parse_person)
            yield request

    def parse_image_gallery(self, response):
        person_info = response.meta['person_info']
        album_info = response.meta['album_info']
        self.logger.info('Found Photo Gallery from : %s', response.url)
        album_info_str  = None
        try:
            r = re.compile('albums:.*,[\r\n\s]*lemmaId:')
            for s in response.xpath('//script/text()').extract():
                match = re.search(r, s)
                if match:
                    album_info_str =  match.group()
                    album_info_str = re.sub(r',[\r\n\s]*lemmaId:', '', album_info_str)
                    album_info_str = "{%s}" % album_info_str.replace('albums', '"albums"')
                    break
        except Exception, e:
            self.logger.error('get album info json error. url: %s, err: %r', response.url, e)
            return
        if album_info_str == None:
            message = 'Album not found. person_name: {0}, person_url: {1}'.format(person_info['name'], person_info['url'])
            ei_item = ErrorInfoItem()
            ei_item['time'] = now_string()
            ei_item['url'] = response.url
            ei_item['error_level'] = "E"
            ei_item['error_type'] = "E1"
            ei_item['description'] = message
            yield ei_item
            self.logger.warning('{%s}. url: %s', message, response.url)
            return

        album_info_dic = None
        try:
            album_info_dic = json.loads(album_info_str)
            album_info_dic = album_info_dic['albums']
        except Exception, e:
            message = 'json.loads album info error. url: {0}, json: {1}, err: {2}'.format(response.url, album_info_str, e)
            ei_item = ErrorInfoItem()
            ei_item['time'] = now_string()
            ei_item['url'] = response.url
            ei_item['error_level'] = "E"
            ei_item['error_type'] = "E2"
            ei_item['description'] = message
            yield ei_item
            self.logger.error(message)
            return
        if isinstance(album_info_dic, list):
            album_info_dic = album_info_dic[0]

        pictures = []
        cover_pics = []
        descriptions = []
        try:
            pictures.append(album_info_dic['pictures'])
            if album_info == None:
                cover_pics.append(album_info_dic['coverpic'])
                descriptions.append(album_info_dic['desc'])
        except KeyError, e:
            try:
                for k,v in album_info_dic.items():
                    if v.has_key('pictures'):
                        pictures.append(v['pictures'])
                        if album_info == None:
                            cover_pics.append(v['coverpic'])
                            descriptions.append(v['desc'])
            except Exception, e:
                message = 'parse pictures info error. url: {0}, err: {1}'.format(response.url, e)
                ei_item = ErrorInfoItem()
                ei_item['time'] = now_string()
                ei_item['url'] = response.url
                ei_item['error_level'] = "E"
                ei_item['error_type'] = "E3"
                ei_item['description'] = message
                yield ei_item
                self.logger.error(message)
                return
        except Exception, e:
            message = 'parse pictures info error. url: {0}, err: {1}'.format(response.url, e)
            ei_item = ErrorInfoItem()
            ei_item['time'] = now_string()
            ei_item['url'] = response.url
            ei_item['error_level'] = "E"
            ei_item['error_type'] = "E3"
            ei_item['description'] = message
            yield ei_item
            self.logger.error(message)
            return

        if album_info == None and len(cover_pics) == 1:
            album_info = AlbumItem()
            album_info['url'] = response.url
            album_info['description'] = descriptions[0]
            album_info['total'] = len(pictures)
            album_info['cover_pic'] = cover_pics[0]
            album_info['person_name'] = person_info['name']
            album_info['person_url'] = person_info['url']
            yield album_info

        for p in pictures:
            for picture_info in p:
              image_item = ImageItem()
              image_item['album_url'] = response.meta["from_url"]
              try:
                prefer_index = str(picture_info['type']['oriWithWater'])
                image = picture_info['sizes'][prefer_index]
                src = picture_info['src']
                # src
                image_item['src'] = src
                # is_cover
                image_item['is_cover'] = (src == album_info['cover_pic'])
                # desc
                description = picture_info['desc'].encode('utf8', 'ignore')
                image_item['desc'] = description
                image_item['url'] = image['url']
                image_item['width'] = image['width']
                image_item['height'] = image['height']
                image_item['size'] = image['size']
                image_item['person_name'] = person_info['name']
                image_item['person_url'] = person_info['url']
              except Exception, e:
                message = 'parse pictures info error. url: {0}, err: {1} picture: {2}.'.format(response.url, e, picture_info)
                ei_item = ErrorInfoItem()
                ei_item['time'] = now_string()
                ei_item['url'] = response.url
                ei_item['error_level'] = "E"
                ei_item['error_type'] = "E4"
                ei_item['description'] = message
                yield ei_item
                self.logger.error(message)
                continue

              # set |src| to redis as crawled mark
              src = image_item['src']
              scann_cnt = self.redis_client.get(src)
              if scann_cnt != None:
                  continue 
              self.redis_client.set(src, 1)

              request = scrapy.Request(image_item['url'], callback = self.download_image)
              request.meta["image_info"] = image_item
              yield request

    def download_image(self, response):
        image_info = response.meta['image_info']
        # mime 
        mime = response.headers['Content-Type']
        image_info['mime'] = mime

        # file_name
        file_name = response.url.split('/')[-1]
        if mime != 'image/jpeg':
            file_name = '{0}.{1}'.format(file_name.split('.')[0], mime.split('/')[-1])
        path_part = os.path.join(file_name[0:2], file_name[16:18])
        image_dir = os.path.join(self.data_path, 'images', path_part)
        file_path = os.path.join(image_dir, file_name)

        # check file if exist
        if os.path.isfile(file_path):
            self.logger.warning("download_image() file exist. image_info: %r" , image_info)
            return

        image_info['file_name'] = file_name
        image_info['file_path'] = os.path.join(path_part, file_name)

        try:
            mkdir(image_dir)
        except OSError, err:
            raise

        try:
          with open(file_path, 'wb') as f:
            f.write(response.body)
        except Exception, err:
          self.logger.error("image file write error. file: %s, err: %r", file_name, err)
          raise

        self.logger.info('Image saved to: %s', file_path)

        yield image_info

class BDBKSpider(CategorySpider):
    name = 'bdbk'
