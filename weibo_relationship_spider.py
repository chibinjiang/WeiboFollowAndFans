#coding=utf-8
import re
import json
import requests
from bs4 import BeautifulSoup as bs
from datetime import datetime as dt
from template.weibo_spider import WeiboSpider
from template.weibo_utils import catch_parse_error

class WeiboFollowSpider(WeiboSpider):
    def __init__(self, start_url, account, password, timeout=10, delay=1, proxy={}):
        WeiboSpider.__init__(self, start_url, account, password, timeout=timeout, delay=delay, proxy=proxy)
        self.pageno = 1

    @catch_parse_error((AttributeError, Exception))
    def parse_relationship(self):
        """ 打开url爬取里面的个人ID """
        selector = Selector(response)
        if "/follow" in response.url:
            ID = re.findall('(\d+)/follow', response.url)[0]
            flag = True
        else:
            ID = re.findall('(\d+)/fans', response.url)[0]
            flag = False
        urls = selector.xpath('//a[text()="关注他" or text()="关注她"]/@href'.decode('utf')).extract()
        uids = re.findall('uid=(\d+)', ";".join(urls), re.S)
        for uid in uids:
            relationshipsItem = RelationshipsItem()
            relationshipsItem["Host1"] = ID if flag else uid
            relationshipsItem["Host2"] = uid if flag else ID
            yield relationshipsItem
            yield Request(url="http://weibo.cn/%s/info" % uid, callback=self.parse_information)

        next_url = selector.xpath('//a[text()="下页"]/@href'.decode('utf8')).extract()
        if next_url:
            yield Request(url=self.host + next_url[0], callback=self.parse_relationship, dont_filter=True)
