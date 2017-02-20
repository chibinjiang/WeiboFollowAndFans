#coding=utf-8
import re
import os
import json
import requests
from bs4 import BeautifulSoup as bs
from datetime import datetime as dt
from zc_spider.weibo_spider import WeiboSpider
from zc_spider.weibo_utils import catch_parse_error
from zc_spider.weibo_config import RELATION_JOBS_CACHE
# init : http://m.weibo.cn/p/second?containerid=1005051652811601_-_FOLLOWERS
# urls : http://m.weibo.cn/container/getSecond?containerid=100505{user_id}_-_FANS&page=3
# http://m.weibo.cn/container/getSecond?containerid=100505{user_id}_-_FOLLOWERS

class WeiboRelationSpider(WeiboSpider):
    """
    Parse follow json:  {
        "ok": 1,  "msg": "", 
        "title": "关注",  "card_type_name": "关注", 
        "count": 203,  "maxPage": 21, 
        "type": "pageuserlist",  "cards": [
            "card_type": 10, 
            "user": {}, 
            "scheme": "http://m.weibo.cn/u/2060865431", 
            "desc1": "通过精致的产品与创新的科技创造美妙的生活体验。", 
            "desc2": ""
        ], 
        "cardlistInfo": {}
    }
    Parse fans json: {
        "ok": 1, 
        "msg": "", 
        "title": "粉丝", 
        "page_title": "粉丝", 
        "card_type_name": "粉丝", 
        "count": 200, 
        "maxPage": 20, 
        "type": "pageuserlist", 
        "cards": [
            "card_type": 10, 
            "user": {}, 
            "scheme": "http://m.weibo.cn/u/6096925740", 
            "desc1": "", 
            "desc2": ""
        ], 
        "cardlistInfo": {}
    }

    """
    def __init__(self, usercard, user_url, start_url, account, password, timeout=10, delay=1, proxy={}):
        WeiboSpider.__init__(self, start_url, account, password, timeout=timeout, delay=delay, proxy=proxy)
        self.uid = usercard
        self.user_url = user_url

    def gen_html_source_d(self, curl):
        html = os.popen(curl + ' --silent').read()
        self.page = html
        return True 

    @catch_parse_error((AttributeError, Exception))
    def get_user_follow_list(self, rconn):
        """
        Start url : http://m.weibo.cn/container/getSecond?containerid=100505{uid}_-_FOLLOWERS
        Other urls : start_url + "&page={num}"
        """
        data = json.loads(self.page)
        user_list = []
        if data['ok'] != 1:
            print "Not OK -> message: ", data['msg']
            return user_list
        if "&page=" not in self.url:
            max_page = int(data['maxPage'])
            if max_page > 1:
                for num in range(1, max_page):
                    next_job = "%s||%s||%s" % (self.uid, 
                        self.user_url, self.url + "&page=%d" % num)
                    rconn.rpush(RELATION_JOBS_CACHE, next_job)

        for card in data['cards']:
            # import ipdb; ipdb.set_trace()
            info = {}
            user = card['user']
            info['user_url'] = self.user_url
            info['uid'] = user['id']
            info['nickname'] = user['screen_name']
            # three number 
            info['blog_num'] = user['statuses_count']
            info['follows'] = user['follow_count']
            info['fans'] = user['followers_count']
            if user.get('verified_type_ext') == 3:
                info['type'] = 'W_icon icon_approve_co'
            elif user.get('verified_type_ext') == 1 and user.get('verified_type') in [11, 12]:
                info['type'] = 'W_icon icon_approve_gold'
            elif user.get('verified_type_ext') == 0:
                info['type'] = 'icon_approve'
            info['create_date'] = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            user_list.append(info)
        return user_list
