#coding=utf-8
import re
import os
import json
import requests
from bs4 import BeautifulSoup as bs
from datetime import datetime as dt
from zc_spider.weibo_spider import WeiboSpider
from zc_spider.weibo_utils import catch_parse_error
from zc_spider.weibo_config import (
    MANUAL_COOKIES, WEIBO_ACCOUNT_PASSWD, 
    LOCAL_REDIS, MAD_MAN_URLS, MAD_MAN_INFO
)
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
    def __init__(self, start_url, account, password, timeout=10, delay=1, proxy={}):
        WeiboSpider.__init__(self, start_url, account, password, timeout=timeout, delay=delay, proxy=proxy)
        # self.uid = usercard

    def gen_html_source(self, curl):
        html = os.popen(curl + ' --silent').read()
        self.page = html
        # import ipdb; ipdb.set_trace()
        return True 

    @catch_parse_error((AttributeError, Exception))
    def parse_relationship(self, rconn):
        """
        Start url : http://m.weibo.cn/container/getSecond?containerid=100505{uid}_-_FOLLOWERS
        Other urls : start_url + "&page={page}"
        """
        data = json.loads(self.page)
        user_list = []
        if data['ok'] != 1:
            print "Not OK -> message: ", data['msg']
            return user_list
        card_rexp = re.search(r'100505(\d+)_-_F', data['cardlistInfo']['containerid'])
        if card_rexp:
            self.uid = card_rexp.group(1)
        else:
            return user_list
        page_regex = re.search(r'&page=(\d+)', self.url)
        current_page = page_regex.group(1) if page_regex else 20
        if data['maxPage'] >= current_page:
            is_last = True
        for temp in data['cards']:
            # import ipdb; ipdb.set_trace()
            user = temp['user']
            info = {}
            info['uid'] = user['id']
            info['url'] = temp['scheme']
            info['date'] = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            if data['title'] == '粉丝':
                info['ship'] = 'fans'
                info['left'], info['right'] = user['id'], self.uid
            elif data['title'] == '关注':
                info['ship'] = 'follow'
                info['left'], info['right'] = self.uid, user['id']
            info['name'] = user['screen_name']
            info['gender'] = user['gender']
            # three number 
            info['blog_num'] = user['statuses_count']
            info['follows'] = user['follow_count']
            info['fans'] = user['followers_count']
            info['image'] = '' if 'default' in user['profile_image_url'] else user['profile_image_url']
            info['intro'] = user.get('verified_reason', '')
            # verified
            info['verified'] = user['verified']
            info['verified_type'] = user['verified_type']
            # rank 
            info["mbtype"] = user['mbtype']
            info["rank"] = user['urank']   # 9 is red V; 36 is yellow
            info["mbrank"] =  user['mbrank']
            desc1 = '' if user['desc1'] == 'null' or not user['desc1'] else user['desc1']
            desc2 = '' if user['desc2'] == 'null' or not user['desc2'] else user['desc2']
            if len(desc1) > 0 and len(desc2) > 0:
                info['desc'] = desc1 + ' . ' + desc2
            elif len(desc1) > 0:
                info['desc'] = desc1
            else:
                info['desc'] = ''
            for k,v in info.items():
                print k,v
            user_list.append(info)
        return is_last, user_list
