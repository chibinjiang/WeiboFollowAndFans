#coding=utf-8
import os
import re
import json
import time
import redis
import base64
import requests
import traceback
from lxml import etree
from datetime import datetime as dt
from bs4 import BeautifulSoup as bs
from tornado import httpclient, gen, ioloop, queues
from requests.exceptions import (
    ProxyError,
    Timeout,
    ConnectionError,
    ConnectTimeout,
)
from template.weibo_utils import (
    extract_cookie_from_curl,
    extract_chinese_info,
    extract_post_data_from_curl,
    gen_abuyun_proxy, 
    handle_proxy_error, 
    handle_sleep, 
    catch_network_error, 
    retry
)
from weibo_config import *

weibo_ranks = ['icon_member', 'icon_club', 'icon_female', 'icon_vlady', 'icon_pf_male', 'W_icon_vipstyle']
exc_list = (IndexError, ProxyError, Timeout, ConnectTimeout, ConnectionError, Exception)


class AsySpider(object):  
    """A simple class of asynchronous spider."""
    def __init__(self, urls, concurrency=10, results=None, **kwargs):
        urls.reverse()
        self.urls = urls
        self.concurrency = concurrency
        self._q = queues.Queue()
        self._fetching = set()
        self._fetched = set()
        if results is None:
            self.results = []

    def fetch(self, url, **kwargs):
        fetch = getattr(httpclient.AsyncHTTPClient(), 'fetch')
        return fetch(url, raise_error=False, **kwargs)

    def handle_html(self, url, html):
        """handle html page"""
        # print(url)
        pass

    def handle_response(self, url, response):
        """inherit and rewrite this method if necessary"""
        if response.code == 200:
            self.handle_html(url, response.body)

        elif response.code == 599:    # retry
            self._fetching.remove(url)
            self._q.put(url)

    @gen.coroutine
    def get_page(self, url):
        try:
            response = yield self.fetch(url)
            #print('######fetched %s' % url)
        except Exception as e:
            print('Exception: %s %s' % (e, url))
            raise gen.Return(e)
        raise gen.Return(response)

    @gen.coroutine
    def _run(self):
        @gen.coroutine
        def fetch_url():
            current_url = yield self._q.get()
            try:
                if current_url in self._fetching:
                    return

                #print('fetching****** %s' % current_url)
                self._fetching.add(current_url)

                response = yield self.get_page(current_url)
                self.handle_response(current_url, response)    # handle reponse

                self._fetched.add(current_url)

                for i in range(self.concurrency):
                    if self.urls:
                        yield self._q.put(self.urls.pop())

            finally:
                self._q.task_done()

        @gen.coroutine
        def worker():
            while True:
                yield fetch_url()

        self._q.put(self.urls.pop())    # add first url

        # Start workers, then wait for the work queue to be empty.
        for _ in range(self.concurrency):
            worker()

        yield self._q.join(timeout=timedelta(seconds=300000))
        try:
            assert self._fetching == self._fetched
        except AssertionError:
            print(self._fetching-self._fetched)
            print(self._fetched-self._fetching)

    def run(self):
        io_loop = ioloop.IOLoop.current()
        io_loop.run_sync(self._run)


class AsyncSpider(AsySpider):
    def handle_html():
        pass


class Spider(object):
    def __init__(self, start_url, curl='', timeout=10, delay=1, proxy={}):
        self.url = start_url
        self.curl = curl
        self.cookie = {}  # self.extract_cookie_from_curl()
        self.post = {}  # self.extract_post_data_from_curl()
        self.headers = {}
        self.timeout = timeout
        self.proxy = proxy
        self.delay = delay
        self.page = ''

    def use_abuyun_proxy(self):
        self.proxy = gen_abuyun_proxy()
    
    def add_request_header(self):
        self.headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            # 'Referer': 'http://weibo.com/sorry?pagenotfound&',
            'Accept-Encoding': 'gzip, deflate, sdch',
            'Accept-Language': 'zh-CN,zh;q=0.8',
        }

    def extract_cookie_from_curl(self):
        cookie_dict = {}
        tokens = self.curl.split("'")
        if not tokens:
            # curl is empty string
            return cookie_dict
        try:
            for i in range(0, len(tokens)-1, 2):
                # if tokens[i].startswith("curl"):
                #     url = tokens[i+1]
                if "-H" in tokens[i]:
                    attr, value = tokens[i+1].split(": ")  # be careful space
                    if 'Cookie' in attr:
                        cookie_dict[attr] = value
        except Exception as e:
            print "!"*20, "Parsed cURL Failed"
            traceback.print_exc()
        return cookie_dict

    def extract_post_data_from_curl(self):
        """
        Given curl that was cpoied from Chrome, no matter baidu or sogou, 
        parse it and then get url and the data you will post/get with requests
        """
        post_data = {}
        tokens = self.curl.split("'")
        if not tokens:
            # curl is empty string
            return cookie_dict
        try:
            for i in range(0, len(tokens)-1, 2):
                if tokens[i].startswith("curl"):
                    url = tokens[i+1]
                elif "-H" in tokens[i]:
                    attr, value = tokens[i+1].split(": ")  # be careful space
                    post_data[attr] = value
        except Exception as e:
            print "!"*20, "Parsed cURL Failed"
            traceback.print_exc()
        return post_data

    # @catch_network_error(exc_list)
    @retry(exc_list, tries=4, delay=2, backoff=2)
    def gen_html_source(self, method='python'):
        """
        Separate get page and parse page
        """
        if not self.cookie:
            return None
        request_args = {'timeout': self.timeout,
            'cookies': self.cookie, 'proxies': self.proxy,
        }
        if method == 'python':
            # proxy = gen_abuyun_proxy()
            source_code = requests.get(self.url, **request_args).text
        else:
            if '--silent' not in self.curl:
                self.curl += '--silent'
            source_code = os.popen(self.curl).read()
        # parser = bs(source_code, 'html.parser')
        elminate_white = re.sub(r'\\r|\\t|\\n', '', source_code)
        elminate_quote = re.sub(r'\\"', '"', elminate_white)
        elminate_slash = re.sub(r'\\/', '/', elminate_quote)
        handle_sleep(self.delay)
        return elminate_slash.encode('utf8')


class WeiboSpider(Spider):
    def __init__(self, start_url, account, password, timeout=10, delay=1, proxy={}):
        Spider.__init__(self, start_url, timeout=timeout, delay=delay, proxy=proxy)
        self.account = account
        self.password = password
        self.is_abnormal = False
        print 'Parsing %s using Account %s' % (self.url, self.account)

    def read_cookie(self, rconn):
        auth = '%s--%s' % (self.account, self.password)
        if auth in rconn.hkeys(ACTIVATED_COOKIE):
            self.cookie = json.loads(rconn.hget(ACTIVATED_COOKIE, auth))
            return True
        else:
            status = self.gen_cookie(rconn)
            if status:
                return True
        # time.sleep(2)
        return False

    def use_cookie_from_curl(self, curl):
        # curl = "curl 'http://weibo.com/1791434577/info' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cache-Control: max-age=0' -H 'Cookie: _T_WM=091a7f8021abc37974cfbb8fc47e6ba3; ALF=1484106233; SUB=_2A251SmypDeTxGeNG71EX8ybKwj6IHXVWtXThrDV8PUJbkNAKLUb_kW1_1M3WyDkt9alEMQg5hlJuRoq9kg..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5HA7SsRPVzLQ_q6ucc2n_c5JpX5oz75NHD95Qf1hB0SoeRSo.EWs4Dqcj6i--ciK.Ni-27i--ciKnRiK.pi--Xi-z4iKyFi--4iK.Ri-z0i--ciK.RiKy8i--fi-z7iK.pi--fi-z4i-zX; TC-Ugrow-G0=968b70b7bcdc28ac97c8130dd353b55e; wvr=6; TC-V5-G0=7975b0b5ccf92b43930889e90d938495; TC-Page-G0=4c4b51307dd4a2e262171871fe64f295' -H 'Connection: keep-alive' --compressed"
        self.cookie = extract_cookie_from_curl(curl)

    @retry(exc_list, tries=3, delay=3, backoff=2)
    def gen_cookie(self, rconn):
        """ 
        获取一个账号的Cookie
        Cookie is str
        """
        auth = '%s--%s' % (self.account, self.password)
        # loginURL = "https://login.sina.com.cn/sso/login.php?client=ssologin.js(v1.4.15)"
        loginURL = "https://login.sina.com/sso/login.php?client=ssologin.js(v1.4.15)"
        username = base64.b64encode(self.account.encode("utf-8")).decode("utf-8")
        postData = {
            "entry": "sso", "gateway": "1",
            "from": "null", "savestate": "30",
            "useticket": "0", "pagerefer": "",
            "vsnf": "1", "su": username,
            "service": "sso", "sp": self.password,
            "sr": "1440*900", "encoding": "UTF-8",
            "cdult": "3", 
            "domain": "sina.com.cn",
            "prelt": "0", "returntype": "TEXT",
        }
        # import ipdb; ipdb.set_trace()
        session = requests.Session()
        r = session.post(loginURL, data=postData, headers=self.headers, proxies=self.proxy)
        jsonStr = r.content.decode("gbk")
        info = json.loads(jsonStr)
        if info["retcode"] == "0":
            # logger.warning("Get Cookie Success!( Account:%s )" % account)
            print "Get Cookie Success!( Account:%s )" % self.account
            self.cookie = session.cookies.get_dict()
            # save cookie into Redis
            rconn.hset(ACTIVATED_COOKIE, auth, json.dumps(self.cookie))
            return True
        else:
            # logger.warning("Failed!( Reason:%s )" % info["reason"])
            print "Failed!( Reason:%s )" % info["reason"]
            time.sleep(2)
            return False

    def update_cookie(self, rconn):
        """ 更新一个账号的Cookie """
        auth = '%s--%s' % (self.account, self.password)
        status = self.gen_cookie(self.account, self.password, rconn)
        if status:
            # logger.warning("The cookie of %s has been updated successfully!" % account)
            print "The cookie of %s has been updated successfully!" % account
            rconn.hset(ACTIVATED_COOKIE, auth, self.cookie)
        else:
            # logger.warning("The cookie of %s updated failed! Remove it!" % accountText)
            print "The cookie of %s updated failed! Remove it!" % accountText
            self.removeCookie(rconn)

    def remove_cookie(self, rconn):
        """ 删除某个账号的Cookie """
        auth = '%s--%s' % (self.account, self.password)
        rconn.hdel(ACTIVATED_COOKIE, auth)
        cookie_num = rconn.hlen(ACTIVATED_COOKIE)
        print "The num of the cookies left is %s" % cookie_num

    # @catch_network_error(exc_list)
    @retry(exc_list, tries=2, delay=3, backoff=2)
    def gen_html_source(self):
        """
        Separate get page and parse page
        """
        if not self.cookie:
            return False
        # s = requests.session()
        # import ipdb; ipdb.set_trace()
        # home_response = s.get('http://m.weibo.com/', cookies=self.cookie, proxies=self.proxy)
        info_response = requests.get(self.url, timeout=self.timeout, headers=self.headers,
            cookies=self.cookie, proxies=self.proxy)
        text = info_response.text.encode('utf8')
        now_time = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        if info_response.status_code == 429:
            raise ConnectionError("Hey, guy, too many requests")
        elif len(text) == 0:
            print 'Access nothing back'
        elif len(text) < 10000:  # Let IndexError disappear
            print >>open('./html/block_error_%s_%s.html' % (self.account, now_time), 'w'), text
            raise ConnectionError('Hey, boy, you were blocked..')
        elif text.find('<title>404错误</title>') > 0:  # <title>404错误</title>
            print >>open('./html/freezed_account_%s_%s.html' % (self.account, now_time), 'w'), text
            raise ConnectionError('The account were freezed')
        elif 16000<len(info_response.text)<18000:
            print >>open('./html/ghost_error_%s_%s.html' % (self.account, now_time), 'w'), text
            raise ConnectionError('Ghost Error, incorrect source code but not freezed')
        if info_response.status_code == 200:
            self.page = text
        time.sleep(self.delay)
        return True
