#-*- coding: utf-8 -*-
#--------  不良人  --------
import os
import re
import sys
import json
import time
import redis
from datetime import datetime as dt
from bs4 import BeautifulSoup as bs
from weibo_relationship_spider import WeiboRelationSpider
from weibo_relationship_writer import WeiboRelationWriter
from zc_spider.weibo_config import (
    MANUAL_COOKIES, WEIBO_ACCOUNT_PASSWD, 
    LOCAL_REDIS, MAD_MAN_URLS, MAD_MAN_INFO
)
reload(sys)
sys.setdefaultencoding('utf-8')

USED_DATABASE = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'passwd': 'mysql',
    'db': 'weibo_data',
    'charset': 'utf8',
    'connect_timeout': 20,
}

USED_REDIS = LOCAL_REDIS

curl = "curl 'http://m.weibo.cn/container/getSecond?containerid=1005051652811601_-_FANS&page=3' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cache-Control: max-age=0' -H 'Cookie: _T_WM=3ba14b38ec55ecdada6c00655da0e0be; ALF=1486260707; SCF=Ap11mp4UEZs9ZcoafG0iD1wVDGjdyuPuLY8BpwtpvSEESqU7totC2-GlyhxAFP4R_yxrN2RyHwEaoSQkifhGedk.; SUB=_2A251axBtDeTxGeNG71EX8ybKwj6IHXVWl7AlrDV6PUJbktBeLVDSkW2Q9G3Mwm2cTsDC8dEjBV6T9-GRkg..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5HA7SsRPVzLQ_q6ucc2n_c5JpX5o2p5NHD95Qf1hB0SoeRSo.EWs4Dqcj6i--ciK.Ni-27i--ciKnRiK.pi--Xi-z4iKyFi--4iK.Ri-z0i--ciK.RiKy8i--fi-z7iK.pi--fi-z4i-zX; SUHB=0laVriVMID_BtL; SSOLoginState=1483694141; H5_INDEX=2; H5_INDEX_TITLE=%E6%97%A0%E6%88%91%E4%B9%8B%E9%98%B3%E6%98%8E%E5%B0%91%E5%B9%B4; M_WEIBOCN_PARAMS=luicode%3D10000011%26lfid%3D1076031652811601%26fid%3D1005051652811601_-_FANS%26uicode%3D10000012' -H 'Connection: keep-alive' --compressed"

XHR_URL = "http://m.weibo.cn/container/getSecond?containerid=100505{uid}_-_{title}&page={page}"
# title = FANS / FOLLOWERS

test_curl = "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4059539257612395&__rnd=1483676899106' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cache-Control: max-age=0' -H 'Cookie: SINAGLOBAL=7912212257618.43.1478585959985; wb_publish_fist100_5843638692=1; wvr=6; _T_WM=03e781554acf9dd24f1be01327a60a32; YF-Page-G0=d0adfff33b42523753dc3806dc660aa7; _s_tentry=-; Apache=9751347814485.37.1483668519299; ULV=1483668519511:25:3:3:9751347814485.37.1483668519299:1483508239455; YF-Ugrow-G0=8751d9166f7676afdce9885c6d31cd61; WBtopGlobal_register_version=c689c52160d0ea3b; SCF=Ap11mp4UEZs9ZcoafG0iD1wVDGjdyuPuLY8BpwtpvSEEvUHF2uToKM-7WlBpLkmhZ8RBzBoq6rkGPr6RQnLxkPM.; SUB=_2A251aoy0DeTxGeNG71EX8ybKwj6IHXVWAfl8rDV8PUNbmtANLXbhkW-Ca4XWBrg6Mlj9Y8JHL6ezeBXp4A..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5HA7SsRPVzLQ_q6ucc2n_c5JpX5K2hUgL.Fo-RShece0nc1Kz2dJLoI0YLxKqL1KMLBK5LxKqL1hnL1K2LxKBLBo.L12zLxK.L1KnLBoeLxKqL1KnL12-LxK-LBo5L1K2LxK-LBo.LBoBt; SUHB=0sqRRqxSCPeB1B; ALF=1484273507; SSOLoginState=1483668708; un=jiangzhibinking@outlook.com; YF-V5-G0=a9b587b1791ab233f24db4e09dad383c; UOR=,,zhiji.heptax.com' -H 'Connection: keep-alive' --compressed"

def single_process():
    dao = WeiboRelationWriter(USED_DATABASE)
    cache = redis.StrictRedis(**USED_REDIS)
    job = '1652811601'
    url = XHR_URL.format(uid=job, title='FOLLOWERS', page=1)
    spider = WeiboRelationSpider(job, url, 'binking', WEIBO_ACCOUNT_PASSWD, timeout=20)
    spider.use_abuyun_proxy()
    spider.add_request_header()
    # spider.use_cookie_from_curl(cache.hget(MANUAL_COOKIES, account))
    spider.use_cookie_from_curl(test_curl)
    status = spider.gen_html_source(raw=True)
    results = spider.parse_relationship()
    dao.insert_relation_into_db(results)


if __name__=="__main__":
    print "\n\n" + "%s Began Scraped Weibo New Users" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    single_process()
    print "*"*10, "Totally Scraped Weibo New Users Time Consumed : %d seconds" % (time.time() - start), "*"*10
