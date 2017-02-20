#-*- coding: utf-8 -*-
#--------  不良人  --------
import os
import re
import sys
import json
import time
import redis
import random
import pickle
import traceback
import multiprocessing as mp
from datetime import datetime as dt
from requests.exceptions import ConnectionError
from zc_spider.weibo_config import (
    MANUAL_COOKIES, WEIBO_ACCOUNT_PASSWD, 
    LOCAL_REDIS, MAD_MAN_URLS, MAD_MAN_INFO
)
from weibo_relationship_spider import WeiboRelationSpider
from weibo_relationship_writer import WeiboRelationWriter
from zc_spider.weibo_utils import RedisException
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
WEIBO_FINISHED_UIDS = 'weibo:finished:uids'  # set
USED_REDIS = LOCAL_REDIS
XHR_URL = "http://m.weibo.cn/container/getSecond?containerid=100505{uid}_-_{title}&page={page}"
# title = FANS / FOLLOWERS
curl = "curl 'http://m.weibo.cn/container/getSecond?containerid=1005051652811601_-_FANS&page=3' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cache-Control: max-age=0' -H 'Cookie: _T_WM=3ba14b38ec55ecdada6c00655da0e0be; ALF=1486260707; SCF=Ap11mp4UEZs9ZcoafG0iD1wVDGjdyuPuLY8BpwtpvSEESqU7totC2-GlyhxAFP4R_yxrN2RyHwEaoSQkifhGedk.; SUB=_2A251axBtDeTxGeNG71EX8ybKwj6IHXVWl7AlrDV6PUJbktBeLVDSkW2Q9G3Mwm2cTsDC8dEjBV6T9-GRkg..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5HA7SsRPVzLQ_q6ucc2n_c5JpX5o2p5NHD95Qf1hB0SoeRSo.EWs4Dqcj6i--ciK.Ni-27i--ciKnRiK.pi--Xi-z4iKyFi--4iK.Ri-z0i--ciK.RiKy8i--fi-z7iK.pi--fi-z4i-zX; SUHB=0laVriVMID_BtL; SSOLoginState=1483694141; H5_INDEX=2; H5_INDEX_TITLE=%E6%97%A0%E6%88%91%E4%B9%8B%E9%98%B3%E6%98%8E%E5%B0%91%E5%B9%B4; M_WEIBOCN_PARAMS=luicode%3D10000011%26lfid%3D1076031652811601%26fid%3D1005051652811601_-_FANS%26uicode%3D10000012' -H 'Connection: keep-alive' --compressed"


def generate(cache):
    """
    Producer for urls and topics, Consummer for topics
    """
    error_count = 0
    cp = mp.current_process()
    while True:
        res = {}
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Generate Buliangren Process pid is %d" % (cp.pid)
        job = cache.blpop(MAD_MAN_URLS, 0)[1]  # job is user card
        try:
            if error_count > 999:
                print '>'*10, 'Exceed 1000 times of GEN errors', '<'*10
                break
            all_account = cache.hkeys(MANUAL_COOKIES)
            account = random.choice(all_account)
                    spider = WeiboRelationSpider(job, url, account, WEIBO_ACCOUNT_PASSWD, timeout=20)
                    spider.use_abuyun_proxy()
                    spider.add_request_header()
                    spider.use_cookie_from_curl(cache.hget(MANUAL_COOKIES, account))
                    status = spider.gen_html_source()
                    is_last, list_of_users = spider.parse_relationship(cache)
                    if list_of_users:
                        cache.rpush(MAD_MAN_INFO, pickle.dumps(list_of_users))
                        for user in list_of_users:
                            print "Find user(%s), blogs(%s), follow(%s), fans(%s)" % \
                            (user['name'], user['blog_num'], user['follows'], user['fans'])
                            if user['fans'] > 10000 or user['follows'] > 5000:
                                continue   # too popular should be ignred
                            if (user['ship'] == 'fans' and not cache.sismember(WEIBO_FINISHED_UIDS, user['left'])):
                                cache.rpush(MAD_MAN_URLS, user['left'])
                            elif (user['ship']=='follow' and not cache.sismember(WEIBO_FINISHED_UIDS, user['right'])):
                                cache.rpush(MAD_MAN_URLS, user['right'])
                    if is_last:
                        continue
            cache.sadd(WEIBO_FINISHED_UIDS, job)
        except RedisException as e:
            print str(e)
            break
        except Exception as e:  # no matter what was raised, cannot let process died
            traceback.print_exc()
            print 'Failed to parse job: ', job
            cache.rpush(MAD_MAN_URLS, job) # put job back
            error_count += 1
        

def write_data(cache):
    """
    Consummer for topics
    """
    error_count = 0
    cp = mp.current_process()
    dao = WeiboRelationWriter(USED_DATABASE)
    while True:
        if error_count > 999:
            print '>'*10, 'Exceed 1000 times of write errors', '<'*10
            break
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Write Buliangren Process pid is %d" % (cp.pid)
        list_of_users = cache.blpop(MAD_MAN_INFO, 0)[1]
        temp = pickle.loads(list_of_users)
        try:
            dao.insert_relation_into_db(temp)
        except Exception as e:  # won't let you died
            error_count += 1
            print 'Failed to write result: ', str(temp)
            cache.rpush(MAD_MAN_INFO, list_of_users)


def run_all_worker():
    r = redis.StrictRedis(**USED_REDIS)
    start_urls = ['5843638692']
    for url in start_urls:
        for title in ['FOLLOWERS', 'FANS']:
            for page in range(1, 22):
                url = XHR_URL.format(uid=job, title=title, page=page)
                    r.rpush(MAD_MAN_URLS, url)
    job_pool = mp.Pool(processes=2,
        initializer=generate, initargs=(r, ))
    result_pool = mp.Pool(processes=1, 
        initializer=write_data, initargs=(r, ))

    cp = mp.current_process()
    print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Run All Works Process pid is %d" % (cp.pid)
    try:
        job_pool.close(); result_pool.close()
        job_pool.join(); result_pool.join()
        print "+"*10, "jobs' length is ", r.llen(MAD_MAN_URLS) 
        print "+"*10, "results' length is ", r.llen(MAD_MAN_INFO)
    except Exception as e:
        traceback.print_exc()
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Exception raise in run all Work"
    except KeyboardInterrupt:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Interrupted by you and quit in force, but save the results"
        print "+"*10, "jobs' length is ", r.llen(MAD_MAN_URLS) 
        print "+"*10, "results' length is ", r.llen(MAD_MAN_INFO)


if __name__=="__main__":
    print "\n\n" + "%s Began Scraped Weibo New Users" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    run_all_worker()
    print "*"*10, "Totally Scraped Weibo New Users Time Consumed : %d seconds" % (time.time() - start), "*"*10
