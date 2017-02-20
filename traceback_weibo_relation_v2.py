#coding=utf-8
#--------  Weibo Followers And Fans  --------
import os
import sys
import time
import redis
import random
import pickle
import traceback
from datetime import datetime as dt
import multiprocessing as mp
from requests.exceptions import ConnectionError
from zc_spider.weibo_config import (
    RELATION_JOBS_CACHE, RELATION_RESULTS_CACHE,
    WEIBO_ACCOUNT_PASSWD, NORMAL_COOKIES,
    QCLOUD_MYSQL, OUTER_MYSQL,
    LOCAL_REDIS, QCLOUD_REDIS
)
from weibo_relationship_spider_v2 import WeiboRelationSpider

reload(sys)
sys.setdefaultencoding('utf-8')

if os.environ.get('SPIDER_ENV') == 'test':
    print "*"*10, "Run in Test environment"
    USED_DATABASE = OUTER_MYSQL
    USED_REDIS = LOCAL_REDIS
elif 'centos' in os.environ.get('HOSTNAME'): 
    print "*"*10, "Run in Qcloud environment"
    USED_DATABASE = QCLOUD_MYSQL
    USED_REDIS = QCLOUD_REDIS
else:
    raise Exception("Unknown Environment, Check it now...")

test_curl = "curl 'http://m.weibo.cn/container/getSecond?containerid=1005052951593932_-_FOLLOWERS&page=1' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cache-Control: max-age=0' -H 'Cookie: browser=d2VpYm9mYXhpYW4%3D; ALF=1490152433; SCF=Ap11mp4UEZs9ZcoafG0iD1wVDGjdyuPuLY8BpwtpvSEEjGlkm9KzWatdQnjbA3jJbKIxYkvAjJXAdwxlwHvh2T4.; SUB=_2A251ruOKDeRxGeVG7FYT8i_OzzWIHXVXUI3CrDV6PUJbktBeLVrgkW1FqHS0pifSjkyU304satffd4rUtA..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WhxM.AD9EjGmQSc51FnJvMU5JpX5o2p5NHD95Q01hMXeozpeoB4Ws4Dqcj.i--4iK.Ri-z0i--ciK.RiKy8i--fi-z7iK.pi--fi-z4i-zX; SUHB=03oFrWuA6tGj8r; SSOLoginState=1487573979; _T_WM=dd802116204f0330fafcd0d5a89786af; M_WEIBOCN_PARAMS=luicode%3D10000012%26lfid%3D1005053969678078_-_FOLLOWERS' -H 'Connection: keep-alive' --compressed"

def user_info_generator(cache):
    """
    Producer for users(cache) and follows(cache2), Consummer for topics
    """
    error_count = 0
    loop_count = 0
    cp = mp.current_process()
    while True:
        res = {}
        loop_count += 1
        if error_count > 9999:
            print '>'*10, 'Exceed 10000 times of gen errors', '<'*10
            break
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Generate Follow Process pid is %d" % (cp.pid)
        job = cache.blpop(RELATION_JOBS_CACHE, 0)[1]   # blpop 获取队列数据
        try:
            # operate spider
            all_account = cache.hkeys(NORMAL_COOKIES)
            if len(all_account) == 0:
                time.sleep(pow(2, loop_count))
                continue
            account = random.choice(all_account)
            uid, user_url, url = job.split("||")
            spider = WeiboRelationSpider(uid, user_url, url, 
                account, WEIBO_ACCOUNT_PASSWD, timeout=20)
            spider.use_abuyun_proxy()
            spider.add_request_header()
            # spider.use_cookie_from_curl(cache.hget(NORMAL_COOKIES, account))
            spider.use_cookie_from_curl(test_curl)
            status = spider.gen_html_source()
            if status in [404, 20003]:
                continue
            f_list = spider.get_user_follow_list(cache)
            if len(f_list ) > 0:
                print "Get relation for %s succeeded .." % user_url
                cache.rpush(RELATION_RESULTS_CACHE, pickle.dumps(f_list))  # push string to the tail
        except Exception as e:  # no matter what was raised, cannot let process died
            cache.rpush(RELATION_JOBS_CACHE, job) # put job back
            print 'Parse %s Error: ' % job + str(e)
            error_count += 1
        except KeyboardInterrupt as e:
            print "Interrupted in Spider process"
            cache.rpush(RELATION_JOBS_CACHE, job) # put job back
            break
        time.sleep(5)


def run_all_worker():
    job_cache = redis.StrictRedis(**USED_REDIS)  # list
    print "Redis have %d records in cache" % job_cache.llen(RELATION_JOBS_CACHE)
    cp = mp.current_process()
    print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Run All Works Process pid is %d" % (cp.pid)
    try:
        job_pool = mp.Pool(processes=2,
            initializer=user_info_generator, initargs=(job_cache, ))
        job_pool.close(); job_pool.join()
    except Exception as e:
        traceback.print_exc()
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Exception raise in runn all Work"
    except KeyboardInterrupt:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Interrupted by you and quit in force, but save the results"
    print "+"*10, "jobs' length is ", job_cache.llen(RELATION_JOBS_CACHE) #jobs.llen(FOLLOWS_JOBS_CACHE)
    print "+"*10, "results' length is ", job_cache.llen(RELATION_RESULTS_CACHE) #jobs.llen(FOLLOWS_JOBS_CACHE)


if __name__=="__main__":
    print "\n\n" + "Began Scraped Weibo User Follows at %s" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    run_all_worker()
    # single_process()
    print "*"*10, "Totally Scraped Weibo User Follows Time Consumed : %d seconds" % (time.time() - start), "*"*10
