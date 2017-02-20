#-*- coding: utf-8 -*-
#--------  话题48992  爬取一个话题下的所有微博  --------
import os
import sys
import time
import redis
from datetime import datetime as dt
from zc_spider.weibo_config import (
    RELATION_JOBS_CACHE,  # weibo:connection:urls
    QCLOUD_MYSQL, OUTER_MYSQL,
    LOCAL_REDIS, QCLOUD_REDIS
)
from weibo_relationship_writer import WeiboRelationWriter

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


def add_jobs(target):
    todo = 0
    dao = WeiboRelationWriter(USED_DATABASE)
    for user_url, uid in dao.read_user_url_from_db():  # iterate
        if not (len(user_url)>0 and len(uid) > 0):
            continue
        todo += 1
        url = "http://m.weibo.cn/container/getSecond?containerid=100505%s_-_FOLLOWERS" % uid
        job = "%s||%s||%s" % (uid, user_url, url)
        if target.lrem(RELATION_JOBS_CACHE, 0, job):
            target.lpush(RELATION_JOBS_CACHE, job)
        else:
            target.rpush(RELATION_JOBS_CACHE, job)
    print 'There are totally %d jobs to process' % todo
    return todo


if __name__=='__main__':
    print "\n\n" + "%s Add users for follow spider began at %s" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    r = redis.StrictRedis(**USED_REDIS)
    if r.llen(RELATION_JOBS_CACHE):
        print "There are still jobs, please add later ..."
    else:
        add_jobs(r)
    print "*"*10, "Totally Time Consumed : %d seconds" % (time.time() - start), "*"*10
    
