#coding=utf-8
import os
import sys
import time
import redis
import random
import pickle
import argparse
import traceback
from datetime import datetime as dt
import multiprocessing as mp
from requests.exceptions import ConnectionError
from zc_spider.weibo_config import (
    RELATION_RESULTS_CACHE,
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


def user_db_writer(cache):
    """
    Consummer for topics
    """
    cp = mp.current_process()
    dao = WeiboRelationWriter(USED_DATABASE)
    while True:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Write Follow Process pid is %d" % (cp.pid)
        res = cache.blpop(RELATION_RESULTS_CACHE, 0)[1]
        edges = pickle.loads(res)
        try:
            dao.insert_follow_into_db(edges)   # ////// broken up, cuz res is string
        except Exception as e:  # won't let you died
            traceback.print_exc()
            cache.rpush(RELATION_RESULTS_CACHE, res)
            print 'Failed to write result: %s' % edges
        except KeyboardInterrupt as e:
            print "Interrupted in Write process"
            cache.rpush(RELATION_RESULTS_CACHE, res)
            break


def run_multiple_writer():
    r = redis.StrictRedis(**USED_REDIS)  # list
    try:
        p = mp.Pool(processes=1, initializer=user_db_writer, initargs=(r, ))
        p.close(); p.join()
    except Exception as e:
        print "Exception Occured: " + str(e)
    except KeyboardInterrupt as e:
        print "Interrupted by You: " + str(e)
    print "All done"

if __name__=="__main__":
    print "\n\n" + "爬取用户全部关注 began at %s" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    run_multiple_writer()
    # single_process()
    print "*"*10, "Totally Scraped Weibo User Follows Time Consumed : %d seconds" % (time.time() - start), "*"*10
