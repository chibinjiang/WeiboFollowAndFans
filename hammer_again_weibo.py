#!coding=utf-8
import os
import json
import redis
from zc_spider.weibo_config import (
    WEIBO_ACCOUNTS_COOKIES, WEIBO_COOKIES,
    LOCAL_REDIS, QCLOUD_REDIS,
)

if os.environ.get('SPIDER_ENV') == 'test':
    print "*"*10, "Run in Test environment"
    USED_REDIS = LOCAL_REDIS
elif 'centos' in os.environ.get('HOSTNAME'):
    print "*"*10, "Run in Qcloud environment"
    USED_REDIS = QCLOUD_REDIS
else:
    raise Exception("Unknown Environment, Check it now...")

def write_curl_str_into_redis(rconn):
    for account in WEIBO_ACCOUNTS_COOKIES:
        if account in rconn.hkeys(WEIBO_COOKIES):
            print "Alive account %s was in Redis" % account
            continue
        html = os.popen(WEIBO_ACCOUNTS_COOKIES[account] + ' --silent').read()
        try:
            if json.loads(html)['code'] == '100000':
                print "Write %s cookie into Redis" % account
                rconn.hset(WEIBO_COOKIES, account, WEIBO_ACCOUNTS_COOKIES[account])
        except:
            if len(html) < 20000:
                print "%s 已失效.." % account
            else:  # still alive
                print "Write %s cookie into Redis" % account
                rconn.hset(WEIBO_COOKIES, account, WEIBO_ACCOUNTS_COOKIES[account])


def remove_dead_curl_str(rconn):
    for account in rconn.hkeys(WEIBO_COOKIES):
        html = os.popen(WEIBO_ACCOUNTS_COOKIES[account] + ' --silent').read()
        try:
            if json.loads(html)['code'] == '100000':
                print '%s will be leaved..' % account
            else:
                print "%s will be removed.." % account
                rconn.hdel(WEIBO_COOKIES, account)
        except:
            if len(html) < 20000:
                print "%s will Survive .." % account
                rconn.hdel(WEIBO_COOKIES, account)
            else:
                print '%s will Survive..' % account


if __name__=='__main__':
    r = redis.StrictRedis(**USED_REDIS)
    remove_dead_curl_str(r)
    write_curl_str_into_redis(r)
