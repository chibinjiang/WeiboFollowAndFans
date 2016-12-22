#coding=utf-8
import re
import time
import random
import requests
from functools import wraps
import traceback
from datetime import datetime as dt
from functools import wraps
import multiprocessing as mp
from requests.exceptions import (
    ProxyError,
    Timeout,
    ConnectionError,
    ConnectTimeout,
)
from weibo_config import *


def extract_chinese_info(source):
    eliminate_chars = r'[!"#$%&\'\*\+\./;\<=\>\?@\[\\\]\^_`\{\|\}~a-zA-Z]'
    eliminate_num1 = re.sub(r'"https?://.+?"', '', source)  # elminate numbers in url
    eliminate_num2 = re.sub(r'\</?\w+\d+\>|', '', eliminate_num1)  # eliminate numbers in tag
    eliminate_num3 = re.sub(r'(\w+?)(-?)(\w+)="[\w ]+?"', '', eliminate_num2)  # eliminate numbers and - in attr
    eliminate_num4 = re.sub(eliminate_chars, '', eliminate_num3)
    return re.sub(r'\s+', ' ', eliminate_num4).strip()  # elimnate blank


def pick_rand_ele_from_list(elements):
    if not elements:
        return ''
    try:
        rand_index = random.randint(0, len(elements)-1)
        return elements[rand_index]
    except IndexError as e:
        print e
    except ValueError as e:
        print e
    except Exception as e:
        print e

def create_processes(func, args, concurrency):
    for _ in range(concurrency):
        sub_proc = mp.Process(target=func, args=args)
        sub_proc.daemon = True
        sub_proc.start()

def gen_abuyun_proxy():
    # authorization
    proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
        "host" : ABUYUN_HOST,
        "port" : ABUYUN_PORT,
        "user" : ABUYUN_USER,
        "pass" : ABUYUN_PASSWD,
    }
    proxies = {
        "http"  : proxyMeta,
        "https" : proxyMeta,
    }
    return proxies


def change_tunnel():
    """
    return 正在使用的 IP 地址;该 IP 已使用时长;该 IP 可继续使用时长
    """
    proxy_info = {}
    targetUrl = "http://proxy.abuyun.com/switch-ip"
    proxy = gen_abuyun_proxy()
    try:
        resp = requests.get(targetUrl, proxies=proxy)
        if resp.status_code == 200:
            ip, used_time, leaved_time = resp.text.strip().split(',')
            proxy_info = {'ip_addr': ip, 'used_time': int(used_time), 'leaved_time': int(leaved_time)}
    except Exception as e:
        print e
    return proxy_info


def test_abuyun():
    """
    Test
    """
    targetUrl = "http://test.abuyun.com/proxy.php"
    proxy = gen_abuyun_proxy()
    resp = requests.get(targetUrl, proxies=proxy)
    print resp.status_code
    print resp.text


def get_current_ip():
    """
    return 正在使用的 IP 地址;该 IP 已使用时长;该 IP 可继续使用时长
    """
    proxy_info = {}
    targetUrl = "http://proxy.abuyun.com/current-ip"
    proxy = gen_abuyun_proxy()
    try:
        resp = requests.get(targetUrl, proxies=proxy)
        if resp.status_code == 200:
            ip, used_time, leaved_time = resp.text.strip().split(',')
            proxy_info = {'ip_addr': ip, 'used_time': int(used_time), 'leaved_time': int(leaved_time)}
    except Exception as e:
        print e
    return proxy_info


def wrap_print(tag, center, repeat=10):
    print tag * repeat,
    print center,
    print tag * repeat


def handle_sleep(seconds):
    print "Sleeping %d seconds " % seconds, 'zZ'*10, 
    time.sleep(seconds)


def handle_proxy_error(seconds):
    # print "Sleep %d seconds " % seconds, 
    handle_sleep(seconds)
    changed_proxy = change_tunnel()  # Change IP tunnel of Abuyun
    if changed_proxy:
        print "and change IP to %s " % changed_proxy.get("ip_addr")
    else:
        print "but Change Proxy Error"


def str_2_int(num_str):
    try:
        return int(num_str.replace(',', ''))
    except:
        return -1


def chin_num2dec(num_str):
    res = -1
    try:
        num = re.search(r'\d+\.?\d*', num_str).group(0)
        res = int(num)
    except ValueError as e:
        if '亿' in num_str:
            if len(num.split('.')) == 2:
                left, right = num.split('.')
                res = int(left)*pow(10, 8) + int(right)*pow(10, 8-len(right))
            else:
                res = int(num) * pow(10, 8)
        elif '万' in num_str:
            if len(num.split('.')) == 2:
                left, right = num.split('.')
                res = int(left)*pow(10, 4) + int(right)*pow(10, 4-len(right))
            else:
                res = int(num) * pow(10, 4)
        else:
            print 'Unexpected numerical string', num_str
    except Exception as e:
        print e
    return res


def catch_network_error(ExceptionToCheck=(Exception, )):
    """
    catch exception and print info
    """
    def deco_catch(f):
        @wraps(f)
        def catch_f(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except ExceptionToCheck as e:
                print traceback.format_exc()
                print dt.now().strftime("%Y-%m-%d %H:%M:%S"), 
            return f(*args, **kwargs)  # still raise Exception
        return catch_f  # true decorator
    return deco_catch

def catch_parse_error(ExceptionToCheck=(Exception, )):
    """
    catch exception and print info
    """
    def deco_catch(f):
        @wraps(f)
        def catch_f(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except ExceptionToCheck as e:
                print traceback.format_exc()
            return f(*args, **kwargs)  # still raise Exception
        return catch_f  # true decorator
    return deco_catch

def retry(ExceptionToCheck=(Exception, ), tries=4, delay=3, backoff=2, logger=None):
    """ Retry calling the decorated function using an exponential backoff.
    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry
    :param ExceptionToCheck(Exception or tuple): the exception to check. may be a tuple of
        exceptions to check
    :param tries(int): number of times to try (not retry) before giving up
    :param delay(int): initial delay between retries in seconds 
    :param backoff: backoff multiplier, sleep 3*2**n seconds
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    mtries -= 1
                    msg = "%s\nRetrying in %d seconds and leave %d times..." % (traceback.format_exc(), mdelay, mtries)
                    if logger:
                        logger.warning(msg)
                    else:
                        print msg
                    # handle_proxy_error(mdelay)
                    handle_sleep(mdelay)
                    mdelay *= backoff
            return f(*args, **kwargs) # still raise Exception
        return f_retry  # true decorator
    return deco_retry


def catch_network_error_2(req_func):
    def handle_exception(*args, **kargs):
        try:
            return req_func(*args, **kargs)
        except Timeout as e:
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), ERROR_MSG_DICT[NETWORK_TIMEOUT],
        except ProxyError as e:
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), ERROR_MSG_DICT[NETWORK_PROXY_ERROR],
        except ConnectionError as e:
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), ERROR_MSG_DICT[NETWORK_CONNECTION_ERROR],
        except Exception as e:
            traceback.print_exc()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), ERROR_MSG_DICT[REQUEST_ERROR]
    return handle_exception


def extract_cookie_from_curl(curl):
    cookie_dict = {}
    tokens = curl.split("'")
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

def extract_header_from_curl(curl):
    pass

def extract_post_data_from_curl(curl):
    """
    Given curl that was cpoied from Chrome, no matter baidu or sogou, 
    parse it and then get url and the data you will post/get with requests
    """
    post_data = {}
    tokens = curl.split("'")
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
