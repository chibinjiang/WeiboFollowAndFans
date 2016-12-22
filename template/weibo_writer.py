#coding=utf-8
import sys
import time
import traceback
from datetime import datetime as dt
import MySQLdb as mdb
from weibo_config import *

reload(sys)
sys.setdefaultencoding('utf-8')


def database_error_hunter(db_func):
    """
    A decrator that catch exceptions and print relative infomation
    """
    def handle_exception(*args, **kargs):
        try:
            return db_func(*args, **kargs)
        except(mdb.ProgrammingError, mdb.OperationalError) as e:
            traceback.print_exc()
            if 'MySQL server has gone away' in e.message:
                print dt.now().strftime("%Y-%m-%d %H:%M:%S"),ERROR_MSG_DICT[DB_SEVER_GONE_AWAY],
            elif 'Deadlock found when trying to get lock' in e.message:
                print dt.now().strftime("%Y-%m-%d %H:%M:%S"),ERROR_MSG_DICT[DB_FOUND_DEADLOCK],
            elif 'Lost connection to MySQL server' in e.message:
                print dt.now().strftime("%Y-%m-%d %H:%M:%S"),ERROR_MSG_DICT[DB_LOST_CONNECTION],
            elif 'Lock wait timeout exceeded' in e.message:
                print dt.now().strftime("%Y-%m-%d %H:%M:%S"),ERROR_MSG_DICT[DB_LOCK_WAIT_TIMEOUT],
            elif e.args[0] in [1064, 1366]:
                print dt.now().strftime("%Y-%m-%d %H:%M:%S"),ERROR_MSG_DICT[DB_UNICODE_ERROR],
            else:
                print dt.now().strftime("%Y-%m-%d %H:%M:%S"),ERROR_MSG_DICT[DB_UNKNOW_ERROR],
        except Exception as e:
            traceback.print_exc()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"),ERROR_MSG_DICT[DB_WRITE_FAILED],
    return handle_exception


class DBAccesor(object):
    def __init__(self, db_dict):
        self.db_setting = db_dict

    def connect_database(self):
        """
        We can't fail in connect database, which will make the subprocess zoombie
        """
        attempt = 1
        for _ in range(16):
            seconds = 3*attempt
            try:
                # WEBCRAWLER_DB_CONN = mdb.connect(**OUTER_MYSQL)
                conn = mdb.connect(**self.db_setting)
                print '$'*10, 'Connected database succeeded...'
                return conn
            except mdb.OperationalError as e:
                print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Sleep %s seconds cuz we can't connect MySQL..." % seconds
            except Exception as e:
                traceback.print_exc()
                print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Sleep %s cuz unknown connecting database error." % seconds
            attempt += 1
            time.sleep(seconds)

