"""
create table people_relation (
    id int not null primary key auto_increment, 
    left_node varchar(16) not null, 
    right_node varchar(16) not null, 
    is_alived varchar(2) not null default 'Y', 
    crawl_date datetime default current_timestamp, 
    update_date datetime default current_timestamp on update current_timestamp, 
    index unique_edge(left_node, right_node));
create table people_info (
    id int not null primary key,
    uid varchar(16) not null,
    url varchar(64) not null default '',
    name varchar(64) not null default '',
    gender varchar(2) not null default '',
    blog_num int(16) not null default 0,
    follow_num int(16) not null default 0,
    fans_num int(16) not null default 0,
    image_url varchar(128) not null default '',
    introduction text not null,
    description text not null,
    verified varchar(16) not null default '',
    verified_type varchar(16) not null default '',
    mbtype varchar(16) not null default '',
    mbrank varchar(16) not null default '',
    rank varchar(16) not null default '',
    is_actived varchar(2) not null default 'Y',
    create_date datetime default current_timestamp,
    update_date datetime default current_timestamp on update current_timestamp,
    index unique_uid (uid),
    index unique_url (url)
)
"""
#!/usr/bin/env python
#-*- coding: utf8-*-
import traceback
from zc_spider.weibo_writer import DBAccesor, database_error_hunter


class WeiboRelationWriter(DBAccesor):

    def __init__(self, db_dict):
        DBAccesor.__init__(self, db_dict)

    def connect_database(self):
        return DBAccesor.connect_database(self)

    @database_error_hunter
    def insert_relation_into_db_test(self, info_list):
        insert_people_sql = """
            INSERT INTO people_info (uid, url, name, gender, blog_num, follow_num, fans_num,
            introduction, description, verified, verified_type, mbtype, mbrank, rank, crawl_date
            )
            SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            FROM DUAL WHERE not exists(
            SELECT * FROM people_info WHERE uid = %s)
        """
        insert_edge_sql = """
            INSERT INTO people_relation (left_node, right_node, crawl_date)
            SELECT %s,%s,%s
            FROM DUAL WHERE not exists(
            SELECT * FROM people_relation WHERE left_node = %s and right_node = %s)
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        try:
            for info in info_list:
                if cursor.execute(insert_people_sql,(
                    info['uid'], info['url'], info['name'], info.get('gender', ''),
                    info.get('blog_num', 0), info.get('follows', 0), info.get('fans', 0),
                    info.get('intro', ''), info.get('desc', ''), info.get('verified', ''),
                    info.get('verified_type', ''), info.get('mbtype', ''), info.get('mbrank', ''),
                    info.get('rank', ''), info['date'], info['uid']
                )):
                    print '$'*10, "1. Insert people(%s, %s) SUCCEED !!!" % (info['uid'], info['name'])
                if cursor.execute(insert_edge_sql, (
                    info['left'], info['right'], info['date'], info['left'], info['right']
                )):
                    print '$'*10, "2. Insert relation(%s->%s) SUCCEED !!!" % (info['left'], info['right'])
            conn.commit(); cursor.close(); conn.close()
        except Exception as e:
            traceback.print_exc()
            conn.rollback()
            conn.commit(); cursor.close(); conn.close()
            raise Exception(str(e))

    def insert_follow_into_db(self, user_list):
        insert_sql = """
            INSERT INTO weibouserfollows
            (nickname, weibo_user_url, follow_nickname, follow_fans_num, follow_weibo_num, 
            follow_focus_num, follow_type, follow_usercard, createdate, is_up2date)
            SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Y'
            FROM DUAL WHERE NOT EXISTS (SELECT id FROM weibouserfollows 
            WHERE weibo_user_url = %s and follow_usercard = %s and is_up2date = 'Y')
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        try:
            for user in user_list:
                if cursor.execute(insert_sql, ('', 
                    user['user_url'], user.get('nickname', ''), 
                    user.get('fans', ''), user.get('blog_num', ''), 
                    user.get('follows', ''), user.get('type', ''), 
                    user['uid'], user['create_date'], 
                    user['user_url'], user['uid']
                )):
                    print '$'*10, 'Write relation(%s->%s) info succeeded !' % (user['user_url'], user['uid'])
                else:
                    print "Relation Existed"
            # conn.commit(); cursor.close(); conn.close()
        except Exception as e:
            traceback.print_exc()
            conn.commit(); cursor.close(); conn.close()
            raise Exception(str(e))


    def read_user_url_from_db(self):
        select_user_sql = """
            SELECT DISTINCT wu.weibo_user_url, wu.weibo_user_card
            FROM WeiboTopic t, topicweiborelation tw, weibocomment wc, weibouser wu  
            WHERE t.topic_url = tw.topic_url 
            AND tw.weibo_url = wc.weibo_url 
            AND wc.weibocomment_author_url = wu.weibo_user_url 
            AND wu.createdate > DATE_SUB(DATE(now()), INTERVAL '1' DAY) LIMIT 10
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        cursor.execute(select_user_sql)
        for res in cursor.fetchall():
            yield (res[0], res[1])