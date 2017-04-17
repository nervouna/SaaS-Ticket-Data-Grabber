#! /usr/bin/env python3
# coding: utf-8

from __future__ import absolute_import
from __future__ import print_function

import os
import sys
import sqlite3
import subprocess
import csv
import argparse
from functools import wraps

import leancloud
import arrow


DATABASE = os.path.join(os.path.abspath(os.path.expanduser('~')), '.saas-ticket.db')


class Ticket(leancloud.Object):
    pass


class Reply(leancloud.Object):
    pass


class Organization(leancloud.Object):
    @classmethod
    def leancloud(cls):
        return cls.create_without_data('564ea2c500b0ee7f59eb8dfc')


def init_db():
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        with open('schema.sql', 'r') as f:
            c.executescript(f.read())
        conn.commit()
    print('📦 ', '数据库初始化完毕，数据库位置：{0} '.format(DATABASE))


def save_config():
    try:
        output = subprocess.check_output(['lean', 'env'])
    except FileNotFoundError:
        sys.exit('💩  请先安装 LeanCloud 命令行工具')
    except subprocess.CalledProcessError:
        sys.exit('💩  请确认是否已通过命令行登录并切换到对应应用')
    output = output.decode('utf-8').strip().split('\n')[1:]
    output = [x.split(' ', 1)[1] for x in output]
    envs = [x.split('=') for x in output]
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.executemany('insert into config values (?, ?)', envs)
        conn.commit()


def init_leancloud_sdk():
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        keys = ('LEANCLOUD_APP_ID', 'LEANCLOUD_APP_KEY', 'LEANCLOUD_APP_MASTER_KEY')
        c.execute('select value from config where key in (?, ?, ?)', keys)
        credentials = [x[0] for x in c.fetchall()]
        try:
            leancloud.init(*credentials)
            leancloud.use_master_key(False)
        except TypeError:
            sys.exit('💩  初始化参数不正确。请检查是否已经正确执行了 save_config 。')
        except leancloud.LeanCloudError as e:
            sys.exit(e)


def _dump(obj):
    if isinstance(obj, Ticket):
        ticket_status = ('待解决', '已回复', '完成')
        t = (obj.get('tid'),
             obj.get('category').get('name'),
             obj.get('title'),
             obj.get('content'),
             int(obj.get('createdAt').timestamp()),
             obj.get('assign2').get('username'),
             ticket_status[obj.get('status')],
             obj.get('user').get('username'))
    elif isinstance(obj, Reply):
        t = (obj.get('rid'),
             obj.get('content') or '无内容',
             obj.get('user').get('username'),
             int(obj.get('createdAt').timestamp()),
             obj.get('tid'))
    else:
        raise TypeError('💩  暂时只支持保存 ticket 和 reply 。')
    return t


def fetch_remote_tickets(start_time=None):
    query = leancloud.Query('Ticket')
    leancloud_org = Organization.leancloud()
    query.add_ascending('createdAt')
    query.equal_to('org', leancloud_org)
    query.limit(1000)
    query.include('category')
    query.include('assign2')
    query.include('user')
    if start_time:
        query.greater_than('createdAt', start_time)
    return query.find()


def fetch_remote_replies(start_time=None):
    query = leancloud.Query('Reply')
    query.add_ascending('createdAt')
    query.limit(1000)
    query.include('user')
    if start_time:
        query.greater_than('createdAt', start_time)
    return query.find()


def save_tickets():
    start = get_local_ticket_updated_time()
    if start:
        start = arrow.get(start).datetime
    tickets = fetch_remote_tickets(start)
    while tickets and len(tickets) % 1000 == 0:
        tickets += fetch_remote_tickets(tickets[-1].get('createdAt'))
    tickets = [_dump(obj) for obj in tickets]
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.executemany('insert into tickets values (?, ?, ?, ?, ?, ?, ?, ?)', tickets)
        conn.commit()


def save_replies():
    start = get_local_reply_updated_time()
    if start:
        start = arrow.get(start).datetime
    replies = fetch_remote_replies(start)
    while replies and len(replies) % 1000 == 0:
        replies += fetch_remote_replies(replies[-1].get('createdAt'))
    replies = [_dump(obj) for obj in replies]
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.executemany('insert into replies values (?, ?, ?, ?, ?)', replies)
        conn.commit()


def get_local_tickets(offset=7):
    offset = -offset
    start_time = arrow.utcnow().to('local').replace(days=offset)
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.execute('select * from tickets where created_at>?', (start_time.timestamp,))
        return c.fetchall()


def get_local_replies(offset=7):
    offset = -offset
    start_time = arrow.utcnow().to('local').replace(days=offset)
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.execute('select * from replies where created_at>?', (start_time.timestamp,))
        return c.fetchall()


def get_local_ticket_count():
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.execute('select count(*) from tickets')
        return c.fetchone()[0]


def get_local_reply_count():
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.execute('select count(*) from replies')
        return c.fetchone()[0]


def get_local_ticket_updated_time():
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.execute('select max(created_at) from tickets')
        return c.fetchone()[0]


def get_local_reply_updated_time():
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.execute('select max(created_at) from replies')
        return c.fetchone()[0]


def show_status():
    tickets = get_local_ticket_count()
    replies = get_local_reply_count()
    if tickets and replies:
        last_updated = arrow.get(max([get_local_ticket_updated_time(),
                                     get_local_reply_updated_time()])).to('local').humanize(locale='zh')
        print('👌 ', '已存储{0}条工单和{1}条工单评论。'.format(tickets, replies))
        print('👌 ', '本地最新数据更新于{0}。'.format(last_updated))
    else:
        print('👌 ', '请使用 pull 命令更新本地数据。')


def save_csv(table_name='tickets'):
    filename = table_name + '.csv'
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        if table_name == 'tickets':
            c.execute('select * from tickets')
        elif table_name == 'replies':
            c.execute('select * from replies')
        names = list(map(lambda x: x[0], c.description))
        created_at = names.index('created_at')
        data = [list(x) for x in c.fetchall()]
        for row in data:
            row[created_at] = arrow.get(row[created_at]).to('local').strftime('%Y-%m-%d')
    with open(filename, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(names)
        writer.writerows(data)
        print('👌 ', '{0} 表的数据已经导出为 {1}，执行 open {1} 可直接打开。'.format(table_name, filename))


def usage(more=False):
    lines = ['S-Ticket Data Collector',
             '👇',
             '参数\t说明',
             'init\t初始化本地数据库，只需运行一次，重复运行会清除本地数据。',
             'pull\t拉取最新数据，增量拉取，首次运行可能用时较长。',
             'status\t查看本地数据库状态。',
             'csv\t从本地数据库生成 CSV 文件。',
             '👆',
             '第一次使用时请 lean login && lean switch 选择正确的应用，',
             '然后使用 python main.py init 初始化本地数据库。']
    print(*lines, sep='\n')


def main():
    args = sys.argv[1:]
    if 'init' in args:
        init_db()
        save_config()
        show_status()
    elif 'pull' in args:
        init_leancloud_sdk()
        save_tickets()
        save_replies()
        show_status()
    elif 'status' in args:
        show_status()
    elif 'csv' in args:
        save_csv('tickets')
        save_csv('replies')
    elif 'help' in args:
        usage(more=True)
    else:
        usage()


if __name__ == '__main__':
    main()
