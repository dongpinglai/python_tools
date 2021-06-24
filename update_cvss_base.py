#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import psycopg2
import mysql.connector
from collections import namedtuple, OrderedDict
import os
import json
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from functools import partial
import time
import re

DbConfig = namedtuple('DbConfig', 'user password host port database db_type')

SRC_DB_CFG = DbConfig('postgres', 'postgres', '127.0.0.1', 5432,'gvmd', 'postgresql')
DST_DB_CFG = DbConfig('root', 'fakepassword', '172.16.110.157', 33066, 'security', 'mysql')

class Transfor(object):
    def __init__(self, src_db_cfg=SRC_DB_CFG, dst_db_cfg=DST_DB_CFG):
        self._src_db_cfg = src_db_cfg
        self._src_db = Db(self._src_db_cfg)
        self._dst_db_cfg = dst_db_cfg
        self._dst_db = Db(self._dst_db_cfg)
        self._executor = ThreadPoolExecutor(128)
    
    def query_nvts(self):
        table_name = 'nvts'
        sql = 'SELECT * FROM %s' % table_name
        return self._src_db.fetch_all(sql)

    def update_cvss_base(self):
        nvts = self.query_nvts()
        fs = self._executor.map(self._run, nvts)
        as_completed(fs)
        print('shutdown....')
        self._executor.shutdown()
        
    def _run(self, item):
        table_name = 'bd_host_vul_lib_2021'
        oid = item['oid']
        cvss_base = item['cvss_base']
        where = 'oid = "%s"' % oid
        update = 'cvss_base = "%s"' % cvss_base
        _dst_db = Db(self._dst_db_cfg)
        try:
            _dst_db.update(table_name, update, where)
        except Exception as e:
            print('Error: ', e)
        else:
            _dst_db.commit()
        finally:
            _dst_db.close() 
    
        
class Db(object):
    def __init__(self, db_cfg):
        self._db_cfg = db_cfg
        self._db_type = self._db_cfg.db_type
        self._conn = None
        self._cursor = None

    @property
    def conn(self):
        if self._conn is None:
            db_cfg = self._db_cfg
            db_type = self._db_type
            if self.is_postgresql:
                self._conn = psycopg2.connect(user=db_cfg.user, password=db_cfg.password, host=db_cfg.host, port=db_cfg.port, database=db_cfg.database)
            else:
                self._conn = mysql.connector.connect(user=db_cfg.user, password=db_cfg.password, host=db_cfg.host, port=db_cfg.port, database=db_cfg.database)
        return self._conn

    @property
    def cursor(self):
        if self._cursor is None:
            if self.is_postgresql:
                self._cursor = self.conn.cursor()
            else:
                self._cursor = self.conn.cursor(dictionary=True, buffered=True)
        return self._cursor

    @property 
    def is_postgresql(self):
        return self._db_type == 'postgresql'

    def query2dict(self, queryset):
        columns = [col.name for col in self.cursor.description]
        for row in queryset:
            yield dict(zip(columns, row))
        
    def execute(self, query, vars=None):
        print('execute', query)
        cur = self.cursor
        cur.execute(query, vars)

    def execute_many(self, query, vars):
        print('execute_many', query)
        self.cursor.executemany(query, vars)

    def fetch_all(self, query, vars=None):
        self.execute(query, vars)
        queryset = self.cursor.fetchall()
        if self.is_postgresql:
            return list(self.query2dict(queryset))
        else:
            return queryset

    def fetch_one(self, query, vars=None):
        self.execute(query, vars)
        queryset = self.cursor.fetchone()
        if queryset:
            if self.is_postgresql: 
                a_dict_gen = self.query2dict([queryset])
                return list(a_dict_gen)[0]
            else:
                return queryset
        else:
            return None

    def sequence2str(self, seq, has_quota=False):
        str_list = ['(']
        for item in seq:
            if has_quota:
                str_list.append('"')
                str_list.append(item)
                str_list.append('"')
            else:
                str_list.append(item)
            str_list.append(',')
        else:
            str_list.pop(-1)
            str_list.append(')')
        return ''.join(str_list)

    def insert(self, db_name, fields, datas):
        sql = 'INSERT INTO %s %s VALUES %s'
        place_hold = ['%s'] * len(fields)
        sql = sql % (db_name, self.sequence2str(fields), self.sequence2str(place_hold))
        self.execute_many(sql, datas)
        return self.cursor.lastrowid

    def update(self, db_name, update, where):
        if where:
            sql = 'UPDATE %s SET %s WHERE %s' % (db_name, update, where)
        else:
            sql = 'UPDATE %s SET %s' % (db_name, update)
        self.execute(sql)

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()
        
        
if __name__ == '__main__':
    transfor = Transfor()
    transfor.update_cvss_base()
