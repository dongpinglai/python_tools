#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import psycopg2
import mysql.connector
from collections import namedtuple, OrderedDict
import os
import json
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import time
import re

DbConfig = namedtuple('DbConfig', 'user password host port database db_type')

SRC_DB_CFG = DbConfig('postgres', 'postgres', '127.0.0.1', 5432,'gvmd', 'postgresql')
DST_DB_CFG = DbConfig('root', 'fake_password', '172.16.110.157', 33066, 'security', 'mysql')

class Transfor(object):
    def __init__(self, src_db_cfg=SRC_DB_CFG, dst_db_cfg=DST_DB_CFG):
        self._src_db_cfg = src_db_cfg
        self._src_db = Db(self._src_db_cfg)
        self._dst_db_cfg = dst_db_cfg
        self._dst_db = Db(self._dst_db_cfg)
        self._default_family_id = 20210604 
        self._default_module_id = 20210604 
        self._default_policy_id = 2
        self._executor = ThreadPoolExecutor(128)
        self._found_script_path = []
    
    def query_nvts(self):
        table_name = 'nvts'
        sql = 'SELECT * FROM %s'  % table_name
        return self._src_db.fetch_all(sql)

    @property
    def max_host_vul_id(self):
        sql = 'select max(vul_id) as max_vul_id, max(id) as max_id from bd_host_vul_lib'
        result = self._dst_db.fetch_one(sql)
        max_vul_id = result.get('max_vul_id', 0)
        max_id = result.get('max_id', 0)
        return max_vul_id, max_id 

    def to_vul_level(self, cvss_base):
        level = 'I'
        cvss_base = float(cvss_base) 
        if not cvss_base:
            return level
        if cvss_base <=2:
            level = 'L' 
        elif 2 < cvss_base <=5:
            level = 'M'
        elif cvss_base > 5:
            level = 'H'
        return level

    def get_ref(self, oid):
        sql = 'SELECT ref_id AS ref FROM vt_refs WHERE vt_oid = %s AND type=%s' 
        result = self._src_db.fetch_one(sql, (oid, 'url'))
        if not result:
            return ''
        return result.get('ref', '')

    def generate_host_vul_data(self, nvts):
        max_vul_id, max_id = self.max_host_vul_id
        id = max_id + 1
        vul_id = max_vul_id + 1
        self._host_vul_ids = []
        oids = [nvt['oid'] for nvt in nvts]
        print('oids', len(oids))
        print('to find script files ....')
        script_files = self.find_script_files()
        print('found script files ', len(script_files))
        if not os.path.exists('oid_paths.json'):
            script_paths = self._executor.map(partial(self.find_script_path, script_files=script_files), oids)
            oid_paths = {}
            for oid, script_path in zip(oids, script_paths):
                oid_paths[oid] = script_path
            with open('oid_paths.json', 'wt') as f:
                json.dump(oid_paths, f)
            self._executor.shutdown()
        else:
            with open('oid_paths.json', 'rt') as f:
                oid_paths = json.load(f)
        for nvt in nvts:
            oid = nvt['oid']
            oids.append(oid)
            item = OrderedDict()
            item['id'] = id
            item['vul_id'] = vul_id
            item['oid'] = oid
            item['module_id'] = self._default_module_id
            item['family_id'] = self._default_family_id
            item['Family'] = 'unknow'
            item['Module'] = 'unknow'
            item['Description'] = nvt.pop('summary', '') + nvt.pop('insight', '') + nvt.pop('affected', '')
            item['Type'] = 0
            item['Ref'] = self.get_ref(oid)
            item['Enable'] = 1
            item['Filename'] = oid_paths[oid]
            # item['Filename'] = self.find_script_path(oid) 
            item['cve'] = nvt.get('cve', '')
            item['published_time'] = self.time2date(nvt.get('creation_time')) 
            item['updated_time'] = self.time2date(nvt.get('modification_time'))
            item['Category'] = nvt.get('category')
            item['vul_name'] = nvt.get('name', '') 
            item['vul_level'] = self.to_vul_level(nvt.get('cvss_base', ''))
            item['Solution'] = nvt.get('solution')
            item['CVEyear'] = self.get_cve_year(nvt.get('cve', ''))
            id += 1
            vul_id += 1
            self._host_vul_ids.append(vul_id)
            yield item

    def get_cve_year(self, nvt_cve):
        cve_year = 'NOCVE'
        nvt_cve = nvt_cve.strip()
        if not nvt_cve:
            return cve_year
        parts = nvt_cve.split(',')
        if parts:
            one_part = parts[0]
            cve_year, _ = one_part.rsplit('-', 1)
        return cve_year

    def time2date(self, time_stamp):
        if time_stamp:
            local_time = time.localtime(time_stamp)
            return time.strftime('%Y-%m-%d', local_time)
        else:
            return ''
        
    def get_fields_tuple_datas(self, datas):
        tuple_datas = []
        fields = []
        for item in datas:
            tuple_data = []
            for field, value in item.iteritems():
                tuple_data.append(value)
                if field not in fields:
                    fields.append(field)
            tuple_datas.append(tuple_data)
        return fields, tuple_datas
                
    def insert_into_host_vul(self, nvts):
        table_name = 'bd_host_vul_lib_2021'
        datas = self.generate_host_vul_data(nvts)
        fields, tuple_datas = self.get_fields_tuple_datas(datas)
        insert_host_ids = self.insert(table_name, fields, tuple_datas)
        self._insert_host_ids = insert_host_ids
        return self._insert_host_ids

    def insert(self, table_name, fields, datas):
        data_length = len(datas)
        step = 10000
        insert_ids = []
        for start in range(0, data_length, step):
            print(start, step)
            _datas = datas[start: start+step]
            _insert_ids = self._dst_db.insert(table_name, fields, _datas)
        
    def find_script_path(self, oid, script_files, topdir='/opt/gvm/var/lib/openvas/plugins/'):
        script_path = 'NOT FOUND'
        print('to find oid script path', oid)
        found_it = 0
        for file_path in script_files:
            with open(file_path, 'rb') as f:
                for line in f.readlines():
                    if re.search(oid, line):
                        file_path_parts = file_path.split(topdir)
                        script_path = os.path.join('scripts', file_path_parts[-1])
                        found_it = 1
                        found_index = script_files.index(file_path)
                        self._found_script_path.append(script_files.pop(found_index))
                        print('in found and to found', len(self._found_script_path), len(script_files))
                        break
            if found_it:
                break
        print('find_script_path', oid, script_path)
        return script_path
        

    def find_script_files(self, topdir='/opt/gvm/var/lib/openvas/plugins/'):
        all_files = []
        for root, dirs, files in os.walk(topdir):
            for _file in files:
                file_path = os.path.join(root, _file)
                all_files.append(file_path)
        return all_files

    def generate_host_p_selector_data(self, host_vul_ids):
        policy_id = self._default_policy_id
        family_id = self._default_family_id 
        return map(lambda vul_id: (policy_id, family_id, vul_id), host_vul_ids)
        
    def insert_into_host_p_selector(self):
        table_name = 'bd_host_policy_selectors_2021'
        fields = ['policy_id', 'family_id', 'vul_id']
        host_vul_ids  = self._host_vul_ids
        datas = self.generate_host_p_selector_data(host_vul_ids)
        self.insert(table_name, fields, datas)

    def trans(self):
        print('staring ....')
        print('query_nvts ....')
        nvts = self.query_nvts()
        print('insert_into_host_vul ...')
        self.insert_into_host_vul(nvts)
        print('insert_into_host_p_selector ....')
        self.insert_into_host_p_selector()
        self._src_db.close()
        self._dst_db.commit()
        self._dst_db.close()
        print('ended .....')
        
        
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
                self._cursor = self.conn.cursor(dictionary=True)
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

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()
        
        
if __name__ == '__main__':
    transfor = Transfor()
    transfor.trans()
