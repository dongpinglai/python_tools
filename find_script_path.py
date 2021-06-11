#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import os
import mysql.connector
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import logging
import re
import json
import time
from threading import Thread
from Queue import Queue, Full, Empty
from functools import partial

logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
s_ch = logging.StreamHandler()
s_ch.setLevel(logging.DEBUG)
s_ch.setFormatter(formatter)
f_ch = logging.FileHandler('find_script_path.log')
f_ch.setFormatter(formatter)
logger.addHandler(s_ch)
logger.addHandler(f_ch)

# pool = ThreadPoolExecutor(128)
pool = ProcessPoolExecutor(256)

queue = Queue(100)
oid_paths = {}	

topdir='/home/bluedon/bdscan/bdhostscan/var/lib/bdscan/plugins/'

def find_script_files(topdir='/opt/gvm/var/lib/openvas/plugins/'):
        all_files = []
        for root, dirs, files in os.walk(topdir):
            for _file in files:
                file_path = os.path.join(root, _file)
                all_files.append(file_path)
        return all_files

def find_script_path(oid, script_files, topdir='/opt/gvm/var/lib/openvas/plugins/'):
        script_path = 'NOT FOUND'                                                                                                                                                       
        print('to find oid script path', oid)
        found_it = 0
        for file_path in script_files:
            with open(file_path, 'rt') as f:
                for line in f.readlines():
                    if re.search(oid, line):                        
                        file_path_parts = file_path.split(topdir)
                        script_path = os.path.join('scripts', file_path_parts[-1])
                        found_it = 1                                             
                        found_index = script_files.index(file_path)
                        break                                      
            if found_it:     
                break   
        print('find_script_path', oid, script_path)
        return script_path  

# def dump(not_found_oids, not_found_paths):
# 	for oid, path in zip(not_found_oids, not_found_paths):
# 		oid_paths[oid] = path
# 	print('to dumping')
# 	with open('not_found_oid_path.json', 'wt') as f:
# 		json.dump(oid_paths, f)
# 	print('dumped ....')

def dump():
	while 1:
		oid, path = queue.get()
		if oid is None and path is None:
			break
		oid_paths[oid] = path
		print('to dumping')
		with open('not_found_oid_path.json', 'wt') as f:
			json.dump(oid_paths, f)
		print('dumped ....')
		time.sleep(0.5)
	

cnx = mysql.connector.connect(user='root', password='fake_password', host='127.0.0.1', port=33066, database='security')

cursor = cnx.cursor(dictionary=True) 


def _run(oid, all_files, topdir=topdir):
	script_path = "NOT FOUND"
	try:
		script_path = find_script_path(oid, all_files, topdir)
	except Exception as e:
		logger.error('find %s script_path error:%s' % (oid, e))
	if script_path.startswith('script/'):
		try:
			cursor.execute('UPDATE %s SET filename = "%s" WHERE oid ="%s" ' % ('bd_host_vul_lib_2021', script_path, oid))
			cnx.commit()
		except Exception as e:
			logger.error('update %s filename %s failed: %s' % (oid, script_path, e))
		else:
			logger.info('update oid "%s" filename: %s' % (oid, script_path))
	queue.put((oid, script_path))
	return script_path


def find_not_found():
	cursor.execute('select oid from bd_host_vul_lib_2021 where filename = %s', ('NOT FOUND',))
	not_found_oids = map(lambda item: item['oid'], cursor.fetchall())
	all_files = find_script_files(topdir)
	# not_found_paths = map(_run, not_found_oids)
	dump_thread = Thread(target=dump)
	dump_thread.daemon = True
	dump_thread.start()
	not_found_paths = pool.map(partial(_run, all_files=all_files), not_found_oids)
	queue.put((None, None))
	dump_thread.join()
	pool.shutdown()
	# dump(not_found_oids, not_found_paths)



if __name__ == '__main__':
	find_not_found()
	



