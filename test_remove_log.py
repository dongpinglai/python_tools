#!/usr/bin/env python
# -*- encoding: utf-8 -*-


import xlrd
import pandas as pd

# book = xlrd.open_workbook("云防线网站列表(1).xls")
# sheet = book.sheet_by_index(0)
# for rx in range(sheet.nrows):
#     print(sheet.row(rx))

# df = pd.read_excel('云防线网站列表(1).xls')
# print(df)
import os
import shutil
import tempfile


count = 0
import json
log_dir = 'test_log_dir'
log_settings_path = 'remove_log.json'
json_f = open('remove_log.json', 'rt') 
with json_f:
    log_settings = json.load(json_f)
exclusive = log_settings['exclusive']
with open('domain.txt', 'rt') as f:
    for line in f.readlines():
        line = line.strip()
        if line not in exclusive:
            exclusive.append(line)
        _dir = os.path.join(log_dir, line)
        if not os.path.exists(_dir):
            os.mkdir(_dir)
        if count % 2 == 0:
            shutil.copy2('../cmake-3.19.2.tar.gz', _dir)
        elif count % 3 == 0:
            shutil.copy2('../kafka_2.11-0.10.2.0.tgz', _dir)
        count += 1
        tempfile.mkdtemp(dir=log_dir)
    print('mkdir count: %d' % count)

# with open(log_settings_path, 'w') as f:
#     json.dump(log_settings, f)

print('rewrite remove_log.json')
        

