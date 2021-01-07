#!/usr/bin/env python
# -*- encoding: utf-8 -*-


import shutil
import os
from os.path import getsize, isfile, isdir
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed


def getLogger(log_path='/tmp/remove_log.log'):
    # 创建Logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # 创建Handler

    # 终端Handler
    consoleHandler = logging.StreamHandler()
    consoleHandler.setLevel(logging.DEBUG)

    # 文件Handler
    fileHandler = logging.FileHandler(log_path, mode='a', encoding='utf-8')
    fileHandler.setLevel(logging.NOTSET)

    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    consoleHandler.setFormatter(formatter)
    fileHandler.setFormatter(formatter)

    # 添加到Logger中
    logger.addHandler(consoleHandler)
    logger.addHandler(fileHandler)
    return logger


logger = getLogger()


class LogDeleter(object):
    def __init__(self, limit_size=10, log_dir='/data/log/syslog/logs/bdwaf/access', exclusive=[], delete_top_file=False):
        """
        limit_size: int, 容量限制大小, 默认10MB
        log_dir: string, 日志目录
        exclusize: list, 禁止删除的文件名列表
        """
        self._limit_size = limit_size * 1024 * 1024
        self._log_dir=log_dir
        self._exclusive = exclusive
        self._exclusive_path = []
        self._delete_top_file = delete_top_file
        self._file_count = 0
        self._dir_count = 0
        if not os.path.exists(log_dir):
            raise Exception('日志目录 %s 不存在 ' % self._log_dir)
        for ex_ in exclusive:
            if ex_:
                ex_path = os.path.join(self._log_dir, ex_.strip())
                self._exclusive_path.append(ex_path)

    @property
    def all_dirs_files(self):
        dirs_files = []
        all_names = os.listdir(self._log_dir)
        for name in all_names:
            yield os.path.join(self._log_dir, name)

    @property
    def all_dirs(self):
        all_dirs_files = self.all_dirs_files
        for path in all_dirs_files:
            if isdir(path):
                yield path

    @property
    def all_files(self):
        all_dirs_files = self.all_dirs_files
        for path in all_dirs_files:
            if isfile(path):
                yield path
        
    def get_dir_size(self, dir):
        """
        return bytes(字节)
        """
        size = 0
        for root, dirs, files in os.walk(dir):
            size += sum([getsize(os.path.join(root, _file)) for _file in files])
            for _dir in dirs:
                size += self.get_dir_size(os.path.join(root, _dir))
        return size

    def _delete_file(self):
        all_files = self.all_files
        for _file in all_files:
            if _file in self._exclusive_path:
                continue 
            if getsize(_file) < self._limit_size:
                os.remove(_file)
                logger.info(u'删除文件: %s' % _file)
                self._file_count += 1
        return self._file_count

    def _delete_dir(self):
        all_dirs = self.all_dirs
        for _dir in all_dirs:
            if _dir in self._exclusive_path:
                continue
            dir_size = self.get_dir_size(_dir)
            if dir_size < self._limit_size:
                shutil.rmtree(_dir)
                logger.info(u'删除目录: %s' % _dir)
                self._dir_count += 1
        return self._dir_count

    def delete(self):
        logger.info(u'开始删除....')
        if self._delete_top_file:
            self._delete_file()
        self._delete_dir()
        logger.info(u'删除完成...., 删除%d个目录, 删除%d个文件' % (self._dir_count, self._file_count))

    def delete_with_multithread(self, n_worker=5):
        self._pool = ThreadPoolExecutor(n_worker)
        self._task_futures = []
        all_files = self.all_files
        all_dirs = self.all_dirs
        file_tasks = self.filter_tasks(all_files, 'file')
        dir_tasks = self.filter_tasks(all_dirs, 'dir')
        logger.info(u'开始删除....')
        self.submit_task(file_tasks)
        self.submit_task(dir_tasks)
        for finished_f in as_completed(self._task_futures):
            task, task_type = finished_f.result()
            if task_type == 'file':
                self._file_count += 1
            elif task_type == 'dir':
                self._dir_count += 1
        logger.info(u'删除完成...., 删除%d个目录, 删除%d个文件' % (self._dir_count, self._file_count))

    def submit_task(self, filter_tasks):
        for task, task_type in filter_tasks:
            f = self._pool.submit(self._delete_work, task, task_type)
            self._task_futures.append(f)

    def filter_tasks(self, tasks, task_type):
        if task_type == 'dir':
            _get_size = self.get_dir_size
        else:
            _get_size = getsize
        for task in tasks:
            if task in self._exclusive_path:
                continue
            if _get_size(task) >= self._limit_size:
                continue
            yield task, task_type

    def _delete_work(self, task, task_type):
        if task_type == 'file':
            os.remove(task)
            logger.info(u'删除文件: %s' % task)
        elif task_type == 'dir':
            shutil.rmtree(task)
            logger.info(u'删除目录: %s' % task)
        else:
            logger.error(u'task_type is wrong, do nothing!!!')
        return (task, task_type)


if __name__ == '__main__':
    import argparse
    import sys
    import json
    import time
    arg_parser = argparse.ArgumentParser(description='将删除日志目录中小于limit-size(默认为10M)的目录（文件），指定不用删除的目录（文件）除外')
    arg_parser.add_argument('--config-file', nargs='?', type=str, default='remove_log.json',
        help='配置文件路径。json文件格式, 默认为remove_log.json。配置文件参数说明: limit-size:容量大小，log-dir: 日志目录, exclusive： 指定不删除的目录（文件）名，delete-top-file: 是否删除日志目录下的文件.')
    args_space = arg_parser.parse_args(sys.argv[1:])
    cfg_file =  args_space.config_file
    if not cfg_file:
        cfg_file = 'remove_log.json'
    if not os.path.exists(cfg_file):
        raise Exception('配置文件%s不存在' % cfg_file)
    with open(cfg_file) as f:
        try:
            cfg = json.load(f)
        except ValueError as e:
            raise Exception('配置文件格式不对')
            
    limit_size = cfg.get('limit-size', 10)
    log_dir = cfg.get('log-dir', '/data/log/syslog/logs/bdwaf/access').encode('utf-8')
    exclusive = cfg.get('exclusive', [])
    delete_top_file = cfg.get('delete-top-file', False)
    multi = cfg.get('multi', False)
    kwargs = {
        'limit_size': limit_size,
        'log_dir': log_dir,
        'exclusive': exclusive,
        'delete_top_file': delete_top_file
    }
    log_deleter = LogDeleter(**kwargs)
    start_time = time.time()
    if multi:
        n_worker = cfg.get('n_worker', 1)
        log_deleter.delete_with_multithread(n_worker)
    else:
        log_deleter.delete()
    end_time = time.time()
    cost_time = end_time - start_time
    print('cost time: %f(ms)' % (cost_time))
