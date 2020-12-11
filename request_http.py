#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import requests
import threading
import multiprocessing
import Queue
from concurrent.futures import ThreadPoolExecutor
from  functools import partial
import urllib3
import time
import random
import urlparse
import re


urllib3.disable_warnings()
DEFAULT_HOST_IP = '172.16.2.90'


class HostsManager(dict):
    def __init__(self, local_hosts_file='/etc/hosts'):
        super(self.__class__, self).__init__(self)
        self._local_hosts_file = local_hosts_file
        self.init_hosts()
    
    def init_hosts(self):
        with open(self._local_hosts_file, 'rt') as f:
            for line in f.readlines():
                parts = re.split(r'\s+', line.strip())
                if len(parts) >= 2:
                    ip = parts[0]
                    domains = parts[1:]
                    for domain in domains:
                        self[(ip, domain)] = 1

    def add_host(self, ip, domain):
        lines = []
        line = ip + ' ' +  domain + '\n'
        lines.append(line)
        with open(self._local_hosts_file, 'at') as f:
            self[(ip, domain)] = 1
            f.writelines(lines)


class RequestHttp(object): 
    def __init__(self,
        queue_max=20,
        thread_num=multiprocessing.cpu_count(),
        local_hosts_file ='/etc/hosts'):
        """
        doc:TODO 
        """
        self._task_queue = Queue.Queue(queue_max)
        self._res_queue = Queue.Queue(queue_max)
        self._thread_num = thread_num
        self._pool = ThreadPoolExecutor(self._thread_num)
        self._call_backs = []
        self._call_backs.append(self.my_print)
        self._running = False
        self._hosts_manager = HostsManager(local_hosts_file)

    def start(self):
        self._running = True
        self.work_thread_setup()
        self.res_thread_setup()

    def add_done_callback(self, call_back):
        if callable(call_back):
            self._call_backs.append(call_back)

    def my_print(self, future):
        result = future.result()
        print('my_print: ', result, result.url)

    def request(self, request_urls):
        if self._running:
            for url in request_urls:
                self.add_host(url)
                self._task_queue.put(url)
        while self._running:
            if self._task_queue.empty() and self._res_queue.empty():
                self.stop()

    def add_hosts(self, urls, host_ips=None):
        if host_ips is None:
            host_ips = [DEFAULT_HOST_IP] * len(urls)
        for url, host_ip in zip(urls, host_ips):
            self.add_host(url, host_ip)

    def add_host(self, url, host_ip=DEFAULT_HOST_IP):
        netloc = urlparse.urlparse(url).netloc
        if (host_ip, netloc) not in self._hosts_manager:
            print('add_host: ', host_ip, netloc)
            self._hosts_manager.add_host(host_ip, netloc)

    def res_thread_setup(self):
        self._res_thread = self._thread_setup(self.res_work)
    
    def work_thread_setup(self):
        self._work_thread = self._thread_setup(self.work)
    
    def _thread_setup(self, func, args=[], kwargs={}):
        _thread = threading.Thread(target=func, args=args, kwargs=kwargs)
        _thread.daemon = True
        _thread.start()
        return _thread

    def res_work(self):
        while self._running:
            url, future = self._res_queue.get()
            for cb in self._call_backs:
                try:
                    cb(future)
                except Exception as e:
                    print('res_work:%s %s' % (url, e))
            self._res_queue.task_done()

    def work(self):
        while self._running:
            try:
                if not self._task_queue.empty():
                    url = self._task_queue.get()
                    request_method = requests.get
                    try:
                        print('request url: %s' % url)
                        res_fut = self._pool.submit(request_method, url, verify=False, timeout=5)
                    except Exception as e:
                        print('submit request %s error: %s' % (url, e))
                    else:
                        self._task_queue.task_done()
                        self._res_queue.put((url, res_fut))
                else:
                    sleep_time = random.randint(1, 10)
                    print('no tasks, work thread go to sleep %s s...' % sleep_time)
                    time.sleep(sleep_time)
            except Exception as e:
                print(e)
                    
    def stop(self):
        print('stopping ...')
        self._running = False
        self._pool.shutdown()
        print('stopped ...')
        

    
if __name__ == '__main__':
    from itertools import cycle
    urls = ['http://www.gzszyy.com/?or 1=1', 'http://www.hit.edu.cn/?or%201=1', 'https://www.zgczwhbwg.com/?or%201=1',
        'https://www.hsziz.com/?or%201=1']
    urls = cycle(urls)
    request_http = RequestHttp(thread_num=10)
    request_http.start()
    request_http.request(urls)
