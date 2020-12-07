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


urllib3.disable_warnings()


class RequestHttp(object):
    def __init__(self, queue_max=20, thread_num=multiprocessing.cpu_count()):
        self._task_queue = Queue.Queue(queue_max)
        self._res_queue = Queue.Queue(queue_max)
        self._thread_num = thread_num
        self._pool = ThreadPoolExecutor(self._thread_num)
        self._call_backs = []
        self._call_backs.append(self.my_print)
        self._running = False

    def start(self):
        self._running = True
        self.work_thread_setup()
        self.res_thread_setup()

    def add_done_callback(self, call_back):
        if callable(call_back):
            self._call_backs.append(call_back)

    def my_print(self, future):
        result = future.result()
        print(result.content)
        
    def res_work(self):
        while self._running:
            future = self._res_queue.get()
            for cb in self._call_backs:
                cb(future)
            
    def res_thread_setup(self):
        self._res_thread = self._thread_setup(self.res_work)
    
    def request(self, request_urls):
        if self._running:
            for url in request_urls:
                self._task_queue.put(url)
        while self._running:
            if self._task_queue.empty() and self._res_queue.empty():
                self.stop()
            
    def work_thread_setup(self):
        self._work_thread = self._thread_setup(self.work)
    
    def _thread_setup(self, func, args=[], kwargs={}):
        _thread = threading.Thread(target=func, args=args, kwargs=kwargs)
        _thread.daemon = True
        _thread.start()
        return _thread

    def work(self):
        while self._running:
            try:
                print(self._task_queue.unfinished_tasks)
                if not self._task_queue.empty():
                    url = self._task_queue.get()
                    request_method = requests.get
                    try:
                        res_fut = self._pool.submit(request_method, url, verify=False, timeout=5)
                    except Exception as e:
                        print('submit error: %s' % e)
                    else:
                        self._task_queue.task_done()
                        self._res_queue.put(res_fut)
                else:
                    print('work thread sleep 1s...')
                    time.sleep(1)
            except Exception as e:
                print(e)
                    
    def stop(self):
        print('stopping ...')
        self._running = False
        self._pool.shutdown()
        print('stopped ...')
        

    
if __name__ == '__main__':
    from itertools import cycle
    urls = ['https://www.gzszyy.com/?or 1=1'] 
    urls = cycle(urls)
    request_http = RequestHttp(thread_num=10)
    request_http.start()
    request_http.request(urls)
