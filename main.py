from queue import Queue, Empty
from spider import TTMJSpider
import random
import threading
import logging
from hashlib import md5
from config_parser import ConfigParser
from DBTools import Mongo,log
import time
ORM = {
    'ttmj': TTMJSpider,
}


class ManagePool(object):
    '''
        主线程，调度工作线程，也可认为是管理线程
    '''
    def __init__(self, options, jobs):
        self.success = 0
        self.url_success = 0
        self.url_failure = 0
        self.failure = 0
        self.options = options
        self.jobs = jobs
        self.urls_queue = Queue()
        self.threads = []
        self.visited_urls = {}
        # list或dict是非线程安全的，因此这里需要加锁对他们进行保护
        self.visited_urls_lock = threading.Lock()
        for spider_name in jobs:
            spider_name = spider_name.strip()
            for i in options.get(spider_name + '_seed').values():
                self.visited_urls[md5(i.encode('utf-8')).hexdigest()] = i
                self.urls_queue.put((i, int(options.get('init', 'max_depth')), spider_name))
            for i in range(1,30):
                url = "https://www.ttmeiju.com/index.php/summary/index/p/{}.html".format(i)
                self.urls_queue.put((url, int(options.get('init', 'max_depth')), spider_name))
                self.visited_urls[md5(url.encode('utf-8')).hexdigest()] = url
        self.init_works()

    def init_works(self):
        name = random.sample(self.jobs, 1)[0]
        payload = {
            'headers': self.options.get('http_headers'),
            'retry_times': int(self.options.get('init', 'retry_times')),
            'retry_time': int(self.options.get('init', 'retry_time')),
            'post_data': self.options.get('post_' + name)
        }
        for i in range(int(self.options.get('init', 'max_thread'))):
            self.threads.append(WorkThread(self, name, payload))

    def threads_start(self):
        for i in self.threads:
            i.start()

    def wait_all_complete(self):
        for i in self.threads:
            if i.isAlive():
                i.join()

        log.logger.info("All things is done")

    def save_to_db(self):
        with Mongo() as db:
            try:
                db.visited_urls.insert(self.visited_urls)
            except db.options.errors.DuplicateKeyError:
                pass


    def is_busy(self):
        for i in self.threads:
            if i.isAlive() and i.running:
                return True
        return False


class WorkThread(threading.Thread):
    def __init__(self, manger, name, payload):
        threading.Thread.__init__(self)
        self.manger = manger
        self.running = False
        self.name = name
        self.payload = payload

    def run(self):
        while True:
            time.sleep(3)
            try:
                url, depth, name = self.manger.urls_queue.get(block=True, timeout=5)
                if not name.__eq__(self.name):
                    self.manger.urls_queue.put((url, depth, name))
                    log.logger.error('爬虫名字不符')
            except Empty:
                #logging
                if not self.manger.is_busy():
                    log.logger.error('QUEUE is not busy')
                    break
            else:
                self.running = True
                worker = ORM[self.name](url, depth, self.payload)
                time.sleep(random.randint(0, 5))
                if not ORM[self.name].cookie:
                    ORM[self.name].cookie = worker.get_session()
                    if not ORM[self.name].cookie:
                        exit(-1)
                links = worker.get_links()
                if links:
                    log.logger.info("from {} depth {} , get {} links".format(url, depth ,len(links)))
                    for i in links:
                        self.manger.url_success += 1
                        self.manger.visited_urls_lock.acquire()
                        if md5(i.encode('utf-8')).hexdigest() not in self.manger.visited_urls:
                            self.manger.urls_queue.put((i, depth - 1, name))
                            self.manger.visited_urls[md5(i.encode('utf-8')).hexdigest()] = i
                        self.manger.visited_urls_lock.release()

                elif links == 0:
                    pass
                else:
                    self.manger.url_failure += 1
                mj = worker.html_parser()
                if mj:
                    self.manger.success += 1
                    log.logger.info(mj[0]['mj_name'])
                    self.save_to_db(mj)
                else:
                    self.manger.url_failure += 1
                self.running = False
                self.manger.urls_queue.task_done()
                #logging
    def save_to_db(self, mj):
        with Mongo() as db:
            try:
                db.ttmj.insert(mj)
            except db.options.errors.DuplicateKeyError:
                pass
            except Exception:
                pass

def run():
    config = ConfigParser('config.ini')
    manger = ManagePool(config, ['ttmj',])
    manger.init_works()
    manger.threads_start()
    while True:
        def log_():
            log.logger.info("当前URL队列中还有：" + str(manger.urls_queue.qsize()) + "数据")
        t =threading.Timer(5, log_)
        t.start()
        if manger.urls_queue.empty():
            break
    manger.wait_all_complete()
    manger.save_to_db()

    log.logger.info("URL_成功数:" + str(manger.url_success))
    log.logger.info("URL_失败数" + str(manger.url_failure))
    log.logger.info("MJ_成功数" + str(manger.success))
    log.logger.info("MJ_失败数" + str(manger.failure))

if __name__ == "__main__":
    flag = 0
    j = 0
    while True:
        j += 1
        print("当前循环次数:" + str(j))
        sleep_time = 60 * 30
        if flag == 0:
            sleep_time = 1
            flag = 1
        t = threading.Timer(sleep_time, run)
        t.start()

