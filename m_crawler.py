# -*- coding:utf-8 -*-
__author__ = 'kobe'

import threading
import time
import Queue
import hashlib
import re
from bs4 import BeautifulSoup
from selenium import webdriver

g_domain=''
g_count=0

class UrlNode(object):
    def __init__(self,url,depth):
        self.url=url
        self.depth=depth

class HtmlNode(object):
    def __init__(self,url,html,depth):
        self.url=url
        self.html=html
        self.depth=depth


class Fetcher(threading.Thread):
    def __init__(self,inqueue,outqueue):
        threading.Thread.__init__(self)
        self.inqueue=inqueue
        self.outqueue=outqueue
        self.stoped=False

    def run(self):
        stop_flag=1
        stop_flag_max=15
        wait_second=10
        while stop_flag<stop_flag_max:
            if self.inqueue.qsize()>0:
                node=self.inqueue.get()
                html=self.fecth(node.url)
                if html:
                    self.outqueue.put(HtmlNode(node.url,html,node.depth))
            else:
                stop_flag+=1
                print('%s待抓取URL为空，等待%s秒后再次获取\n'%(self.name,str(wait_second*stop_flag)))
                time.sleep(wait_second)
        print('线程%s退出\n'%self.name)
        self.stoped=True

    def fecth(self,url):
        try:
            drive=webdriver.PhantomJS(executable_path='/usr/bin/phantomjs')
            drive.set_page_load_timeout(60)
            drive.get(url)
            html=drive.page_source
        except Exception,e:
            print('线程：%s访问网址：%s 出错\n'%(self.name,url))
            return None
        finally:
            drive.close()
        return html


class Pool(object):
    def __init__(self,size):
        self.size = size
        self.html_queue = Queue.Queue()
        self.pool = []
        self.queues = []
        self.init_pool()

    def start(self):
        for t in self.pool:
            t.setDaemon(True)
            t.start()

    def init_pool(self):
        for i in range(self.size):
            q = Queue.Queue()
            t = Fetcher(q,self.html_queue)
            self.pool.append(t)
            self.queues.append(q)

    def put_jobs(self,jobs):
        stop_flag = 0
        count = len(jobs)
        while stop_flag < count:
            for q in self.queues:
                if stop_flag < count:
                    q.put(jobs[stop_flag])
                    stop_flag += 1
                else:
                    break

    def check(self):#检查线程是否停止
        stop_flag=0
        while stop_flag<self.size:
            for t in self.pool:
                if t.stoped:
                    stop_flag+=1
                    print('%s停止'%t.name)
                if not t.is_alive:
                    stop_flag+=1
                    print('%s死亡'%t.name)

def links_absolute(soup):
    global g_domain
    urls=[]
    for tag in soup.findAll('a', href=True):
        url=tag['href']
        if  url != '#' and url != '/' and url.find(g_domain)>-1:
            urls.append(url)
    r=[]
    for url in set(urls):
        r.append(url)
    return r

def save_downloaed(html_node):
    return True

def scheduler(size,url):
    global g_count
    complete_dict = {}
    f_value = hashlib.sha256(url).hexdigest()
    complete_dict[f_value] = url

    def update_url(queque):#更新待抓取URL
        global g_count

        while True:
            if queque.qsize()>0:
                node=queque.get()
                r_lock.acquire()
                g_count += 1
                r_lock.release()
                print node.url+' '+str(node.depth)+' count is '+str(g_count)
                save_downloaed(node)#保存已下载
                soup=BeautifulSoup(node.html,'lxml')
                links=links_absolute(soup)
                jobs=[]
                for l in links:
                    url=l.replace(' ','')
                    h_value = hashlib.sha256(url).hexdigest()
                    if h_value not in complete_dict:
                        complete_dict[h_value] = url
                        url_node=UrlNode(url,(node.depth+1))
                        jobs.append(url_node)
                if len(jobs):
                    pool.put_jobs(jobs)
            else:
                time.sleep(5)

    r_lock = threading.RLock()
    pool = Pool(size)
    html_queque = pool.html_queue

    print('开始抓取')
    pool.put_jobs([UrlNode(url,0)])
    pool.start()

    # update_url(html_queque)
    threading._start_new_thread(update_url,(html_queque,))
    pool.check()
    print('所有线程退出')

def getInput():
    url=raw_input('请输入网址：')
    size_str=raw_input('请输入线程数：')
    try:
        size=int(size_str)
    except Exception,e:
        print('错误：线程数应为数字')
        return None,None
    regex = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    if not regex.match(url):
        print('错误：网址不正确')
        return None,None
    return size,url

if __name__=='__main__':
    size,url=None,None
    while not (size and url):
        size,url=getInput()
    g_domain=url
    scheduler(size,url)





