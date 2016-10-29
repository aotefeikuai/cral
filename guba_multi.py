# -*- coding:utf-8 -*-

import time
from multiprocessing import Pool
from datetime import timedelta
from tornado import httpclient, gen, ioloop, queues
# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup #需安装
import requests
import urllib2
import lxml #需安装
import re
import urllib
import sys
import torndb #需安装
import xlrd #需安装

reload(sys)
sys.setdefaultencoding('utf-8')  # 将编码设置为utf-8

fname = "000300cons.xls"
bk = xlrd.open_workbook(fname) #读取成分股
shxrange = range(bk.nsheets)
try:
   sh = bk.sheet_by_name("20160613")
except:
   print "no sheet in %s named Sheet1" % fname
# 获取行数
nrows = sh.nrows

row_list = []
# 获取各行数据
for i in range(1, nrows):
   row_data = sh.cell_value(i,0)
   row_list.append(row_data)


for stock in row_list:  #将获取的成分股全部爬取一遍
    x=1
    db = torndb.Connection("localhost:3301", "HS_300", user="root", password="123456789", charset='utf8')
    cre='create table aqaaa'+stock+'(title text,author text,read_num int,comment int,date text,url text)'  #新建数据库列表
    print cre       #显示目前爬到哪支股票
    db.execute(cre)

    class AsySpider(object):
        """A simple class of asynchronous spider."""

        def __init__(self, urls, concurrency):
            urls.reverse()
            self.urls = urls
            self.concurrency = concurrency
            self._q = queues.Queue()
            self._fetching = set()
            self._fetched = set()

        def handle_page(self, url, html):
            filename = url.rsplit('/', 1)[1]
            soup = BeautifulSoup(html, "lxml")      #利用bs来解析页面
            article = soup.find_all(class_='articleh')

            for imga in article:   #进入页面爬取所需内容
                read = imga.find_all(class_='l1')   #通过beautifulsoup找到需要的标题作者等等内容
                read = read[0].string
                title = imga.find_all(class_='l3')
                title = title[0].string
                author = imga.find_all(class_='l4')
                author = author[0].string
                url = imga.find_all(class_='l3')
                pat = re.compile(r'href="([^"]*)"') #通过正则找取url 如换网站需更改此正则表达式
                h = pat.search(str(url[0]))
                url = h.group(1)
                url = 'http://guba.eastmoney.com' + url
                comment = imga.find_all(class_='l2')
                comment = comment[0].string
                date = imga.find_all(class_='l6')
                date = date[0].string
                print date
                sql = "INSERT INTO aqaaa" + stock + " (title,author,read_num,comment,date,url) VALUES (%s,%s,%s,%s,%s,%s) "
                db.insert(sql, title, author, read, comment, date, url)     #将爬取内容写入数据库中

        @gen.coroutine
        def get_page(self, url):      #此后部分为实现异步与多线程读取页面
            try:
                response = yield httpclient.AsyncHTTPClient().fetch(url)
                # print('######fetched %s' % url)
            except Exception as e:
                print('Exception: %s %s' % (e, url))
                raise gen.Return('')
            raise gen.Return(response.body)

        @gen.coroutine
        def _run(self):

            @gen.coroutine
            def fetch_url():
                current_url = yield self._q.get()
                try:
                    if current_url in self._fetching:
                        return

                    # print('fetching****** %s' % current_url)
                    self._fetching.add(current_url)
                    html = yield self.get_page(current_url)
                    self._fetched.add(current_url)

                    self.handle_page(current_url, html)

                    for i in range(self.concurrency):
                        if self.urls:
                            yield self._q.put(self.urls.pop())

                finally:
                    self._q.task_done()

            @gen.coroutine
            def worker():
                while True:
                    yield fetch_url()

            self._q.put(self.urls.pop())

            # Start workers, then wait for the work queue to be empty.
            for _ in range(self.concurrency):
                worker()
            yield self._q.join(timeout=timedelta(seconds=300000))
            assert self._fetching == self._fetched

        def run(self):
            io_loop = ioloop.IOLoop.current()
            io_loop.run_sync(self._run)


    def run_spider(beg, end):
        urls = []
        for page in range(beg, end):
            urls.append('http://guba.eastmoney.com/list,'+stock+',f_%s.html' % page)
        s = AsySpider(urls, 10)
        s.run()


    def main():
        _st = time.time()
        p = Pool()
        all_num = 100     #设置爬取网页的页数
        num = 4  # 使用cpu核心数目
        per_num, left = divmod(all_num, num)
        s = range(0, all_num, per_num)
        res = []
        for i in range(len(s) - 1):
            res.append((s[i], s[i + 1]))
        res.append((s[len(s) - 1], all_num))
        # print res

        for i in res:
            p.apply_async(run_spider, args=(i[0], i[1],))
        p.close()
        p.join()

        print time.time() - _st


    if __name__ == '__main__':
        main()
