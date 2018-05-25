#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-05-21 16:21:59
# Project: divtest

from pyspider.libs.base_handler import *
import re
import random
import pymysql

class Handler(BaseHandler):
    crawl_config = {
    }
    def __init__(self):
        self.db = pymysql.connect(host = "localhost", user = "root", passwd = "2574256", db =  "pythontest", charset='utf8')

    def add_question(self, url, title):
        try:
            cursor = self.db.cursor()
            sql = 'insert into URL(LINK, TITLE) values ("%s","%s")' % (url, title);
            cursor.execute(sql)
            self.db.commit()
        except:
            self.db.rollback()
            
    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('http://www.baidu.com', callback=self.index_page)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        for each in response.doc('a[href^="http"]').items():
            self.crawl(each.attr.href, callback=self.detail_page,validate_cert=False)

    @config(priority=2)
    def detail_page(self, response):
        self.url = response.url
        self.title = response.doc('title').text()
        self.add_question(self.url, self.title)
        return {
            "url": response.url,
            "title": response.doc('title').text(),
        }

