#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-05-25 14:11:12
# Project: Disease_medlive

from pyspider.libs.base_handler import *
import json
import pymysql
import datetime

# from pyspider.database.mysql.mysqldb import SQL

## str(datetime.datetime.now()) # current time

DIR_PATH0 = '/Users/JasonZhang/Desktop/Test_Web/Disease'


class Handler(BaseHandler):
    crawl_config = {
    }

    def save_links_domain(self, url, category):
        db = pymysql.connect(host="localhost", user="root", password="", db="Disease", charset="utf8")
        try:
            cursor = db.cursor()
            sql = 'INSERT INTO links_domain(wpurl, category, ld_add_date) VALUES("%s","%s",now())' % (url, category)
            print(sql)
            cursor.execute(sql)
            db.commit()
            qid = cursor.lastrowid

            print(qid)
            return qid
        except:
            db.rollback()
            print('false')
        return 0

    def save_links(self, qid, url, name):
        db = pymysql.connect("localhost", "root", "", "Disease", charset="utf8")
        try:
            cursor = db.cursor()
            sql = 'INSERT INTO links(link_domain_id, url, name, l_add_date) VALUES(%d,"%s","%s",now())' % (
            qid, url, name)
            print(sql)
            cursor.execute(sql)
            d_id = cursor.lastrowid
            db.commit()
            print(d_id)
            return d_id
        except:
            db.rollback()
            print('false')
        return 0

    def save_detail(self, d_id, name, title, crawl_date, content, url):
        db = pymysql.connect("localhost", "root", "", "Disease", charset="utf8")
        try:
            cursor = db.cursor()
            sql = 'INSERT INTO detail(link_id, Name, Title, ds_add_date, ds_last_crawl_date, content, ds_url) VALUES(%d,"%s","%s",now(),"%s","%s","%s")' % (
            d_id, name, title, crawl_date, content, url)
            print(sql)
            cursor.execute(sql)
            id2 = cursor.lastrowid
            db.commit()
            print(id2)
        except:
            db.rollback()
        return 0

    def __init__(self):
        self.base_url = 'http://disease.medlive.cn/wiki/list/171'
        self.deal = Deal()

    @every(minutes=24 * 60)
    def on_start(self):
        # while self.page_num <= self.total_num:
        #   rang_url = 'drugCate2nd.do?treeCode=H' + str(self.page_num).zfill(2)  + '#H' +str(self.page_num).zfill(2)
        url = self.base_url
        print(url)
        self.crawl(url, callback=self.index_page)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        for each in response.doc('.sortCont a').items():
            self.crawl(each.attr.href, callback=self.detail_page)
            # each.attr.href = each.attr.href[:23] + each.attr.href[31:]
            # self.crawl(each.attr.href, callback=self.detail_page)
            # page_url = 'https:' + each
            # self.crawl(page_url, callback=self.detail_page)

    # @config(priority=2)
    def detail_page(self, response):
        name_0 = response.doc('dt').text()
        # dir_path = self.deal.mkDir(name_0)
        # dir_2 = dir_path
        # print(dir_path)
        cat_name = name_0.strip()  ### Name of Level 1 category (Disease Group)
        print(cat_name)

        url = response.url
        qid = self.save_links_domain(url, cat_name)

        for each in response.doc('dd > a').items():
            if each.text().find('已完成') != -1:
                self.crawl(each.attr.href, callback=self.more_detail_page, save={'cat_lv1': cat_name, 'qid': qid})

    def more_detail_page(self, response):
        name = response.doc('.case_name > label').text()
        # dir_path = self.deal.mkDir(name_0)
        cat_1 = response.save['cat_lv1']  ### Import disease group
        cat_name = name.strip()

        # cat_lv2 = cat_1 + ' > ' + cat_name
        cat_lv2 = cat_name  ### Name of Level 2 category (Disease Name)
        print(cat_lv2)

        qid = response.save['qid']
        url = response.url
        d_id = self.save_links(qid, url, cat_name)
        self.crawl(response.doc('.case_name > a').attr.href, callback=self.domain_page,
                   save={'cat_lv2': cat_lv2, 'd_id': d_id})
        # brief = response.doc('.info-left').text()

    def domain_page(self, response):
        d_id = response.save['d_id']
        cat_lv2 = response.save['cat_lv2']
        for each in response.doc('div.chapter:nth-last-of-type(n + 3) a').items():
            if each.hasClass('nodata') != 1:
                self.crawl(each.attr.href, callback=self.info_page, save={'cat_lv2': cat_lv2, 'd_id': d_id})

    def info_page(self, response):
        title = response.doc('.current').text()
        print(title)
        d_id = response.save['d_id']
        name = response.save['cat_lv2']

        # qid = self.save_detail(titles, content)
        for each in response.doc('.dis_link a').items():
            self.crawl(each.attr.href, callback=self.moreinfo_page)

        for each in response.doc('.skin_knw .bd').items():
            crawl_date = str(datetime.datetime.now())
            content = response.doc('.skin_knw .bd').text()
            url = response.url
            self.save_detail(d_id, name, title, crawl_date, content, url)
            # self.save_detail(qid, execute())
            return {
                "url": response.url,
                "detail": response.doc('.skin_knw .bd').text()
            }

    def moreinfo_page(self, response):
        # self.save_detail
        return {
            "url": response.url,
            "detail": response.doc('.skin_knw .bd').text()
        }
        brief = response.doc('.skin_knw .bd').text()
        print(brief)

        return {}

        # self.deal.saveBrief(brief, dir3, name)

    def on_result(self, result):
        # if not result or not result['title']:
        # return
        # sql = SQL()
        # sql.replace('example',**result)
        return