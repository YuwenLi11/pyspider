#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-05-30 19:42:18
# Project: Disease_real_v2

from pyspider.libs.base_handler import *
import json
import pymysql
import datetime

## str(datetime.datetime.now()) # current time

DIR_PATH0 = '/Users/JasonZhang/Desktop/Test_Web/Disease'


class Handler(BaseHandler):
    crawl_config = {
    }

    def save_html(self, content, dir_path, name):
        dir_path = dir_path.strip()
        file_name = dir_path + "/" + name + ".html"
        print(file_name)
        f = open(file_name, "w+")
        # f.write(content.encode('utf-8'))
        f.write(content)
        print('Save html')
        return file_name

    def save_detail(self, pageid, parentid, title, url, html_location, crawl_date):
        db = pymysql.connect("localhost", "root", "", "Disease", charset="utf8")
        try:
            cursor = db.cursor()
            print('Connected')
            if parentid != None:
                sql = 'INSERT INTO Pages(pageid, parentid, title, url, html_location, add_date, last_crawl_date) VALUES(%d,%d,"%s","%s","%s",now(),"%s")' % (
                pageid, parentid, title, url, html_location, crawl_date)
            else:
                sql = 'INSERT INTO Pages(pageid, parentid, title, url, html_location, add_date, last_crawl_date) VALUES(%d,null,"%s","%s","%s",now(),"%s")' % (
                pageid, title, url, html_location, crawl_date)
            print(sql)
            cursor.execute(sql)
            db.commit()
            return pageid
        except:
            db.rollback()
            print('Something wrong')
            return 0

    def __init__(self):
        self.base_url = 'http://disease.medlive.cn/wiki/list/171'
        self.deal = Deal()

    @every(minutes=24 * 60)
    def on_start(self):
        url = self.base_url
        print(url)
        self.crawl(url, callback=self.index_page)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        n = 1
        for each in response.doc('.sortCont a').items():
            n1 = n * 1000000
            self.crawl(each.attr.href, callback=self.detail_page, save={'pid': n1})
            n = n + 1

    # @config(priority=2)
    def detail_page(self, response):
        pageid = response.save['pid']
        print(pageid)
        parentid = None
        name_0 = response.doc('dt').text()
        title = name_0.strip()  ### Name of Level 1 category (Disease Group)
        print(title)
        url = response.url
        content = response.doc.html()
        html_location = self.save_html(content, DIR_PATH0, str(pageid).zfill(10))
        crawl_date = str(datetime.datetime.now())

        qid = self.save_detail(pageid, parentid, title, url, html_location, crawl_date)
        print(qid)
        n = 1
        for each in response.doc('dd > a').items():
            if each.text().find('已完成') != -1:
                qid2 = qid + n * 1000
                self.crawl(each.attr.href, callback=self.more_detail_page,
                           save={'cat_lv1': title, 'qid': qid, 'qid2': qid2})
                n = n + 1

    def more_detail_page(self, response):
        parentid = response.save['qid']
        pageid = response.save['qid2']
        name = response.doc('.case_name > label').text()
        cat_1 = response.save['cat_lv1']  ### Import disease group
        cat_name = name.strip()
        cat_lv2 = cat_1 + ' > ' + cat_name  ### Name of Level 2 category (Disease Name)
        print(cat_lv2)
        url = response.url
        content = response.doc.html()
        html_location = self.save_html(content, DIR_PATH0, str(pageid).zfill(10))
        crawl_date = str(datetime.datetime.now())

        qid_2 = self.save_detail(pageid, parentid, cat_lv2, url, html_location, crawl_date)
        print(pageid)
        self.crawl(response.doc('.case_name > a').attr.href, callback=self.domain_page,
                   save={'cat_lv2': cat_lv2, 'd_id': qid_2})

    def domain_page(self, response):
        d_id = response.save['d_id']
        cat_lv2 = response.save['cat_lv2']
        m = 1
        for x in range(1, 7):
            n = 'div.chapter:nth-of-type(' + str(x) + ') a'
            for each in response.doc(n).items():
                if each.hasClass('nodata') != 1:
                    # for each in response.doc('div.chapter:nth-last-of-type(n + 3) a').items():
                    # if each.hasClass('nodata') != 1:
                    title_css = 'div.chapter:nth-of-type(' + str(x) + ') dt'
                    title = response.doc(title_css).text()
                    title1 = title.strip()
                    info_id = d_id + m
                    self.crawl(each.attr.href, callback=self.info_page,
                               save={'cat_lv2': cat_lv2, 'info_id': info_id, 'p_id': d_id, 'title1': title})
                    m = m + 1

    def info_page(self, response):
        pageid = response.save['info_id']
        parentid = response.save['p_id']
        title1 = response.save['title1']
        title = response.doc('.current').text()
        title2 = title.strip()
        cat_lv2 = response.save['cat_lv2']
        cat_lv3 = cat_lv2 + ' > ' + title1 + ' > ' + title2
        print(cat_lv3)
        url = response.url
        content = response.doc.html()
        html_location = self.save_html(content, DIR_PATH0, str(pageid).zfill(10))
        crawl_date = str(datetime.datetime.now())

        qid_2 = self.save_detail(pageid, parentid, cat_lv3, url, html_location, crawl_date)
        print(pageid)

        for each in response.doc('.dis_link a').items():
            self.crawl(each.attr.href, callback=self.moreinfo_page)

        for each in response.doc('.skin_knw .bd').items():
            crawl_date = str(datetime.datetime.now())
            content = response.doc('.skin_knw .bd').text()
            url = response.url

            return {

            }

    def moreinfo_page(self, response):

        return {

        }

    def on_result(self, result):

        return