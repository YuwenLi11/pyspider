#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-05-29 16:35:13
# Project: Disease_real

from pyspider.libs.base_handler import *
import json
import pymysql
import datetime

class Handler(BaseHandler):
    crawl_config = {
    }
    
    
    def save_links_domain(self, url, category):
        db = pymysql.connect(host="localhost",user="root",password="", db="Disease", charset = "utf8")
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
        db = pymysql.connect("localhost", "root", "", "Disease", charset = "utf8")
        try:
            cursor = db.cursor()
            sql = 'INSERT INTO links(link_domain_id, url, name, l_add_date) VALUES(%d,"%s","%s",now())' % (qid, url, name)
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
        db = pymysql.connect("localhost", "root", "", "Disease", charset = "utf8")
        try:
            cursor = db.cursor()
            sql = 'INSERT INTO detail(link_id, Name, Title, ds_add_date, ds_last_crawl_date, content, ds_url) VALUES(%d,"%s","%s",now(),"%s","%s","%s")' % (d_id, name, title, crawl_date, content, url)
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
        
        
 
    @every(minutes=24 * 60)
    def on_start(self):
            url = self.base_url
            print (url)
            self.crawl(url, callback=self.index_page)
          
 
    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        for each in response.doc('.sortCont a').items():
            self.crawl(each.attr.href, callback=self.detail_page)
            
 
    #@config(priority=2)
    def detail_page(self, response): 
        name_0 = response.doc('dt').text()
        cat_name = name_0.strip() ### Name of Level 1 category (Disease Group)
        print(cat_name)
        url = response.url
        qid = self.save_links_domain(url, cat_name)
        for each in response.doc('dd > a').items():
            if each.text().find('已完成') != -1:
                self.crawl(each.attr.href, callback=self.more_detail_page, save={'cat_lv1': cat_name, 'qid': qid})
                
      
    def more_detail_page(self, response):
        name = response.doc('.case_name > label').text()
        cat_1 = response.save['cat_lv1'] ### Import disease group
        cat_name = name.strip()
        cat_lv2 = cat_name ### Name of Level 2 category (Disease Name)
        print(cat_lv2)
        qid = response.save['qid']
        url = response.url
        d_id = self.save_links(qid, url, cat_name)
        self.crawl(response.doc('.case_name > a').attr.href, callback=self.domain_page, save={'cat_lv2': cat_lv2, 'd_id':d_id})
        #brief = response.doc('.info-left').text()
        
        
    def domain_page(self, response):
        d_id = response.save['d_id']
        cat_lv2 = response.save['cat_lv2']
        for each in response.doc('div.chapter:nth-last-of-type(n + 3) a').items():
            if each.hasClass('nodata') != 1:
                self.crawl(each.attr.href, callback=self.info_page,save={'cat_lv2': cat_lv2, 'd_id':d_id})
         
        
    def info_page(self, response):
        title = response.doc('.current').text()
        print(title)
        d_id = response.save['d_id']
        name = response.save['cat_lv2']
        for each in response.doc('.dis_link a').items():
            self.crawl(each.attr.href, callback=self.moreinfo_page)
            
        for each in response.doc('.skin_knw .bd').items():
            crawl_date = str(datetime.datetime.now())
            content = response.doc('.skin_knw .bd').text()
            url = response.url
            self.save_detail(d_id, name, title, crawl_date, content, url)
            return {
                "url": response.url,
                "detail": response.doc('.skin_knw .bd').text()
            }
        
        
    def moreinfo_page(self, response):
        return {
            "url": response.url,
            "detail": response.doc('.skin_knw .bd').text()
}
        brief = response.doc('.skin_knw .bd').text()
        print(brief)
        
        return{}
        
        
    def on_result(self, result):
        #if not result or not result['title']:
            #return
        #sql = SQL()
        #sql.replace('example',**result)
        return
