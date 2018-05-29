#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-05-25 14:11:20
# Project: disease

from pyspider.libs.base_handler import *


class Handler(BaseHandler):
    crawl_config = {
    }
    
#    def __initial__(self):
#        self.category = ''
#        self.disease_name = ''
        
        
    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('http://disease.medlive.cn/wiki/list/171', callback=self.index_page)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        for each in response.doc('ul.sortListA a').items():
            cate = response.doc('ul.sortListA a').text()
            self.crawl(each.attr.href, callback=self.list_page)
            
    def list_page(self, response):
        name = response.doc('.case_name > label').text()
        for each in response.doc('dd a').items():
            if each.text().find('已完成') != -1:
                self.crawl(each.attr.href, callback=self.general_page)
    
    def general_page(self, response):
        for each in response.doc('.case_name > a').items():
            self.crawl(each.attr.href, callback=self.disease_page)
    
    def disease_page(self, response):
        for each in response.doc('dd > a').items():
            if each.hasClass('nodata') != 1:
                self.crawl(each.attr.href, callback=self.detail_page)
            
    
    @config(priority=2)
    def detail_page(self, response):
        for each in response.doc('.dis_link a').items():
            self.crawl(each.attr.href, callback=self.moreinfo_page)
            
        for each in response.doc('.skin_knw .bd').items():
            return {
                "url": response.url,
                "detail": response.doc('.skin_knw .bd').text()
            }
        
    def moreinfo_page(self, response):
        return {
            "url": response.url,
            "detail": response.doc('.skin_knw .bd').text()
        }
        