from pyspider.libs.base_handler import *
import json
import pymysql
import datetime
import os
from urllib.parse import urlparse
import hashlib

## str(datetime.datetime.now()) # current time

DIR_PATH = '/Users/JasonZhang/Desktop/Test_Web/home_dir'
CONTENT_FILES_WS_PREFIX = 'ws_'
CONTENT_FILES_WP_PREFIX = 'wp_'
CONTENT_FILES_EXT = '.html'
START_URL = 'http://disease.medlive.cn/wiki/list/171'
index_detail_css = {".sortCont a"}
index_paging_css = []
detail_more_detail_css = {"dd > a"}
detail_paging_css = []
more_detail_domain_css = {'.case_name > a'}
more_detail_paging_css = []


class Handler(BaseHandler):
    crawl_config = {
    }

    def save_html(self, content, dir_path, wpid, wsid):

        fullname = CONTENT_FILES_WP_PREFIX + str(wpid) + CONTENT_FILES_EXT
        file_path = os.path.join(dir_path, CONTENT_FILES_WS_PREFIX + str(wsid), str(int(wpid / 100000000)),
                                 str(int((wpid % 100000000) / 1000000)), str(int((wpid % 1000000) / 10000)),
                                 str(int((wpid % 10000) / 100)), fullname)

        print(file_path)
        f = open(file_path, "w+")
        # f.write(content.encode('utf-8'))
        f.write(content)
        f.close()
        print('Save html')
        return file_path

    def save_detail(self, wsid, referer_wpid, wpurl, content, dir_path):
        db = pymysql.connect("localhost", "root", "", "Disease", charset="utf8")
        try:
            cursor = db.cursor()
            print('Connected')
            ### Get md5 hash value and content size
            m = hashlib.md5()
            m.update(content.encode('utf-8'))
            wp_content_md5 = m.hexdigest()
            wp_content_size = len(content)
            ### Save into MySQL databse
            sql = 'INSERT INTO pages_test(wsid, referer_wpid, wpurl, wp_content_md5, wp_content_size, wp_add_date) VALUES(%d,%d,"%s","%s",%d,now())' % (
            wsid, referer_wpid, wpurl, wp_content_md5, wp_content_size)
            print(sql)
            cursor.execute(sql)
            wpid = cursor.lastrowid

            print('Done writing into database')
            ### Create filename and directory path
            fullname = CONTENT_FILES_WP_PREFIX + str(wpid) + CONTENT_FILES_EXT
            dir_path = os.path.join(dir_path, CONTENT_FILES_WS_PREFIX + str(wsid), str(int(wpid / 100000000)),
                                    str(int((wpid % 100000000) / 1000000)), str(int((wpid % 1000000) / 10000)),
                                    str(int((wpid % 10000) / 100)))
            print(dir_path)
            exists = os.path.exists(dir_path)
            if not exists:
                os.makedirs(dir_path)
            ### Save html into directory
            file_path = os.path.join(dir_path, fullname)
            f = open(file_path, "w+")
            print('Successfully open')
            # f.write(content.encode('utf-8'))
            f.write(content)
            f.close()
            print('Save html')
            ### Update crawl status
            wp_status_crawl = 1
            sql2 = "UPDATE pages_test SET wp_status_crawl = %d, wp_last_crawl_date = now() where wpid = %d" % (
            wp_status_crawl, wpid)
            cursor.execute(sql2)

            db.commit()
            print('Successfully update')
            return wpid
        except:
            db.rollback()
            print('Something wrong')
            return 0

    def save_site(self, wsurl, dir_path):
        db = pymysql.connect("localhost", "root", "", "Disease", charset="utf8")
        try:
            cursor = db.cursor()
            print('Connected')
            ### Get hostname and protocol type
            hostname = urlparse(wsurl).netloc
            print(hostname)
            http_type = urlparse(wsurl).scheme
            if http_type == 'http':
                protocol = 0
            elif http_type == 'https':
                protocol = 1
            else:
                protocol = 9
            print(protocol)

            sql = 'INSERT INTO sites_test(host, protocol, ws_date_added) VALUES("%s",%d,now())' % (hostname, protocol)
            print(sql)
            cursor.execute(sql)
            wsid = cursor.lastrowid
            db.commit()
            return wsid
        except:
            db.rollback()
            print('Something wrong')
            return 0

    def __init__(self):
        self.base_url = START_URL
        # self.deal = Deal()

    @every(minutes=24 * 60)
    def on_start(self):
        site_url = self.base_url
        print(site_url)
        wsid = self.save_site(site_url, DIR_PATH)
        self.crawl(site_url, callback=self.index_page, save={'wsid': wsid})

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        wsid = response.save['wsid']
        referer_wpid = 0
        wpurl = response.url
        content = response.doc.html()
        qid = self.save_detail(wsid, referer_wpid, wpurl, content, DIR_PATH)
        print(qid)

        for each_css in index_detail_css:
            for each in response.doc(each_css).items():
                self.crawl(each.attr.href, callback=self.detail_page, save={'wsid': wsid, 'qid': qid})

        for each in response.doc(index_paging_css).items():
            self.crawl(each.attr.href, callback=self.index_page, save={'wsid': wsid})

    # @config(priority=2)
    def detail_page(self, response):
        wsid = response.save['wsid']
        referer_wpid = response.save['qid']
        # name_0 = response.doc('dt').text()
        # title = name_0.strip() ### Name of Level 1 category (Disease Group)
        # print(title)
        wpurl = response.url
        content = response.doc.html()
        qid = self.save_detail(wsid, referer_wpid, wpurl, content, DIR_PATH)
        print(qid)

        for each_css in detail_more_detail_css:
            for each in response.doc(each_css).items():
                if each.text().find('已完成') != -1:
                    self.crawl(each.attr.href, callback=self.more_detail_page, save={'wsid': wsid, 'qid': qid})

        for each in response.doc(detail_paging_css).items():
            self.crawl(each.attr.href, callback=self.detail_page, save={'wsid': wsid})

    def more_detail_page(self, response):
        wsid = response.save['wsid']
        referer_wpid = response.save['qid']
        # name_0 = response.doc('dt').text()
        # title = name_0.strip() ### Name of Level 1 category (Disease Group)
        # print(title)
        wpurl = response.url
        content = response.doc.html()

        qid = self.save_detail(wsid, referer_wpid, wpurl, content, DIR_PATH)
        print(qid)

        for each_css in more_detail_domain_css:
            for each in response.doc(each_css).items():
                self.crawl(each.attr.href, callback=self.domain_page, save={'wsid': wsid, 'qid': qid})

        for each in response.doc(more_detail_paging_css).items():
            self.crawl(each.attr.href, callback=self.more_detail_page, save={'wsid': wsid})

    def domain_page(self, response):
        wsid = response.save['wsid']
        qid = response.save['qid']
        # cat_lv2 = response.save['cat_lv2']

        for x in range(1, 7):
            n = 'div.chapter:nth-of-type(' + str(x) + ') a'
            for each in response.doc(n).items():
                if each.hasClass('nodata') != 1:
                    self.crawl(each.attr.href, callback=self.info_page, save={'wsid': wsid, 'qid': qid})

        # for each in response.doc('div.chapter:nth-last-of-type(n + 3) a').items():
        # if each.hasClass('nodata') != 1:
        # title_css = 'div.chapter:nth-of-type(' + str(x) + ') dt'
        # title = response.doc(title_css).text()
        # title1 = title.strip()

        # self.crawl(each.attr.href, callback=self.info_page,save={'cat_lv2': cat_lv2, 'info_id':info_id, 'p_id':d_id, 'title1':title})

    def info_page(self, response):
        wsid = response.save['wsid']
        referer_wpid = response.save['qid']
        # title1 = response.save['title1']
        # title = response.doc('.current').text()
        # title2 = title.strip()
        # cat_lv2 = response.save['cat_lv2']
        # cat_lv3 = cat_lv2 + ' > ' + title1 + ' > ' + title2
        # print(cat_lv3)
        wpurl = response.url
        content = response.doc.html()
        # html_location = self.save_html(content, DIR_PATH0, str(pageid).zfill(10))
        # crawl_date = str(datetime.datetime.now())

        # qid = self.save_detail(pageid, parentid, cat_lv3, url, html_location, crawl_date)
        qid = self.save_detail(wsid, referer_wpid, wpurl, content, DIR_PATH)
        print(qid)

        for each in response.doc('.dis_link a').items():
            self.crawl(each.attr.href, callback=self.moreinfo_page)

        for each in response.doc('.skin_knw .bd').items():
            # crawl_date = str(datetime.datetime.now())
            # content = response.doc('.skin_knw .bd').text()
            # url = response.url

            return {

            }

    def moreinfo_page(self, response):

        return {

        }

    def on_result(self, result):

        return