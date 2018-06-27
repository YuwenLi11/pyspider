from pyspider.libs.base_handler import *
import json
import pymysql
import datetime
import os
from urllib.parse import urlparse
import hashlib
import time
from bs4 import BeautifulSoup
import urllib.request

## str(datetime.datetime.now()) # current time

CONFIG_FILE_PATH = "/data/apps/crawler/envi-pyspider/config_file/mcd8_dictionary.json"
HEADERS_FILE_PATH = "/data/apps/crawler/envi-pyspider/config_file/ws_header.json"
COOKIES_FILE_PATH = "/data/apps/crawler/envi-pyspider/config_file/cookies.json"


class Handler(BaseHandler):
    myheader = json.load(open(HEADERS_FILE_PATH))
    mycookies = json.load(open(COOKIES_FILE_PATH))

    crawl_config = {
        'headers': myheader,
        # 'cookies': mycookies
    }

    def __init__(self):
        with open(CONFIG_FILE_PATH) as json_data_file:
            self.data = json.load(json_data_file)
        self.host = self.data['host']
        self.user = self.data['user']
        self.passwd = self.data['passwd']
        self.db = self.data['db']
        self.page_table = self.data['page_table']
        self.site_table = self.data['site_table']
        self.detail_table = self.data['detail_table']
        self.DIR_PATH = self.data['DIR_PATH']
        self.CONTENT_FILES_WS_PREFIX = self.data['CONTENT_FILES_WS_PREFIX']
        self.CONTENT_FILES_WP_PREFIX = self.data['CONTENT_FILES_WP_PREFIX']
        self.CONTENT_FILES_EXT = self.data['CONTENT_FILES_EXT']
        self.START_URL = self.data['START_URL']
        self.dbcharset = self.data['charset']
        self.fetch_method = self.data['fetch_method']
        self.max_plv = self.data['max_plv']
        self.total_css = self.data['total_css']
        self.single_detail_data_type = self.data['single_detail_data_type']
        self.detail_page_title = self.data['detail_page_title']
        self.detail_paging_css = self.data['detail_paging_css']
        self.detail_paging_text = self.data['detail_paging_text']
        self.tables_css = self.data['tables_css']
        self.json_tables_css = self.data['json_tables_css']
        self.detail_text_content = self.data['detail_text_content']
        self.crawler_type = self.data['BE_A_GOOD_CRAWLER']
        self.base_url = self.START_URL
        db = pymysql.connect(self.host, self.user, self.passwd, self.db, charset=self.dbcharset)
        # self.deal = Deal()

    def save_page(self, wsid, referer_wpid, wpurl, content, dir_path):
        db = pymysql.connect(self.host, self.user, self.passwd, self.db, charset=self.dbcharset)
        try:
            cursor = db.cursor()
            print('Connected')
            ### Get md5 hash value and content size
            m = hashlib.md5()
            m.update(content.encode('utf-8'))
            wp_content_md5 = m.hexdigest()
            wp_content_size = len(content)
            ### Save into MySQL databse
            sql = 'INSERT INTO ' + self.page_table + '(wsid, referer_wpid, wpurl, wp_content_md5, wp_content_size, wp_add_date) VALUES(%d,%d,"%s","%s",%d,now())' % (
                wsid, referer_wpid, wpurl, wp_content_md5, wp_content_size)
            print(sql)
            cursor.execute(sql)
            wpid = cursor.lastrowid

            print('Done writing into database')
            ### Create filename and directory path
            fullname = self.CONTENT_FILES_WP_PREFIX + str(wpid) + self.CONTENT_FILES_EXT
            dir_path = os.path.join(dir_path, self.CONTENT_FILES_WS_PREFIX + str(wsid),
                                    str(int(wpid / 100000000)).zfill(3),
                                    str(int((wpid % 100000000) / 1000000)).zfill(3),
                                    str(int((wpid % 1000000) / 10000)).zfill(3),
                                    str(int((wpid % 10000) / 100)).zfill(3))
            print(dir_path)
            exists = os.path.exists(dir_path)
            if not exists:
                os.makedirs(dir_path)
            ### Save html into directory
            file_path = os.path.join(dir_path, fullname)
            f = open(file_path, "w+")
            print('Successfully open')
            f.write(content)
            f.close()
            print('Save html')
            ### Update crawl status
            wp_status_crawl = 1
            sql2 = 'UPDATE ' + self.page_table + ' SET wp_status_crawl = %d, wp_last_crawl_date = now() where wpid = %d' % (
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
        db = pymysql.connect(self.host, self.user, self.passwd, self.db, charset=self.dbcharset)
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

            sql = 'INSERT INTO ' + self.site_table + '(host, protocol, ws_date_added) VALUES("%s",%d,now())' % (
            hostname, protocol)
            print(sql)
            cursor.execute(sql)
            wsid = cursor.lastrowid
            db.commit()
            return wsid
        except:
            db.rollback()
            print('Something wrong')
            return 0

    def save_details(self, wsid, wpid, details_names, details_values, details_types):
        db = pymysql.connect(self.host, self.user, self.passwd, self.db, charset=self.dbcharset)
        try:
            cursor = db.cursor()
            print('Connected')
            sql = 'INSERT INTO ' + self.detail_table + """(wsid, wpid, details_names, details_values, details_types, last_updated_date) VALUES(%d, %d,"%s",'%s',%d,now())""" % (
            wsid, wpid, details_names, details_values, details_types)
            print(sql)
            cursor.execute(sql)
            detail_id = cursor.lastrowid
            db.commit()
            print('Detail saved')
            return detail_id
        except:
            db.rollback()
            print('Something wrong')
            return 0
    
    def checkurl(self, url):
        try:
            page = urllib.request.urlopen(url)
            return 1
        except:
            return 0
        
    @every(minutes=24 * 60)
    def on_start(self):
        site_url = self.base_url
        print(site_url)
        wsid = self.save_site(site_url, self.DIR_PATH)
        self.crawl(site_url, callback=self.index_page, save={'wsid': wsid, 'qid': 0, 'plv': 0, 'up' : 1, 'down' : 1},
                   fetch_type=self.fetch_method)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        wsid = response.save['wsid']
        up = response.save['up']
        down = response.save['down']
        wpurl = response.url
        content = response.content
        print(response.encoding)
        referer_wpid = response.save['qid']
        qid = self.save_page(wsid, referer_wpid, wpurl, content, self.DIR_PATH)
        
        
        soup = BeautifulSoup(response.doc(self.detail_text_content['content_total']).html(), 'lxml')
        
        detail_page_title_name = self.detail_page_title['name']
        detail_page_title_value = soup.find(self.detail_page_title['title_css'], 
                                            class_ = self.detail_page_title['title_css_class']).text
        detail_id = self.save_details(wsid, qid, detail_page_title_name, detail_page_title_value, 0)
                            
        for index in range(len(soup.find_all(self.detail_text_content['content_css'],
                                             class_ = self.detail_text_content['content_css_class'][0]))):
            detail_page_name = soup.find_all(self.detail_text_content['content_css'],
                                             class_ = self.detail_text_content['content_css_class'][0])[index].text
            detail_page_value = soup.find_all(self.detail_text_content['content_css'],
                                              class_ = self.detail_text_content['content_css_class'][1])[index].text
            detail_id = self.save_details(wsid, qid, detail_page_name, detail_page_value, 0)  
        
            
        if up == 1:
            x = 5
            while x > 0:
                up_css = 'li:nth-of-type(' + str(x) + ') a'
                if self.checkurl(response.doc(up_css).attr.href) == 1:
                    self.crawl(response.doc(up_css).attr.href, callback=self.index_page,
                                save={'wsid': wsid, 'qid': qid,'up' : 1, 'down' : 0}, 
                                fetch_type=self.fetch_method, allow_redirects=False)
                    break
                else: 
                    x -= 1
            
        if down == 1:
            y = 7          
            while y < 12:
                down_css = 'li:nth-of-type(' + str(y) + ') a'  
                if self.checkurl(response.doc(down_css).attr.href) == 1:
                    self.crawl(response.doc(down_css).attr.href,callback=self.index_page,
                                save={'wsid': wsid, 'qid': qid,'up' : 0, 'down' : 1}, 
                                fetch_type=self.fetch_method, allow_redirects=False)
                    break
                else: 
                    y += 1


