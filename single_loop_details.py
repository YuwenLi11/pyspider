from pyspider.libs.base_handler import *
import json
import pymysql
import datetime
import os
from urllib.parse import urlparse
import hashlib
import time
from bs4 import BeautifulSoup

## str(datetime.datetime.now()) # current time

CONFIG_FILE_PATH = "/Users/JasonZhang/Desktop/Test_Web/pharmnet_test.json"
HEADERS_FILE_PATH = "/Users/JasonZhang/Desktop/Test_Web/ws_header.json"
COOKIES_FILE_PATH = ""


class Handler(BaseHandler):
    myheader = json.load(open(HEADERS_FILE_PATH))

    crawl_config = {
        'headers': myheader
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
        self.max_plv = self.data['max_plv']
        self.total_css = self.data['total_css']
        self.tables_css = self.data['tables_css']
        self.json_tables_css = self.data['json_tables_css']
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
            dir_path = os.path.join(dir_path, self.CONTENT_FILES_WS_PREFIX + str(wsid), str(int(wpid / 100000000)),
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

    def save_details(self, wpid, details_names, details_values, details_types):
        db = pymysql.connect(self.host, self.user, self.passwd, self.db, charset=self.dbcharset)
        try:
            cursor = db.cursor()
            print('Connected')
            sql = 'INSERT INTO ' + self.detail_table + """(wpid, details_names, details_values, details_types, last_updated_date) VALUES(%d,"%s",'%s',%d,now())""" % (
            wpid, details_names, details_values, details_types)
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

    @every(minutes=24 * 60)
    def on_start(self):
        site_url = self.base_url
        print(site_url)
        wsid = self.save_site(site_url, self.DIR_PATH)
        self.crawl(site_url, callback=self.index_page, save={'wsid': wsid, 'qid': 0, 'plv': 0}, fetch_type='js')

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        plv = response.save['plv']
        wsid = response.save['wsid']
        wpurl = response.url
        content = response.doc.html()
        referer_wpid = response.save['qid']
        plv = plv + 1
        print(plv)
        qid = self.save_page(wsid, referer_wpid, wpurl, content, self.DIR_PATH)
        print(qid)
        if plv < self.max_plv:
            for each_css in self.total_css[(plv - 1)]['link']:
                for each in response.doc(each_css).items():
                    self.crawl(each.attr.href, callback=self.index_page, save={'wsid': wsid, 'qid': qid, 'plv': plv},
                               fetch_type='js')

            for each in response.doc(self.total_css[(plv - 1)]['paging']).items():
                plv = plv - 1
                qid = referer_wpid
                self.crawl(each.attr.href, callback=self.index_page, save={'wsid': wsid, 'qid': qid, 'plv': plv},
                           fetch_type='js')
        elif plv == self.max_plv:
            # detail_name = response.doc('tr > .greys')
            # print(detail_name)
            # lis = response.doc('.content > table')
            # print(lis.each(lambda e: e))

            # for td in lis.find('td.green'):
            # print(td.text, td.getnext().text)

            ### Table_details_text
            for each_css in self.tables_css:
                table_css = each_css['table_css']
                rows_css = each_css['rows_css']
                columns_css = each_css['columns_css']
                max_item_number = each_css['max_item_number']
                count = 1;
                for each_item in range(1, (max_item_number + 1)):
                    pair_css = table_css + ' ' + rows_css + ':nth-of-type(' + str(each_item) + ') '
                    name_css = pair_css + columns_css + ':nth-of-type(1) '
                    value_css = pair_css + columns_css + ':nth-of-type(2) '

                    item_name = response.doc(name_css).text()
                    item_value = response.doc(value_css).text()

                    print(item_name)
                    print(item_value)

                    count = count + 1
                    if item_name == "":
                        break
                    else:
                        detail_id = self.save_details(qid, item_name, item_value, 0)

            ### Tables_one_value_json
            for each_css in self.json_tables_css:
                total_table = []
                table_css = each_css['table_css']
                rows_css = each_css['rows_css']
                columns_css = each_css['columns_css']
                title_css = each_css['title_css']
                for each in response.doc('.gys dd > table').items():
                    print('start')
                    # for vertical tables
                    table_data = [[cell.text for cell in row(columns_css)[0:2]]
                                  for row in BeautifulSoup(str(each))(rows_css)]
                    total_table.append(dict(table_data))
                # print(json.dumps(dict(table_data), ensure_ascii=False))
                # print(table_data)
                table_value_json = json.dumps(total_table, ensure_ascii=False)
                # print(str(table_value_json))
                item_name = response.doc(title_css).text()
                detail_id = self.save_details(qid, item_name, table_value_json, 1)

            # for td in response.doc('div.content > table tr:nth-of-type(10) td:nth-of-type(2)').items():

            # print("emmm")
            # print(td.text())
            # print("stamp")
            # for td in response.doc('.content > table').find('td.greys'):
            # print(td.text, td.getnext().text)

        return