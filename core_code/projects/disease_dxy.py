from pyspider.libs.base_handler import *
import json
import pymysql
import datetime
import os
from urllib.parse import urlparse
import hashlib
import time
from bs4 import BeautifulSoup
import csv
import itertools

## str(datetime.datetime.now()) # current time

CONFIG_FILE_PATH = "/data/apps/crawler/envi-pyspider/config_file/disease_dxy.json"
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
        self.start_url = self.START_URL
        db = pymysql.connect(self.host, self.user, self.passwd, self.db, charset=self.dbcharset)
        # self.deal = Deal()

    def save_page(self, wsid, referer_wpid, wpurl, content, dir_path):
        db = pymysql.connect(self.host, self.user, self.passwd, self.db, charset=self.dbcharset)
        try:
            cursor = db.cursor()
            print('Connected')
            ### Get md5 hash value and content size
            m = hashlib.md5()
            # print(content)
            # if isinstance(content, unicode) is True:
            # print("unicode content")
            try:
                m.update(content)
                content = content.decode("utf-8")
            except:
                m.update(content.encode('utf-8'))
            wp_content_md5 = m.hexdigest()
            wp_content_size = len(content)
            print("all right")
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

    def save_details(self, wsid, wpid, details_names, details_values, details_types):
        db = pymysql.connect(self.host, self.user, self.passwd, self.db, charset=self.dbcharset)
        try:
            cursor = db.cursor()
            print('Connected')
            sql = 'INSERT INTO ' + self.detail_table + """(wsid, wpid, details_names, details_values, details_types, last_updated_date) VALUES(%d, %d,"%s",'%s',%d,now()) on duplicate key UPDATE details_values = CONCAT(details_values, "%s")""" % (
            wsid, wpid, details_names, details_values, details_types, details_values)
            # sql = 'INSERT INTO ' + self.detail_table + """(wsid, wpid, details_names, details_values, details_types, last_updated_date) VALUES(%d, %d,"%s",'%s',%d,now())""" % (wsid, wpid, details_names, details_values, details_types)
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

    def param_loop(self, params_path, params_cloumn, params_values_sets):
        return

    @every(minutes=24 * 60)
    def on_start(self):
        site_url = self.start_url
        print(site_url)
        wsid = self.save_site(site_url, self.DIR_PATH)
        self.crawl(site_url, callback=self.index_page, save={'wsid': wsid, 'qid': 0, 'plv': 0},
                   fetch_type=self.fetch_method)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        plv = response.save['plv']
        wsid = response.save['wsid']
        wpurl = response.url
        content = response.content
        print(response.encoding)
        referer_wpid = response.save['qid']
        plv = plv + 1
        print(plv)
        qid = self.save_page(wsid, referer_wpid, wpurl, content, self.DIR_PATH)
        print(qid)
        ### webpage not on detail level
        if plv < self.max_plv:
            # http method decided
            ### Method1: Submit form with POST method (not completed)
            if self.total_css[(plv - 1)]['method'] == "Post":
                post_data = {
                    "source": "中美天津史克制药有限公司"
                }
                self.crawl(response.url, save={'wsid': wsid, 'qid': referer_wpid, 'plv': plv},
                           fetch_type=self.fetch_method, method="POST", data=post_data, callback=self.index_page)
                print("post finished")


            ### Method2: Submit form with GET method
            elif self.total_css[(plv - 1)]['method_data'] != []:
                if self.total_css[(plv - 1)]['method_data'] != []:
                    print(self.total_css[(plv - 1)]['method_data'])
                    param_count = 0
                    param_names = []
                    param_pair = {}
                    ### Read parameters from json and csv module is removed

                    ### send xhr from page info
                    "https://dxy.com/view/i/disease/list?section_group_name=pifuxingbing&page_index=2"
                    for each_param in self.total_css[(plv - 1)]['method_data']:
                        param_count = param_count + 1
                        param_name = each_param["name"]
                        param_names.append(param_name)
                        if each_param["value"] == "CURRENT_WEBPAGE":
                            section_name = wpurl.split('/')[4]
                            print(section_name)
                            new_url = "https://dxy.com/view/i/disease/list?section_group_name=" + section_name
                            self.crawl(new_url, callback=self.xhr_json_page,
                                       save={"page_num": 1, "section_group_name": section_name, 'wsid': wsid,
                                             'qid': qid, 'plv': plv, "main_page_cookies": response.cookies})

                        time.sleep(0.05 * self.crawler_type)
                    return


            ### Method3: Direct URL picking and paging
            else:
                ### Direct url call
                print(response.json)
                this_level = self.total_css[(plv - 1)]
                for each_css in this_level['link']:
                    for each in response.doc(each_css).items():
                        self.crawl(each.attr.href, save={'wsid': wsid, 'qid': qid, 'plv': plv},
                                   fetch_type=self.fetch_method, allow_redirects=this_level['allow_redirects'],
                                   cookies=response.cookies, callback=self.index_page)
                    time.sleep(0.02 * self.crawler_type)

                ### Paging
                for each in response.doc(self.total_css[(plv - 1)]['paging']).items():
                    if each.text() == self.total_css[(plv - 1)]['paging_text']:
                        plv = plv - 1
                        self.crawl(each.attr.href, callback=self.index_page,
                                   save={'wsid': wsid, 'qid': referer_wpid, 'plv': plv}, fetch_type=self.fetch_method,
                                   allow_redirects=False)
                    time.sleep(0.05 * self.crawler_type)





        ### If the page level is on detail page
        elif plv == self.max_plv:
            detail_page_title_name = self.detail_page_title['name']
            detail_page_title_value = response.doc(self.detail_page_title['title_css']).text()
            if detail_page_title_value == "":
                pass
            else:
                detail_id = self.save_details(wsid, qid, detail_page_title_name, detail_page_title_value, 0)

            ### Paging
            for each in response.doc(self.detail_paging_css).items():
                if each.text() == self.detail_paging_text:
                    self.crawl(each.attr.href, callback=self.index_page,
                               save={'wsid': wsid, 'qid': referer_wpid, 'plv': (self.max_plv - 1)},
                               fetch_type=self.fetch_method, allow_redirects=False)
                    time.sleep(0.05 * self.crawler_type)
            # detail_name = response.doc('tr > .greys')
            # print(detail_name)
            # lis = response.doc('.content > table')
            # print(lis.each(lambda e: e))

            # for td in lis.find('td.green'):
            # print(td.text, td.getnext().text)

            ##########

            ### Extract Details

            ##########

            ### Exist data type to prevent duplicated items
            exist_data_type = 0

            ### Table_details_text
            for each_css in self.tables_css:
                table_css = each_css['table_css']
                rows_css = each_css['rows_css']
                columns_css = each_css['columns_css']
                # max_item_number = each_css['max_item_number']
                # count = 1;

                # for each_item in range(1,(max_item_number+1)):
                # pair_css = table_css + ' ' + rows_css + ':nth-of-type(' + str(each_item) + ') '
                # name_css = pair_css + columns_css + ':nth-of-type(1) '
                # value_css = pair_css + columns_css + ':nth-of-type(2) '

                # item_name = response.doc(name_css).text()
                # item_value = response.doc(value_css).text()

                ########
                if response.doc(table_css).html() != None:
                    for row in BeautifulSoup(response.doc(table_css).html(), "lxml")("tr"):
                        item_name = row("td")[0].text
                        item_value = row('td')[1].text

                        print(item_name)
                        print(item_value)

                        # count = count+1
                        if item_name == "":
                            break
                        else:
                            detail_id = self.save_details(wsid, qid, item_name, item_value, 0)
                            exist_data_type = 1
            ### If only one data type allowed, stop here
            if self.single_detail_data_type == "Yes":
                if exist_data_type == 1:
                    return

            ### Tables_one_value_json
            for each_css in self.json_tables_css:
                total_table = []
                table_css = each_css['table_css']
                rows_css = each_css['rows_css']
                columns_css = each_css['columns_css']
                title_css = each_css['title_css']
                for each in response.doc(table_css).items():
                    print('start')
                    # for vertical tables
                    table_data = [[cell.text for cell in row(columns_css)[0:2]]
                                  for row in BeautifulSoup(str(each), "lxml")(rows_css)]
                    total_table.append(dict(table_data))
                # print(json.dumps(dict(table_data), ensure_ascii=False))
                # print(table_data)
                table_value_json = json.dumps(total_table, ensure_ascii=False)
                # print(str(table_value_json))
                item_name = response.doc(title_css).text()
                detail_id = self.save_details(wsid, qid, item_name, table_value_json, 1)
                exist_data_type = 1
            ### If only one data type allowed, stop here
            if self.single_detail_data_type == "Yes":
                if exist_data_type == 1:
                    return

            ### TEST text content extract
            for each_content in self.detail_text_content:
                # print(each)
                # print("11111111111")
                # Read content area
                soup = ""
                for each in response.doc(each_content["content_css"]).items():
                    soup = BeautifulSoup(str(each))
                if soup == "":
                    continue
                valid_tag_exist = 0
                ### Tag for title element
                for each_title_tag in each_content["content_title_tag"]:

                    for h3 in soup.findAll(each_title_tag):
                        print(h3.text.strip())
                        item_name = h3.text.strip()
                        details_content_items = ""
                        title_tag_format = "<" + each_title_tag + ">"
                        while title_tag_format not in str(h3.next_sibling):
                            h3 = h3.next_sibling
                            if h3 is None:
                                break
                            # print("start")
                            # print(h3.string.strip())
                            try:
                                print("added")
                                details_content_items = details_content_items + h3.string.strip()

                            except:
                                pass
                        # print("round done")
                        print(details_content_items)
                        item_value = details_content_items
                        if item_name == "":
                            continue
                        else:
                            detail_id = self.save_details(wsid, qid, item_name, item_value, 0)

                        valid_tag_exist = 1
                ### if no title tag found, save all text content as "全部内容"
                if valid_tag_exist == 0:
                    item_name = "疾病简介"
                    item_value = each.text()
                    print("1111")
                    print(item_value)
                    if item_name == "":
                        continue
                    else:
                        detail_id = self.save_details(wsid, qid, item_name, item_value, 0)

            ### Name Value pair with class tag
            for each in response.doc(".disease-detail-card").items():
                for each_title in each(".disease-detail-card-title").items():
                    item_name = each_title.text()
                    item_value = each_title.nextAll(".disease-detail-card-deatil").text()
                    if item_name == "":
                        continue
                    else:
                        detail_id = self.save_details(wsid, qid, item_name, item_value, 0)

                        ### Extract information from json file and crawl links

    def xhr_json_page(self, response):
        section_name = response.save["section_group_name"]
        plv = response.save['plv']
        wsid = response.save['wsid']
        qid = response.save['qid']
        main_page_cookies = response.save['main_page_cookies']
        try:
            if float(response.json['data']['page_index']) < float(response.save['page_num']):
                return
            else:
                page_num = int(response.json['data']['page_index']) + 1
                json_data = response.json['data']['items']
                for each in json_data:
                    print(each)
                    new_url = "https://dxy.com/disease/" + str(each['id'])
                    self.crawl(new_url, callback=self.index_page, save={'wsid': wsid, 'qid': qid, 'plv': plv},
                               cookies=main_page_cookies)
        except:
            page_num = 1

        param_data = [{"section_group_name": section_name, "page_index": page_num}]
        for each in param_data:
            self.crawl("https://dxy.com/view/i/disease/list", params=each, callback=self.xhr_json_page,
                       save={"page_num": page_num, "section_group_name": section_name, 'wsid': wsid, 'qid': qid,
                             'plv': plv, "main_page_cookies": main_page_cookies})



