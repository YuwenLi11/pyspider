from pyspider.libs.base_handler import *
import json
import pymysql
import datetime
import os
from urllib.parse import urlparse
from urllib.parse import quote
from urllib.request import urlopen
import hashlib
import time
from bs4 import BeautifulSoup
import csv
import itertools

## str(datetime.datetime.now()) # current time

CONFIG_FILE_PATH = "/data/apps/crawler/envi-pyspider/config_file/drug_medlive.json"
HEADERS_FILE_PATH = "/data/apps/crawler/envi-pyspider/config_file/ws_header.json"
COOKIES_FILE_PATH = "/data/apps/crawler/envi-pyspider/config_file/cookies.json"


class Handler(BaseHandler):
    myheader = json.load(open(HEADERS_FILE_PATH))
    mycookies = json.load(open(COOKIES_FILE_PATH))

    crawl_config = {
        'headers': myheader,
        # 'cookies': mycookies
        # 'proxy':'50.233.137.39:80'
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
            if wpurl[-4:] == ".rar" or wpurl[-4:] == ".zip" or wpurl[-4:] == ".pdf":
                wp_content_md5 = ""
            else:
                m = hashlib.md5()
                m.update(content.encode('utf-8'))
                wp_content_md5 = m.hexdigest()
            print("testtest")
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
            if wpurl[-4:] == ".rar" or wpurl[-4:] == ".zip" or wpurl[-4:] == ".pdf":
                # f = open(file_path, "wb")
                db.commit()
                print('non-html files')
                return wpid
            else:
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
        referer_wpid = response.save['qid']
        plv = plv + 1
        ### If this page meet the skippable condition
        if plv < self.max_plv:
            key_tag_css = self.total_css[(plv - 1)]['lv_key_tag_css']
            for each_tag_number in range(0, len(key_tag_css)):
                if self.total_css[(plv - 1)]['lv_key_tag_value'][each_tag_number] not in response.doc(
                        key_tag_css[each_tag_number]).text():
                    self.crawl(wpurl, callback=self.index_page,
                               save={'wsid': wsid, 'qid': referer_wpid, 'plv': plv, "url": wpurl},
                               fetch_type=self.fetch_method, allow_redirects=False, cookies=response.cookies)
                    return

        content = response.content
        ### Decide if .rar or .zip file
        if wpurl == "":
            url_org = response.save['url']
            wpurl = quote(url_org, safe='/:?=')
            # if wpurl[-4:] == ".rar" or wpurl[-4:] == ".zip":
            # url = urlopen(wpurl)
            # content = url.read()
        # print(response.content)
        referer_wpid = response.save['qid']
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
                    ### Read parameters from json and csv
                    for each_param in self.total_css[(plv - 1)]['method_data']:
                        param_count = param_count + 1
                        param_name = each_param["name"]
                        param_names.append(param_name)
                        if each_param["value"] != "":
                            param_value_set = [each_param["value"]]
                        elif each_param["value_path"] != "":
                            param_column = each_param["value_column"]
                            param_path = each_param["value_path"]
                            f = open(param_path, "r")
                            param_value_set = []
                            # print(f.read())
                            for line in csv.reader(f):
                                param_value = line[param_column - 1]
                                param_value_set.append(param_value)
                        else:
                            param_value_set = ['']
                        param_value_set_1 = []
                        print(param_value_set)
                        foo = "set_" + str(param_count)
                        # foo = "param_value_set_1"
                        print(foo)

                        param_pair[foo] = param_value_set
                        print(param_pair)

                    params_data = []
                    dic = {}
                    ### Two parameter dictonary sets generation
                    if param_count == 2:
                        list_all = (list(itertools.product(param_pair['set_1'], param_pair['set_2'])))

                        print(list_all)
                        for i in range(len(list_all)):

                            for count_n in range(0, param_count):
                                dic[param_names[count_n]] = list(list_all[i])[count_n]
                            # print(dic)
                            # dic['third'] = list(list_all[i])[2]
                            ##### BUG????
                            params_data.append(dic.copy())

                            # print(params_data)
                            # print("11111111111")
                    ### One parameter dictonary sets generation
                    else:
                        for i in range(len(list_all)):
                            dic[param_names[0]] = list(param_pair[i])[0]

                    ### Crawl for each set of parameters
                    for each in params_data:
                        # params_data = {
                        # "source": "中美天津史克制药有限公司"
                        # }
                        print(each)
                        self.crawl(response.url, save={'wsid': wsid, 'qid': referer_wpid, 'plv': plv},
                                   fetch_type=self.fetch_method, params=each, cookies=response.cookies,
                                   callback=self.index_page)
                        time.sleep(0.05 * self.crawler_type)


            ### Method3: Direct URL picking and paging
            else:
                ### Direct url call
                print(response.cookies)
                for each_css in self.total_css[(plv - 1)]['link']:
                    for each in response.doc(each_css).items():
                        if plv == 3:
                            new_url = "http://drugs.medlive.cn/drugref/html/" + each.attr.href.split("/")[6]
                            self.crawl(new_url, callback=self.index_page,
                                       save={'wsid': wsid, 'qid': qid, 'plv': plv, "url": new_url},
                                       fetch_type=self.fetch_method, allow_redirects=False, cookies=response.cookies)
                        else:

                            self.crawl(each.attr.href, callback=self.index_page,
                                       save={'wsid': wsid, 'qid': qid, 'plv': plv, "url": each.attr.href},
                                       fetch_type=self.fetch_method, allow_redirects=False, cookies=response.cookies)
                    time.sleep(0.02 * self.crawler_type)

                ### Paging
                for each in response.doc(self.total_css[(plv - 1)]['paging']).items():
                    if each.text() == self.total_css[(plv - 1)]['paging_text']:
                        plv = plv - 1
                        if plv == 2:
                            new_url = "http://drugs.medlive.cn/drugref/" + each.attr.href.split("/")[5]
                            self.crawl(new_url, callback=self.index_page,
                                       save={'wsid': wsid, 'qid': qid, 'plv': plv, "url": new_url},
                                       fetch_type=self.fetch_method, allow_redirects=False, cookies=response.cookies)
                        else:
                            self.crawl(each.attr.href, callback=self.index_page,
                                       save={'wsid': wsid, 'qid': referer_wpid, 'plv': plv},
                                       fetch_type=self.fetch_method, allow_redirects=True, cookies=response.cookies)
                    time.sleep(0.05 * self.crawler_type)

                # self.crawl("http://drugs.medlive.cn/drugref/drugCateLast.do?treeCode=&page=2", callback=self.index_page, save={'wsid': wsid, 'qid': qid, 'plv': plv, "url":""}, fetch_type=self.fetch_method, allow_redirects = False, cookies = response.cookies)


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
                    item_name = "全部内容"
                    item_value = each.text()
                    print("1111")
                    print(item_value)
                    if item_name == "":
                        continue
                    else:
                        detail_id = self.save_details(wsid, qid, item_name, item_value, 0)

            for each in response.doc(".info-left").items():
                for each_title in each(".title").items():
                    item_name = each_title.text()
                    item_value = each_title.nextAll(".more-infomation").text()
                    if item_name == "":
                        continue
                    else:
                        detail_id = self.save_details(wsid, qid, item_name, item_value, 0)

