# -*- coding: utf-8 -*-
import codecs
import pymysql
import json

def main():
    db = pymysql.connect("localhost", "root", "2574256", "pharmnet", charset="utf8")
    cursor = db.cursor()
    sql = 'SELECT details_values FROM webpages_details WHERE details_types = 1'
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        result = cursor.fetchall()
    except:
        print("Error: unable to fecth data")
    print(len(result))

    dir = "/Users/liyuwen/Desktop/test.csv"

    f = codecs.open(dir, "w+","utf-8")

    TitleRow = "公司名称, 联系电话, 联系传真, 地　　址, 企业相关\n"
    f.write(TitleRow)
    for i in range(len(result)):
        j_term1 = list(result)[i][0]
        j_form = json.loads(j_term1)
        for each in j_form:
            name = each["公司名称"].replace('  [产品目录]', '')
            tel = each["联系电话"]
            fax = each["联系传真"]
            add = each["地　　址"]
            rel = each["企业相关"]
            row = name + ',' + tel + ',' + fax + ',' + add + ',' + rel + '\n'
            f.write(row)

if __name__ == '__main__':
    main()


