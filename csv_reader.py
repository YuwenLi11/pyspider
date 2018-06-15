# -*- coding: utf-8 -*-
import codecs
import pymysql
import json
import csv
def main():
    ####
    # each company's name is result[0] in the for loop
    ###
    f = open("/Users/liyuwen/Desktop/test.csv", "r")

    for line in f:
        line = f.readline()
        result = line.split(',')
        print(result[0])

if __name__ == '__main__':
    main()
