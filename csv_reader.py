# -*- coding: utf-8 -*-
import csv
def main():
    ####
    # each company's name is result[0] in the for loop
    ###
    f = open("/Users/liyuwen/Desktop/pyspider/companies_data.csv", "r")
    reader = csv.reader(f)
    for line in reader:
        result = line[0]
        print(result)

if __name__ == '__main__':
    main()
