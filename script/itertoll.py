# -*- coding: utf-8 -*-
import itertools
def main():
    I = ['1','2']
    J = ['spades', 'hearts']
    K = ['3','4']

    list_all = (list(itertools.product(I,J,K)))
    list_result =[]


    for i in range(len(list_all)):
        dic = {}
        dic['first'] = list(list_all[i])[0]
        dic['second'] = list(list_all[i])[1]
        dic['third'] = list(list_all[i])[2]
        list_result.append(dic)

    print(list_result)

if __name__ == '__main__':
    main()
