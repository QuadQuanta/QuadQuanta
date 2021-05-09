#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   example.py
@Time    :   2021/05/08
@Author  :   levonwoo
@Version :   0.1
@Contact :   
@License :   (C)Copyright 2020-2021
@Desc    :   一个简易选股示例
'''

# here put the import lib
import numpy as np
import time
from QuadQuanta.data.clickhouse_api import query_clickhouse


def stragety_day(start_time, end_time):
    """
    打印出成交量大于昨日的2倍的股票

    Parameters
    ----------
    start_time : [type]
        [description]
    end_time : [type]
        [description]
    """
    _start = time.time()
    all_data = query_clickhouse(start_time=start_time,
                                end_time=end_time,
                                frequency='daily')
    _end = time.time()
    print(_end - _start)
    stock_list = np.unique(all_data['code'])
    symbol_list = []
    for code in stock_list:
        try:
            # 获取单个股票起止时间段内时序数据
            data = all_data[all_data['code'] == code]

            date_data = data['date']
            volume = data['volume']
            try:
                for i in range(1, len(data)):
                    if volume[i] > 2 * volume[i - 1]:
                        symbol_list.append(code)
                        # print("{},{}".format(date_data[i], code))
            except Exception as e:
                continue
        except Exception as e:
            print(e)
            continue
    _end = time.time()
    print(_end - _start)


if __name__ == '__main__':
    stragety_day(start_time='2020-04-24', end_time='2021-05-08')
