#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   update.py
@Time    :   2021/05/08
@Author  :   levonwoo
@Version :   0.1
@Contact :   
@License :   (C)Copyright 2020-2021
@Desc    :   日线数据更新
'''

# here put the import lib
import datetime
from QuadQuanta.data import save_all_jqdata
from QuadQuanta.config import config

if __name__ == '__main__':
    today = datetime.date.today()
    start_time = config.start_date + ' 09:00:00'
    end_time = str(today) + ' 17:00:00'
    save_all_jqdata(start_time, end_time, frequency='daily', database='jqdata')

    # TODO 第一次存分钟数据注意关注聚宽流量，所取分钟数据大于剩余流量可能会发生未知错误
    # import time
    # start_ = time.time()
    # start_time = config.start_date + ' 09:00:00'
    # # end_time = str(today) + ' 17:00:00'
    #
    # end_time = str(datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(days=15, hours=8))
    # save_all_jqdata(start_time,
    #                 end_time,
    #                 frequency='minute',
    #                 database='jqdata')
    # end_ = time.time()
    # print(end_ - start_)