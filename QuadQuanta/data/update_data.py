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

from QuadQuanta.config import config
from QuadQuanta.data.save_data import save_bars


def update_day_bar(frequency='daily', database='jqdata',start_time = config.start_date):
    today = datetime.date.today()
    start_time = start_time + ' 09:00:00'
    end_time = str(today) + ' 17:00:00'
    save_bars(start_time, end_time, frequency=frequency, database=database)


if __name__ == '__main__':
    # update_day_bar()
    update_day_bar('daily', 'test',start_time='2021-01-01')
    #
    # end_time = str(datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(days=15, hours=8))
    # save_all_jqdata(start_time,
    #                 end_time,
    #                 frequency='minute',
    #                 database='jqdata')
