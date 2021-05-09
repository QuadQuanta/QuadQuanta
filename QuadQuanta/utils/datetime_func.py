#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   datetime_func.py
@Time    :   2021/05/07
@Author  :   levonwolf
@Version :   0.1
@Contact :   
@License :   (C)Copyright 2020-2021
@Desc    :   None
'''
# here put the import lib
import time
import pandas as pd


def date_convert_stamp(date):
    """
    转换日期时间字符串为浮点数的时间戳

    Parameters
    ----------
    date : str
        日期时间
    Returns
    -------
    time
        [description]
    """
    datestr = pd.Timestamp(date).strftime("%Y-%m-%d")
    date = time.mktime(time.strptime(datestr, '%Y-%m-%d'))
    return date


def datetime_convert_stamp(time_):
    """
    explanation:
       转换日期时间的字符串为浮点数的时间戳

    params:
        * time_->
            含义: 日期时间
            类型: str
            参数支持: ['2018-01-01 00:00:00']

    return:
        time
    """
    if len(str(time_)) == 10:
        # yyyy-mm-dd格式
        return time.mktime(time.strptime(time_, '%Y-%m-%d'))
    elif len(str(time_)) == 16:
        # yyyy-mm-dd hh:mm格式
        return time.mktime(time.strptime(time_, '%Y-%m-%d %H:%M'))
    else:
        timestr = str(time_)[0:19]
        return time.mktime(time.strptime(timestr, '%Y-%m-%d %H:%M:%S'))


if __name__ == '__main__':
    pass