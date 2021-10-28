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
    转换日期字符串为浮点数的时间戳

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
    date_stamp = time.mktime(time.strptime(datestr, '%Y-%m-%d'))
    return date_stamp


def datetime_convert_stamp(time_):
    """
    转换日期时间的字符串为浮点数的时间戳

    Parameters
    ----------
    time_ : str
        日期时间

    Returns
    -------
    float
        浮点数时间戳
    """
    datetimestr = pd.Timestamp(time_).strftime("%Y-%m-%d %H:%M:%S")
    date_stamp = time.mktime(time.strptime(datetimestr, '%Y-%m-%d %H:%M:%S'))
    return date_stamp


if __name__ == '__main__':
    pass
