#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   data_trans.py
@Time    :   2021/06/02
@Author  :   levonwoo
@Version :   0.2
@Contact :   
@License :   (C)Copyright 2020-2021
@Desc    :   数据类型转换
'''

# here put the import lib

import datetime
from collections import OrderedDict

import numpy as np


def tuplelist_to_np(tuple_list: list, table_name: str):
    """
    将从clickhouse中SELECT的tuple_list数据转为结构化ndarray

    Parameters
    ----------
    tuple_list : list
        SELECT语句的到的tuple_list
    table_name : str
        表名,日线表为'stock_day',分钟表为'stock_min',竞价表为'call_auction',交易日历表为'trade_days'

    Returns
    -------
    ndarray
        返回结构化ndarray数组

    Raises
    ------
    NotImplementedError
        [description]
    """

    if table_name in ['stock_day', 'stock_min', 'daily', 'minute']:
        # 元组数组通过numpy结构化,注意数据长度code:8字符 date:10字符.可能存在问题
        return np.array(tuple_list,
                        dtype=[('datetime', 'object'), ('code', 'U8'),
                               ('open', 'f8'), ('close', 'f8'), ('high', 'f8'),
                               ('low', 'f8'), ('volume', 'f8'),
                               ('amount', 'f8'), ('avg', 'f8'),
                               ('high_limit', 'f8'), ('low_limit', 'f8'),
                               ('pre_close', 'f8'), ('date', 'U10'),
                               ('date_stamp', 'f8')])
    elif table_name in ['call_auction', 'auction']:
        return np.array(tuple_list,
                        dtype=[('datetime', 'object'), ('code', 'U8'),
                               ('close', 'f8'), ('volume', 'f8'),
                               ('amount', 'f8'), ('date', 'U10'),
                               ('date_stamp', 'f8')])
    elif table_name in ['trade_days']:
        return np.array(tuple_list,
                        dtype=[('datetime', 'object'), ('date', 'U10')])
    else:
        raise NotImplementedError


def pd_to_tuplelist(pd_data, frequency):
    """
    pandas.DataFrame数据转为tuple_list,每一行为tuple_list中的tuple
    遍历pandas.DataFrame每一列，赋值到字典，字典值转为二维列表，map(tuple, zip(*array))对二维列表转置

    Parameters
    ----------
    pd_data : pandas.DataFrame
        聚宽get_price函数返回结果
    frequency : str
        数据频率，已完成的有日线（daily），一分钟线(minute)。

    Returns
    -------
    list
        [description]

    Raises
    ------
    NotImplementedError
        [description]
    """
    if len(pd_data) == 0:
        return []

    base_keys_list = [
        'datetime', 'code', 'open', 'close', 'high', 'low', 'volume', 'amount',
        'avg', 'high_limit', 'low_limit', 'pre_close', 'date', 'date_stamp'
    ]
    if frequency in ['auction', 'call_auction']:
        base_keys_list = [
            'datetime', 'code', 'close', 'volume', 'amount', 'date',
            'date_stamp'
        ]
    elif frequency in ['trade_days']:
        base_keys_list = ['datetime', 'date']
    rawdata = OrderedDict().fromkeys(base_keys_list)

    if frequency in ['min', 'minute', '1min']:
        rawdata['datetime'] = list(
            map(
                lambda x: datetime.datetime.utcfromtimestamp(
                    x.astype(datetime.datetime) / pow(10, 9)),
                pd_data.index.values))
    elif frequency in ['d', 'day', '1day', 'daily']:
        # 时间+15小时表示收盘时间
        rawdata['datetime'] = list(
            map(
                lambda x: datetime.datetime.utcfromtimestamp(
                    x.astype(datetime.datetime) / pow(10, 9)) + datetime.
                              timedelta(hours=15), pd_data.index.values))
    elif frequency in ['auction', 'call_auction']:
        rawdata['datetime'] = list(
            map(
                lambda x: datetime.datetime.utcfromtimestamp(
                    x.astype(datetime.datetime) / pow(10, 9)),
                pd_data.index.values))
    elif frequency in ['trade_days']:
        pass
    else:
        raise NotImplementedError

    for filed, series in pd_data.iteritems():
        if filed in rawdata.keys():
            rawdata[filed] = series.tolist()
    #  list(rawdata.values())表示字典值转为列表
    #  map(tuple, zip(*array))表示二维数组转置
    return list(map(tuple, zip(*list(rawdata.values()))))
