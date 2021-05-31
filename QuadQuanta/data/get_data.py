#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   get_data.py
@Time    :   2021/05/27
@Author  :   levonwoo
@Version :   0.2
@Contact :   
@License :   (C)Copyright 2020-2021
@Desc    :   None
'''

# here put the import lib
import datetime

import jqdatasdk as jq
import pandas as pd
from QuadQuanta.config import config
from QuadQuanta.data.clickhouse_api import query_exist_max_datetime
from QuadQuanta.utils.datetime_func import datetime_convert_stamp


def get_jq_bars(code,
                start_time: str,
                end_time: str,
                client=None,
                frequency: str = 'daily'):
    """
    获取起止时间内单个或多个聚宽股票并添加自定义字段

    Parameters
    ----------
    code : list or str
        [description]
    start_time : str
        [description]
    end_time : str
        [description]
    client : [type]
        [description]
    frequency : str
        [description]

    Returns
    -------
    [type]
        [description]
    """
    if isinstance(code, str):
        code = list(map(str.strip, code.split(',')))

    if len(code) == 0:
        raise ValueError

    if frequency in ['d', 'day', 'daily']:
        frequency = 'daily'
    elif frequency in ['min', 'minute']:
        frequency = 'minute'
    elif frequency in ['call_auction', 'auction']:
        frequency = 'call_auction'
    else:
        raise NotImplementedError

    columns = [
        'time', 'code', 'open', 'close', 'high', 'low', 'volume', 'money',
        'avg', 'high_limit', 'low_limit', 'pre_close'
    ]
    if frequency == 'call_auction':
        columns = [
            'time',
            'code',
            'close',
            'volume',
            'amount',
        ]
    empty_pd = pd.concat([pd.DataFrame({k: [] for k in columns}), None, None])

    # 查询最大datetime
    if client:
        exist_max_datetime = query_exist_max_datetime(code, frequency,
                                                      client)[0][0]
    else:
        exist_max_datetime = config.start_date
    # 数据从2014年开始保存
    # TODO 用交易日历代替简单的日期加一
    if str(exist_max_datetime) > config.start_date:  # 默认'2014-01-01'
        _start_time = str(exist_max_datetime + datetime.timedelta(hours=18))
    else:
        if start_time <= config.start_date:  # 默认'2014-01-01'
            start_time = config.start_date + ' 9:00:00'
        _start_time = start_time

    if _start_time <= end_time:
        if frequency in ['daily', 'minute']:
            pd_data = jq.get_price(jq.normalize_code(code),
                                   start_date=_start_time,
                                   end_date=end_time,
                                   frequency=frequency,
                                   fields=[
                                       'open', 'close', 'high', 'low', 'volume',
                                       'money', 'avg', 'high_limit',
                                       'low_limit', 'pre_close'
                                   ],
                                   skip_paused=True,
                                   fq='none',
                                   count=None,
                                   panel=False)
            # TODO 有没有更优雅的方式
            pd_data['pre_close'].fillna(
                pd_data['open'],
                inplace=True)  # 新股上市首日分钟线没有pre_close数据，用当天开盘价填充

        elif frequency == 'call_auction':
            pd_data = jq.get_call_auction(jq.normalize_code(code),
                                          start_date=_start_time,
                                          end_date=end_time,
                                          fields=[
                                              'time',
                                              'current',
                                              'volume',
                                              'money',
                                          ])
        else:
            raise NotImplementedError
        pd_data = pd_data.dropna(axis=0, how='any')  # 删除包含NAN的行
    else:
        return empty_pd

    if len(pd_data) == 0:
        return empty_pd
    else:
        pd_data['datetime'] = pd_data['time']

        pd_data = pd_data.assign(
            amount=pd_data['money'],
            code=pd_data['code'].apply(lambda x: x[:6]),  # code列聚宽格式转为六位纯数字格式
            date=pd_data['datetime'].apply(lambda x: str(x)[0:10]),
            date_stamp=pd_data['datetime'].apply(
                lambda x: datetime_convert_stamp(x))).set_index('datetime',
                                                                drop=True,
                                                                inplace=False)
        if frequency in ['call_auction', 'auction']:
            pd_data = pd_data.assign(close=pd_data['current'])
        return pd_data


def get_trade_days(start_time=None, end_time=None):
    if start_time or end_time:
        trade_days = jq.get_trade_days(start_time, end_time)
    else:
        trade_days = jq.get_all_trade_days()

    pd_data = pd.DataFrame(trade_days, columns=['datetime'])
    return pd_data.assign(
        date=pd_data['datetime'].apply(lambda x: str(x)))
