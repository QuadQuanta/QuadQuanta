#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   get_data.py
@Time    :   2021/05/27
@Author  :   levonwoo
@Version :   0.2
@Contact :   
@License :   (C)Copyright 2020-2021
@Desc    :   获取数据模块
'''

# here put the import lib
import time
import datetime

import jqdatasdk as jq
import pandas as pd
from QuadQuanta.config import config
from QuadQuanta.data.clickhouse_api import query_exist_max_datetime, query_clickhouse, query_N_clickhouse
from QuadQuanta.data.data_trans import pd_to_tuplelist, tuplelist_to_np
from QuadQuanta.utils.datetime_func import datetime_convert_stamp, is_valid_date
from QuadQuanta.const import *


def get_bars(code=None,
             start_time='1970-01-01',
             end_time='2100-01-01',
             frequency='daily',
             data_soure=DataSource.CLICKHOUSE,
             count=None,
             **kwargs):
    """
    通用K线获取接口，包括日线、分钟线、竞价。kwargs可选字段包括'client'：数据库连接,'format':返回数据类型

    Parameters
    ----------
    code : list or str, optional
        股票代码, by default None
    start_time : str, optional
        数据开始时间, by default '1970-01-01'
    end_time : str, optional
        数据结束时间, by default '2100-01-01'
    frequency : str, optional
        k线周期, by default 'daily'
    data_soure : str , optional
        数据源, by default DataSource.CLICKHOUSE
    count : int, optional
        时间序列个数, by default None
    kwargs: dict ,optional
        可选关键字有client:数据库连接,用于数据库后增量更新;format:指定返回值类型, np 或者 pd

    Returns
    -------
    pandas.DataFrame or numpy.ndarray 

    Raises
    ------
    NotImplementedError
        [description]
    """
    if data_soure == DataSource.JQDATA:
        return get_jq_bars(code, start_time, end_time, frequency, count,
                           **kwargs)
    elif data_soure == DataSource.CLICKHOUSE:
        return get_click_bars(code, start_time, end_time, frequency, count,
                              **kwargs)
    else:
        raise NotImplementedError


def get_jq_bars(code=None,
                start_time='1970-01-01',
                end_time='2100-01-01',
                frequency='daily',
                count=None,
                **kwargs):
    """
    从聚宽源获取起止时间内单个或多个聚宽股票并添加自定义字段

    Parameters
    ----------
    code : list or str, optional
        六位数字股票代码列表，如['000001'],['000001',...,'003039'],str会强制转换为list, by default None
    start_time : str, optional
        数据开始时间, by default '1970-01-01'
    end_time : str, optional
        数据结束时间, by default '2100-01-01'
    frequency : str, optional
        k线周期, by default 'daily'
    count : int, optional
        时间序列个数, by default None
    kwargs: dict ,optional
        可选关键字有client:数据库连接,用于数据库后增量更新;format:指定返回值类型, np 或者 pd

    Returns
    -------
    [type]
        [description]

    Raises
    ------
    ValueError
        [description]
    NotImplementedError
        [description]
    NotImplementedError
        [description]
    Exception
        [description]
    """

    jq.auth(config.jqusername, config.jqpasswd)
    if isinstance(code, str):
        code = list(map(str.strip, code.split(',')))
    if len(code) == 0:
        raise ValueError('股票代码格式错误')
    if is_valid_date(start_time) and is_valid_date(end_time):
        try:
            time.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            start_time = start_time + ' 09:00:00'
            end_time = end_time + ' 17:00:00'

    columns = [
        'time', 'code', 'open', 'close', 'high', 'low', 'volume', 'money',
        'avg', 'high_limit', 'low_limit', 'pre_close'
    ]

    if frequency in ['d', 'day', 'daily']:
        frequency = 'daily'
    elif frequency in ['min', 'minute']:
        frequency = 'minute'
    elif frequency in ['call_auction', 'auction']:
        frequency = 'call_auction'
        columns = ['time', 'code', 'close', 'volume', 'amount']
    else:
        raise NotImplementedError

    empty_pd = pd.concat([pd.DataFrame({k: [] for k in columns}), None, None])

    # 查询最大datetime
    if 'client' in kwargs.keys():
        exist_max_datetime = query_exist_max_datetime(code, frequency,
                                                      kwargs['client'])[0][0]
    else:
        exist_max_datetime = config.start_date
    # 从最大datetime的次日开始
    if str(exist_max_datetime) > config.start_date:  # 默认'2014-01-01'
        _start_time = str(exist_max_datetime + datetime.timedelta(hours=18))
    else:
        if start_time <= config.start_date:  # 默认'2014-01-01'
            start_time = config.start_date + ' 9:00:00'
        _start_time = start_time

    if _start_time <= end_time:
        if frequency in ['daily', 'minute']:
            if count:
                _start_time = None
                pd_data = jq.get_price(jq.normalize_code(code),
                                       start_date=_start_time,
                                       end_date=end_time,
                                       frequency=frequency,
                                       fields=[
                                           'open', 'close', 'high', 'low',
                                           'volume', 'money', 'avg',
                                           'high_limit', 'low_limit',
                                           'pre_close'
                                       ],
                                       skip_paused=True,
                                       fq='none',
                                       count=count,
                                       panel=False)
            else:
                pd_data = jq.get_price(jq.normalize_code(code),
                                       start_date=_start_time,
                                       end_date=end_time,
                                       frequency=frequency,
                                       fields=[
                                           'open', 'close', 'high', 'low',
                                           'volume', 'money', 'avg',
                                           'high_limit', 'low_limit',
                                           'pre_close'
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
        raise Exception('开始日期大于结束日期')

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
        if frequency == 'call_auction':
            pd_data = pd_data.assign(close=pd_data['current'])
        try:
            if kwargs['format'] in ['pd', 'pandas']:
                return pd_data
            elif kwargs['format'] in ['np', 'numpy']:
                return tuplelist_to_np(pd_to_tuplelist(pd_data, frequency),
                                       frequency)
        except:
            return pd_to_tuplelist(pd_data, frequency)


def get_click_bars(code=None,
                   start_time='1970-01-01',
                   end_time='2100-01-01',
                   frequency='daily',
                   count=None,
                   **kwargs):
    """
    从clickhouse数据库获取

    ----------
    code : list or str, optional
        六位数字股票代码列表，如['000001'],['000001',...,'003039'],str会强制转换为list, by default None
    start_time : str, optional
        数据开始时间, by default '1970-01-01'
    end_time : str, optional
        数据结束时间, by default '2100-01-01'
    frequency : str, optional
        k线周期, by default 'daily'
    count : int, optional
        时间序列个数, by default None
    kwargs: dict ,optional
        可选关键字有client:数据库连接,用于数据库后增量更新;format:指定返回值类型, np 或者 pd

    Returns
    -------
    [type]
        [description]
    """
    if count:
        res = query_N_clickhouse(count, code, end_time, frequency, **kwargs)
        try:
            if kwargs['format'] in ['pd', 'pandas']:
                return pd.DataFrame(res).set_index('datetime')
            else:
                return res
        except:
            return res
    else:
        res = query_clickhouse(code, start_time, end_time, frequency, **kwargs)
        try:
            if kwargs['format'] in ['pd', 'pandas']:
                return pd.DataFrame(res).set_index('datetime')
            else:
                return res
        except Exception as e:
            print(e)
            return res


def get_trade_days(start_time=None, end_time=None):
    if start_time or end_time:
        trade_days = jq.get_trade_days(start_time, end_time)
    else:
        trade_days = jq.get_all_trade_days()

    pd_data = pd.DataFrame(trade_days, columns=['datetime'])
    return pd_data.assign(date=pd_data['datetime'].apply(lambda x: str(x)))


if __name__ == '__main__':
    print(
        get_bars(['000001', '000002'],
                 '2020-01-01',
                 '2020-02-01',
                 'daily',
                 DataSource.CLICKHOUSE,
                 format='pd'))
