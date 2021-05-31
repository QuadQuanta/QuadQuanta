#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   clickhouse_api.py
@Time    :   2021/05/07
@Author  :   levonwoo
@Version :   0.1
@Contact :   
@License :   (C)Copyright 2020-2021
@Desc    :   None
'''

import asyncio
import time

# here put the import lib
import numpy as np
from clickhouse_driver import Client
from QuadQuanta.config import config
from QuadQuanta.utils.common import removeDuplicates


def create_clickhouse_database(database: str = None,
                               client=Client(host='127.0.0.1')):
    create_database_sql = 'CREATE DATABASE IF NOT EXISTS %s' % database
    client.execute(create_database_sql)


def create_clickhouse_table(type: str = None,
                            client: Client = Client(host='127.0.0.1',
                                                    database='jqdata')):
    """
    创建clickhouse表

    Parameters
    ----------
    client : clickhouse_driver.Client
        和clickhouse的客户端连接
    type : str
        存储表类型,已完成的有日线（daily）,一分钟线(minute),开盘竞价,交易日历

    Raises
    ------
    NotImplementedError
        [description]
    """
    # TODO 用枚举类型重构
    if type in ['min', 'minute', '1min']:
        create_table_sql = 'CREATE TABLE IF NOT EXISTS stock_min (datetime DateTime,code String, open Float32, \
                           close Float32,high Float32,low Float32, volume Float64, amount Float64,avg Float32,  \
                           high_limit Float32,low_limit Float32,pre_close Float32, date String, date_stamp Float64) \
                            ENGINE = MergeTree() ORDER BY (datetime, code)'

    elif type in ['d', 'day', '1day', 'daily']:
        create_table_sql = 'CREATE TABLE IF NOT EXISTS stock_day (datetime DateTime,code String, open Float32, \
                           close Float32,high Float32,low Float32, volume Float64, amount Float64,avg Float32,  \
                           high_limit Float32,low_limit Float32,pre_close Float32, date String, date_stamp Float64) \
                            ENGINE = MergeTree() ORDER BY (datetime, code)'

    elif type in ['auction', 'call_auction']:
        create_table_sql = 'CREATE TABLE IF NOT EXISTS call_auction (datetime DateTime,code String, close Float32, \
                           volume Float64, amount Float64, date String, date_stamp Float64) ENGINE = MergeTree() ORDER BY (datetime, code)'

    elif type in ['trade_days']:
        create_table_sql = 'CREATE TABLE IF NOT EXISTS trade_days (datetime Date, date String) ENGINE = MergeTree() ORDER BY (datetime)'
    else:
        raise NotImplementedError
    client.execute(create_table_sql)


def insert_to_clickhouse(data,
                         type,
                         client: Client = Client(host='127.0.0.1',
                                                 database='jqdata')):
    """
    将数据插入clickhouse数据库

    Parameters
    ----------
    data : tuple_list
        元组数组类型数据,每个元组为一行
    client : clickhouse_driver.Client
        和clickhouse的客户端连接
    type : str
        存储表类型,已完成的有日线（daily）,一分钟线(minute),开盘竞价,交易日历

    Raises
    ------
    NotImplementedError
        [description]
    """
    if type in ['min', 'minute', '1min']:
        insert_data_sql = 'INSERT INTO stock_min (datetime, code, open, close, high, low, volume, amount,\
             avg, high_limit, low_limit, pre_close, date, date_stamp) VALUES'

    elif type in ['d', 'day', '1day', 'daily']:
        insert_data_sql = 'INSERT INTO stock_day (datetime, code, open, close, high, low, volume, amount,\
             avg, high_limit, low_limit, pre_close, date, date_stamp) VALUES'

    elif type in ['auction', 'call_auction']:
        insert_data_sql = 'INSERT INTO call_auction (datetime, code, close, volume, amount, date, date_stamp) VALUES'
    elif type in ['trade_days']:
        insert_data_sql = 'INSERT INTO trade_days (datetime, date) VALUES'
    else:
        raise NotImplementedError
    client.execute(insert_data_sql, data, types_check=True)


def query_exist_max_datetime(code=None,
                             type='daily',
                             client: Client = Client(host='127.0.0.1',
                                                     database='jqdata')):
    """[summary]

    Parameters
    ----------
    code : list
        六位数股票代码列表,如['000001'], ['000001',...,'689009']
    type : str
        数据频率,已完成的有日线（daily）,一分钟线(minute)。
    client : clickhouse_driver.Client
        和clickhouse的客户端连接

    Returns
    -------
    tuple_list
        返回结果为tuple_list格式,取tuple_list[0][0]为
    """
    if isinstance(code, str):
        code = list(map(str.strip, code.split(',')))
    if type in ['min', 'minute', '1min']:
        max_datetime_sql = 'SELECT max(datetime) from stock_min WHERE `code` IN %(code)s'
    elif type in ['d', 'day', '1day', 'daily']:
        max_datetime_sql = 'SELECT max(datetime) from stock_day WHERE `code` IN %(code)s'
    elif type in ['call_auction', 'auction']:
        max_datetime_sql = 'SELECT max(datetime) from call_auction WHERE `code` IN %(code)s'
    elif type in ['trade_days']:
        max_datetime_sql = 'SELECT max(datetime) from trade_days'
        # TODO return
    else:
        raise NotImplementedError

    return client.execute(max_datetime_sql, {'code': code})


def tuplelist_to_np(tuple_list: list, table_name: str):
    """
    将从clickhouse中SELECT的tuple_list数据转为结构化ndarray

    Parameters
    ----------
    tuple_list : list
        SELECT语句的到的tuple_list
    table_name : str
        表名,日线表为'stock_day',分钟表为'stock_min'

    Returns
    -------
    ndarray
        返回结构化ndarray数组

    Raises
    ------
    NotImplementedError
        [description]
    """

    if table_name in ['stock_day', 'stock_min']:
        # 元组数组通过numpy结构化,注意数据长度code:8字符 date:10字符.可能存在问题
        return np.array(tuple_list,
                        dtype=[('datetime', 'object'), ('code', 'U8'),
                               ('open', 'f8'), ('close', 'f8'), ('high', 'f8'),
                               ('low', 'f8'), ('volume', 'f8'),
                               ('amount', 'f8'), ('avg', 'f8'),
                               ('high_limit', 'f8'), ('low_limit', 'f8'),
                               ('pre_close', 'f8'), ('date', 'U10'),
                               ('date_stamp', 'f8')])
    elif table_name in ['call_auction']:
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


def query_clickhouse(code: list = None,
                     start_time: str = None,
                     end_time: str = None,
                     frequency='daily',
                     database='jqdata') -> np.ndarray:
    """[summary]

    Parameters
    ----------
    code : list, optional
        [description], by default None
    start_time : str, optional
        [description], by default None
    end_time : str, optional
        [description], by default None
    frequency : str, optional
        [description], by default 'daily'
    database : str, optional
        数据库名,默认为聚宽数据, by default 'jqdata'

    Returns
    -------
    np.ndarray
        [description]

    Raises
    ------
    NotImplementedError
        [description]
    NotImplementedError
        [description]
    """
    if code:
        # TODO 判断code是否有效
        if isinstance(code, str):
            code = list(map(str.strip, code.split(',')))
    if start_time and end_time:
        try:
            time.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            start_time = start_time + ' 09:00:00'
            end_time = end_time + ' 17:00:00'
        #  判断日期合法
        try:
            time.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            time.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            if start_time > end_time:
                raise ValueError
        except ValueError:
            print("输入日期不合法或开始时间大于结束时间")

    if frequency in ['day', 'daily', 'd']:
        table_name = 'stock_day'
    elif frequency in ['min', 'minute', '1min']:
        table_name = 'stock_min'
    elif frequency in ['auction', 'call_auction']:
        table_name = 'call_auction'
    else:
        raise NotImplementedError

    client = Client(host=config.clickhouse_IP, database=database)
    create_clickhouse_table(frequency, client)

    if start_time and end_time and code:
        sql = "SELECT x.* FROM %s x" % table_name + " WHERE `datetime` >= %(start_time)s \
                        AND `datetime` <= %(end_time)s AND `code` IN %(code)s ORDER BY (`datetime`, `code`)"

        # 查询,返回数据类型为元组数组
        res_tuple_list = client.execute(sql, {
            'start_time': start_time,
            'end_time': end_time,
            'code': code
        })
    elif start_time and end_time:
        sql = "SELECT x.* FROM %s x" % table_name + " WHERE `datetime` >= %(start_time)s \
                        AND `datetime` <= %(end_time)s ORDER BY (`datetime`, `code`)"

        res_tuple_list = client.execute(sql, {
            'start_time': start_time,
            'end_time': end_time
        })
    elif code:
        # 注意WHERE前的空格
        sql = "SELECT x.* FROM %s x" % table_name + " WHERE `code` IN %(code)s ORDER BY (`datetime`, `code`)"
        res_tuple_list = client.execute(sql, {'code': code})
    else:
        sql = "SELECT x.* FROM %s x" % table_name
        res_tuple_list = client.execute(sql)
    #  TODO clickhouse分片

    # TODO 判读tuple_list是否有序
    # TODO LIMIT 取时间序列
    # 默认有序条件下删除res_tuple_list重复数据
    res_tuple_list = removeDuplicates(res_tuple_list)
    # 元组数组通过numpy结构化,注意数据长度code:8字符 date:10字符.可能存在问题

    return tuplelist_to_np(res_tuple_list, table_name)


def query_N_clickhouse(limit: int,
                       code: list = None,
                       end_time: str = None,
                       frequency='daily',
                       database='jqdata') -> np.ndarray:
    """
    取N个时间序列

    Parameters
    ----------
    code : list, optional
        [description], by default None
    start_time : str, optional
        [description], by default None
    end_time : str, optional
        [description], by default None
    frequency : str, optional
        [description], by default 'daily'
    database : str, optional
        数据库名,默认为聚宽数据, by default 'jqdata'

    Returns
    -------
    np.ndarray
        [description]

    Raises
    ------
    NotImplementedError
        [description]
    NotImplementedError
        [description]
    """
    if code:
        if isinstance(code, str):
            code = list(map(str.strip, code.split(',')))
    if end_time:
        try:
            time.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            end_time = end_time + ' 17:00:00'

    if frequency in ['day', 'daily', 'd']:
        table_name = 'stock_day'
    elif frequency in ['min', 'minute', '1min']:
        table_name = 'stock_min'
    elif frequency in ['auction', 'call_auction']:
        table_name = 'call_auction'
    else:
        raise NotImplementedError

    client = Client(host=config.clickhouse_IP, database=database)
    create_clickhouse_table(frequency, client)
    # DESC 降序
    if end_time and code:
        sql = "SELECT x.* FROM %s x" % table_name + " WHERE `datetime` <= %(end_time)s \
        AND `code` IN %(code)s ORDER BY (`datetime`, `code`) DESC LIMIT %(limit)s by `code`"

        # 查询,返回数据类型为元组数组
        res_tuple_list = client.execute(sql, {
            'end_time': end_time,
            'code': code,
            'limit': limit,
        })
    elif end_time:
        sql = "SELECT x.* FROM %s x " % table_name + " WHERE `datetime` <= %(end_time)s \
        ORDER BY (`datetime`, `code`) DESC LIMIT %(limit)s by `code`"

        res_tuple_list = client.execute(sql, {
            'end_time': end_time,
            'limit': limit,
        })
    elif code:
        # 注意WHERE前的空格
        sql = "SELECT x.* FROM %s x " % table_name + " WHERE `code` IN %(code)s \
         ORDER BY (`datetime`, `code`) DESC LIMIT %(limit)s by `code`"

        res_tuple_list = client.execute(sql, {
            'code': code,
            'limit': limit,
        })
    else:
        sql = "SELECT x.* FROM %s x " % table_name + " ORDER BY (`datetime`, `code`) DESC LIMIT %(limit)s by `code`"
        res_tuple_list = client.execute(sql, {
            'limit': limit,
        })

    # 默认有序条件下删除res_tuple_list重复数据
    res_tuple_list = removeDuplicates(res_tuple_list)
    # 元组数组通过numpy结构化,注意数据长度code:8字符 date:10字符.可能存在问题

    return tuplelist_to_np(res_tuple_list, table_name)


async def asynquery_clickhouse(code: list = None,
                               start_time: str = None,
                               end_time: str = None,
                               frequency='daily',
                               database='jqdata') -> np.ndarray:
    """
    异步查询

    Parameters
    ----------
    code : list, optional
        [description], by default None
    start_time : str, optional
        [description], by default None
    end_time : str, optional
        [description], by default None
    frequency : str, optional
        [description], by default 'daily'
    database : str, optional
        数据库名,默认为聚宽数据, by default 'jqdata'

    Returns
    -------
    np.ndarray
        [description]

    Raises
    ------
    NotImplementedError
        [description]
    NotImplementedError
        [description]
    """
    raise NotImplementedError


if __name__ == '__main__':
    client = Client(host=config.clickhouse_IP, database='test')
    print(len(query_N_clickhouse(10)))
    # create_clickhouse_table('trade_days', client)
