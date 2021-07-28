#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   clickhouse_api.py
@Time    :   2021/05/07
@Author  :   levonwoo
@Version :   0.2
@Contact :   
@License :   (C)Copyright 2020-2021
@Desc    :   clickhouse接口
'''

import time
from itertools import chain

# here put the import lib
import numpy as np
from clickhouse_driver import Client
from QuadQuanta.config import config
from QuadQuanta.data.data_trans import tuplelist_to_np
from QuadQuanta.utils.common import is_sorted, removeDuplicates
from QuadQuanta.utils.datetime_func import is_valid_date


def create_clickhouse_database(database: str,
                               client: Client = Client(host='127.0.0.1')):
    """
    数据库不存在则创建clickhouse数据库

    Parameters
    ----------
    database : str
        数据库名
    client : Client, optional
        clickhouse客户端连接, by default Client(host='127.0.0.1')
    """
    create_database_sql = 'CREATE DATABASE IF NOT EXISTS %s' % database
    client.execute(create_database_sql)


def create_clickhouse_table(type: str,
                            client: Client = Client(host='127.0.0.1',
                                                    database='jqdata')):
    """
    创建clickhouse数据表

    Parameters
    ----------
    type : str
        存储表类型,已完成的有日线（daily）,一分钟线(minute),开盘竞价,交易日历
    client : Client, optional
        clickhouse的客户端连接, by default Client(host='127.0.0.1', database='jqdata')

    Raises
    ------
    NotImplementedError
        [description]
    """
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


def drop_click_table(table_name: str,
                     client: Client = Client(host='127.0.0.1',
                                             database='jqdata')):
    """
    丢弃clickhouse表

    Parameters
    ----------
    table_name : str
        要丢弃的表名
    client : Client, optional
        clickhouse的客户端连接, by default Client(host='127.0.0.1', database='jqdata')
    """
    drop_sql = "DROP TABLE IF EXISTS %s" % table_name
    client.execute(drop_sql)


def insert_clickhouse(data,
                      type,
                      client: Client = Client(host='127.0.0.1',
                                              database='jqdata')):
    """
    将数据插入clickhouse数据库

    Parameters
    ----------
    data : tuple_list
        元组数组类型数据,每个元组为一行
    type : str
        存储表类型,已完成的有日线（daily）,一分钟线(minute),开盘竞价,交易日历
    client : Client, optional
        clickhouse的客户端连接, by default Client(host='127.0.0.1', database='jqdata')

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
    """
    查询clickhouse表中某个code已经存在的最大日期, code=None表示表中的所有code

    Parameters
    ----------
    code : list, optional
        六位数股票代码列表,如['000001'], ['000001',...,'689009'], by default None
    type : str, optional
        数据类型,已完成的有日线（daily）,一分钟线(minute),竞价(call_auction),交易日历(trade_days), by default 'daily'
    client : clickhouse_driver.Client, optional
        clickhouse客户端连接, by default Client(host='127.0.0.1', database='jqdata')

    Returns
    -------
    [type]
        [description]

    Raises
    ------
    NotImplementedError
        [description]
    """
    if isinstance(code, str):
        code = list(map(str.strip, code.split(',')))

    if type in ['day', 'daily', 'd']:
        table_name = 'stock_day'
    elif type in ['min', 'minute', '1min']:
        table_name = 'stock_min'
    elif type in ['auction', 'call_auction']:
        table_name = 'call_auction'
    else:
        raise NotImplementedError
    if code:
        if isinstance(code, str):
            code = list(map(str.strip, code.split(',')))
        max_datetime_sql = 'SELECT max(datetime) FROM %s' % table_name + ' ' + 'WHERE `code` IN %(code)s'
        res = client.execute(max_datetime_sql, {'code': code})
    else:
        max_datetime_sql = 'SELECT max(datetime) FROM %s' % table_name
        res = client.execute(max_datetime_sql)

    return res


def query_exist_date(code=None,
                     start_time='2000-01-01',
                     end_time='2200-01-01',
                     frequency='daily',
                     client: Client = Client(host='127.0.0.1',
                                             database='jqdata')):
    """
    查询clickhouse表中code在指定日期区间内已保存数据的日期列表, code=None表示表中的所有code

    Parameters
    ----------
    code : list, optional
        六位数股票代码列表,如['000001'], ['000001',...,'689009'], by default None
    start_time : str, optional
        [description], by default '2000-01-01'
    end_time : str, optional
        [description], by default '2200-01-01'
    frequency : str, optional
        数据类型,已完成的有日线（daily）,一分钟线(minute),竞价(call_auction), by default 'daily'
    client : clickhouse_driver.Client, optional
        clickhouse客户端连接, by default Client(host='127.0.0.1', database='jqdata')

    Returns
    -------
    [type]
        [description]

    Raises
    ------
    NotImplementedError
        [description]
    ValueError
        [description]
    Exception
        [description]
    """
    if frequency in ['day', 'daily', 'd']:
        table_name = 'stock_day'
    elif frequency in ['min', 'minute', '1min']:
        table_name = 'stock_min'
    elif frequency in ['auction', 'call_auction']:
        table_name = 'call_auction'
    else:
        raise NotImplementedError

    if is_valid_date(start_time) and is_valid_date(end_time):
        try:
            time.strptime(start_time, "%Y-%m-%d")
        except ValueError:
            start_time = start_time[:10]
            end_time = end_time[:10]
    #  判断日期合法
    if start_time > end_time:
        raise ValueError('开始时间大于结束时间')

    if code:
        if isinstance(code, str):
            code = list(map(str.strip, code.split(',')))
        sql = "SELECT DISTINCT x.date FROM %s x" % table_name + " WHERE `date` >= %(start_time)s AND `date` <= %(end_time)s AND `code` IN %(code)s ORDER BY (`date`)"
        # 查询,返回数据类型为元组数组
        res_tuple_list = client.execute(sql, {
            'start_time': start_time,
            'end_time': end_time,
            'code': code
        })
    else:
        sql = "SELECT DISTINCT x.date FROM %s x" % table_name + " WHERE `date` >= %(start_time)s AND `date` <= %(end_time)s  ORDER BY (`date`)"
        res_tuple_list = client.execute(sql, {
            'start_time': start_time,
            'end_time': end_time,
        })
    # tuple_list转list
    res = list(chain(*np.array(res_tuple_list).tolist()))
    return res


def query_clickhouse(code: list = None,
                     start_time: str = '1970-01-01',
                     end_time: str = '2200-01-01',
                     frequency='daily',
                     database='jqdata',
                     **kwargs) -> np.ndarray:
    """
    clickhouse查询接口,默认为None的条件,返回所有数据

    Parameters
    ----------
    code : list or str, optional
        六位数字股票代码列表, by default None
    start_time : str, optional
        开始日期, by default None
    end_time : str, optional
        结束日期, by default None
    frequency : str, optional
        数据周期, by default 'daily'
    database : str, optional
        clickhouse数据库名,默认从聚宽数据查询, by default 'jqdata'

    Returns
    -------
    np.ndarray
        [description]
    Raises
    ------
    ValueError
        [description]
    NotImplementedError
        [description]
    """
    if frequency in ['day', 'daily', 'd']:
        table_name = 'stock_day'
    elif frequency in ['min', 'minute', '1min']:
        table_name = 'stock_min'
    elif frequency in ['auction', 'call_auction']:
        table_name = 'call_auction'
    elif frequency in ['trade_days']:
        table_name = 'trade_days'
    else:
        raise NotImplementedError

    if is_valid_date(start_time) and is_valid_date(end_time):
        if table_name == 'trade_days':
            try:
                time.strptime(start_time, "%Y-%m-%d")
            except ValueError:
                start_time = start_time[:10]
                end_time = end_time[:10]
        else:
            try:
                time.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                start_time = start_time + ' 09:00:00'
                end_time = end_time + ' 17:00:00'
    #  判断日期合法
    if start_time > end_time:
        raise ValueError('开始时间大于结束时间')

    client = Client(host=config.clickhouse_IP, database=database)

    if code:
        if isinstance(code, str):
            # TODO 是否是有效的股票代码
            code = list(map(str.strip, code.split(',')))
        # 注意WHERE前的空格
        sql = "SELECT DISTINCT x.* FROM %s x" % table_name + " WHERE `datetime` >= %(start_time)s \
                        AND `datetime` <= %(end_time)s AND `code` IN %(code)s ORDER BY (`datetime`, `code`)"

        # 查询,返回数据类型为元组数组
        res_tuple_list = client.execute(sql, {
            'start_time': start_time,
            'end_time': end_time,
            'code': code
        })
    else:
        sql = "SELECT DISTINCT x.* FROM %s x" % table_name + " WHERE `datetime` >= %(start_time)s \
                                AND `datetime` <= %(end_time)s ORDER BY (`datetime`, `code`)"

        if table_name == 'trade_days':
            sql = "SELECT DISTINCT x.* FROM %s x" % table_name + " WHERE `datetime` >= %(start_time)s \
                                    AND `datetime` <= %(end_time)s ORDER BY (`datetime`)"

        res_tuple_list = client.execute(sql, {
            'start_time': start_time,
            'end_time': end_time
        })
    #  TODO clickhouse分片

    # 默认有序条件下删除res_tuple_list重复数据
    # if is_sorted(res_tuple_list):
    #     res_tuple_list = removeDuplicates(res_tuple_list)
    # else:
    #     raise Exception('clickhouse返回列表非有序')

    # 元组数组通过numpy结构化,注意数据长度code:8字符 date:10字符.可能存在问题

    return tuplelist_to_np(res_tuple_list, table_name)


def query_N_clickhouse(count: int,
                       code: list = None,
                       end_time: str = '2200-01-01',
                       frequency='daily',
                       database='jqdata',
                       **kwargs) -> np.ndarray:
    """
    获取结束日期之前的N个时间序列数据

    Parameters
    ----------
    count : int
        时间序列个数
    code : list, optional
        股票代码列表, by default None
    end_time : str, optional
        结束时间, by default '2200-01-01'
    frequency : str, optional
        k线周期, by default 'daily'
    database : str, optional
        clickhouse数据库名, by default 'jqdata'

    Returns
    -------
    np.ndarray
        [description]

    Raises
    ------
    NotImplementedError
        [description]
    """

    if frequency in ['day', 'daily', 'd']:
        table_name = 'stock_day'
    elif frequency in ['min', 'minute', '1min']:
        table_name = 'stock_min'
    elif frequency in ['auction', 'call_auction']:
        table_name = 'call_auction'
    elif frequency in ['trade_days']:
        table_name = 'trade_days'
    else:
        raise NotImplementedError
    if is_valid_date(end_time):
        if table_name == 'trade_days':
            try:
                time.strptime(end_time, "%Y-%m-%d")
            except ValueError:
                end_time = end_time[:10]
        else:
            try:
                time.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                if table_name != 'trade_days':
                    end_time = end_time + ' 17:00:00'

    client = Client(host=config.clickhouse_IP, database=database)
    # DESC 降序
    if code:
        if isinstance(code, str):
            code = list(map(str.strip, code.split(',')))
        sql = "SELECT DISTINCT x.* FROM %s x" % table_name + " WHERE `datetime` <= %(end_time)s \
        AND `code` IN %(code)s ORDER BY (`datetime`, `code`) DESC LIMIT %(limit)s by `code`"

        # 查询,返回数据类型为元组数组
        res_tuple_list = client.execute(sql, {
            'end_time': end_time,
            'code': code,
            'limit': count,
        })
    else:
        sql = "SELECT DISTINCT x.* FROM %s x " % table_name + " WHERE `datetime` <= %(end_time)s \
        ORDER BY (`datetime`, `code`) DESC LIMIT %(limit)s by `code`"

        if table_name == 'trade_days':
            sql = "SELECT DISTINCT x.* FROM %s x" % table_name + " WHERE `datetime` <= %(end_time)s \
            ORDER BY (`datetime`) DESC LIMIT %(limit)s"

        res_tuple_list = client.execute(sql, {
            'end_time': end_time,
            'limit': count,
        })
    # 将倒序列表翻转
    res_tuple_list.reverse()
    # 默认有序条件下删除res_tuple_list重复数据
    # if is_sorted(res_tuple_list):
    #     res_tuple_list = removeDuplicates(res_tuple_list)
    # else:
    #     raise Exception('clickhouse返回列表非有序')
    # 元组数组通过numpy结构化,注意数据长度code:8字符 date:10字符.可能存在问题

    return tuplelist_to_np(res_tuple_list, table_name)


if __name__ == '__main__':
    client = Client(host=config.clickhouse_IP, database='jqdata')
    # print((query_exist_date(start_time='2020-01-01',
    #                         end_time='2020-05-01',
    #                         client=client)))
    # print((query_N_clickhouse(2, '000001', end_time='2021-05-20')))
    # print(query_exist_max_datetime(code=['000001'], type='daily',
    #                                client=client))
    # create_clickhouse_table('trade_days', client)
    print((query_clickhouse(start_time='2014-05-20',
                            end_time='2014-05-20',
                            frequency='minute',
                            database='jqdata')))
    # insert_clickhouse()
