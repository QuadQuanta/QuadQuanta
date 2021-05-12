#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   fetch_jqdata.py
@Time    :   2021/05/07
@Author  :   levonwoo
@Version :   0.1
@Contact :   
@License :   (C)Copyright 2020-2021
@Desc    :   None
'''

# here put the import lib
from collections import OrderedDict
import datetime
import jqdatasdk as jq
from clickhouse_driver import Client
import pandas as pd
from tqdm import tqdm

from QuadQuanta.config import config
from QuadQuanta.utils.datetime_func import date_convert_stamp, datetime_convert_stamp
from QuadQuanta.data import create_clickhouse_database, create_clickhouse_table
from QuadQuanta.data import insert_to_clickhouse
from QuadQuanta.data import query_exist_max_datetime


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
    else:
        raise NotImplementedError

    for filed, series in pd_data.iteritems():
        if filed in rawdata.keys():
            rawdata[filed] = series.tolist()
    #  list(rawdata.values())表示字典值转为列表
    #  map(tuple, zip(*array))表示二维数组转置
    return list(map(tuple, zip(*list(rawdata.values()))))


def save_all_jqdata(start_time, end_time, frequency='daily', database='jqdata'):
    """
    保存起始时间内所有聚宽股票数据到clickhouse

    Parameters
    ----------
    start_time : str
        [description]
    end_time : str, 
        [description] 
    frequency : str, optional
        [description], by default 'daily'
    """
    jq.auth(config.jqusername, config.jqpasswd)
    # 强制转换start_time, end_time时间改为9:00:00和17:00
    try:
        client = Client(host=config.clickhouse_IP)
        create_clickhouse_database(client, database)
        client = Client(host=config.clickhouse_IP, database=database)
        start_time = start_time[:10] + ' 09:00:00'
        end_time = end_time[:10] + ' 17:00:00'

        # 表不存在则创建相应表
        create_clickhouse_table(client, frequency)
        # 这种方式获取股票列表会有NAN数据，且需要转换股票代码格式
        stock_pd = jq.get_all_securities().assign(code=lambda x: x.index)
        code_list = stock_pd['code'].apply(
            lambda x: str(x)[:6]).unique().tolist()

        if end_time < start_time:
            raise ValueError  # 终止日期小于开始日期

        try:
            # 日线级别数据保存，全部一起获取
            if frequency in ['d', 'daily', 'day']:
                insert_to_clickhouse(
                    pd_to_tuplelist(
                        fetch_jqdata(code_list, start_time, end_time, client,
                                     frequency), frequency), client, frequency)

            # 分钟级别数据保存，每个股票单独保存
            elif frequency in ['mim', 'minute']:
                for i in tqdm(range(len(code_list))):
                    try:
                        insert_to_clickhouse(
                            pd_to_tuplelist(
                                fetch_jqdata(code_list[i], start_time, end_time,
                                             client, frequency), frequency),
                            client, frequency)
                    # TODO log输出
                    except Exception as e:
                        print('{}:error:{}'.format(code_list[i], e))
                        continue
            else:
                raise NotImplementedError

        except Exception as e:
            print('error:{}'.format(e))
    except Exception as e:
        pass


def fetch_jqdata(code, start_time: str, end_time: str, client, frequency: str):
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
    else:
        raise NotImplementedError

    columns = [
        'time', 'code', 'open', 'close', 'high', 'low', 'volume', 'money',
        'avg', 'high_limit', 'low_limit', 'pre_close'
    ]
    empty_pd = pd.concat([pd.DataFrame({k: [] for k in columns}), None, None])

    # 查询最大datetime
    exist_max_datetime = query_exist_max_datetime(code, frequency, client)[0][0]
    # 数据从2014年开始保存
    # TODO 用交易日历代替简单的日期加一
    if str(exist_max_datetime) > config.start_date:  # 默认'2014-01-01'
        _start_time = str(exist_max_datetime + datetime.timedelta(hours=18))
    else:
        if start_time < config.start_date:  # 默认'2014-01-01'
            start_time = config.start_date + ' 9:00:00'
        _start_time = start_time

    if _start_time < end_time:
        pd_data = jq.get_price(jq.normalize_code(code),
                               start_date=_start_time,
                               end_date=end_time,
                               frequency=frequency,
                               fields=[
                                   'open', 'close', 'high', 'low', 'volume',
                                   'money', 'avg', 'high_limit', 'low_limit',
                                   'pre_close'
                               ],
                               skip_paused=True,
                               fq='none',
                               count=None,
                               panel=False)
        # TODO 有没有更优雅的方式
        pd_data['pre_close'] = pd_data['pre_close'].fillna(
            method='backfill')  #新股上市首日分钟线没有pre_close数据，用下一行填充
        pd_data = pd_data.dropna(axis=0, how='any')  # 删除包含NAN的行

    else:
        return empty_pd

    if len(pd_data) == 0:
        return empty_pd
    else:
        pd_data['datetime'] = pd_data['time']

        return pd_data.assign(
            amount=pd_data['money'],
            code=pd_data['code'].apply(lambda x: x[:6]),  # code列聚宽格式转为六位纯数字格式
            date=pd_data['datetime'].apply(lambda x: str(x)[0:10]),
            date_stamp=pd_data['datetime'].apply(
                lambda x: datetime_convert_stamp(x))).set_index('datetime',
                                                                drop=True,
                                                                inplace=False)


if __name__ == '__main__':
    # save_all_jqdata('2014-01-01 09:00:00',
    #                 '2021-05-08 17:00:00',
    #                 frequency='daily')
    save_all_jqdata('2021-05-06 09:00:00',
                    '2021-05-08 17:00:00',
                    frequency='minute',
                    database='test')
