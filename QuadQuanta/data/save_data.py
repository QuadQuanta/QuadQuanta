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

import datetime
import time
# here put the import lib

import jqdatasdk as jq
import pandas as pd
from clickhouse_driver import Client
from QuadQuanta.data.data_trans import pd_to_tuplelist
from QuadQuanta.config import config
from QuadQuanta.data.clickhouse_api import (create_clickhouse_database,
                                            create_clickhouse_table,
                                            drop_click_table, insert_clickhouse)
from QuadQuanta.data.get_data import get_jq_bars, get_jq_trade_days, get_trade_days
from QuadQuanta.utils.datetime_func import is_valid_date
from QuadQuanta.utils.logs import logger
from tqdm import tqdm


def save_bars(start_time='2014-01-01',
              end_time='2014-01-10',
              frequency='daily',
              database='jqdata'):
    """
    保存起始时间内所有聚宽股票数据到clickhouse

    Parameters
    ----------
    start_time : str
        开始时间
    end_time : str, 
        结束时间
    frequency : str, optional
        数据频率, by default 'daily'
    """
    jq.auth(config.jqusername, config.jqpasswd)
    # 强制转换start_time, end_time时间改为9:00:00和17:00
    client = Client(host=config.clickhouse_IP)
    create_clickhouse_database(database, client)
    client = Client(host=config.clickhouse_IP, database=database)

    if is_valid_date(start_time) and is_valid_date(end_time):
        try:
            time.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            start_time = start_time + ' 09:00:00'
            end_time = end_time + ' 17:00:00'
    current_hour = datetime.datetime.now().hour
    today = datetime.datetime.today()
    # 交易日收盘前更新,只更新到昨日数据
    if current_hour < 16 and str(today)[:10] <= end_time[:10]:
        end_time = str(today - datetime.timedelta(1))[:10]
        end_time = end_time[:10] + ' 17:00:00'

    # 表不存在则创建相应表
    create_clickhouse_table(frequency, client)
    # 这种方式获取股票列表会有NAN数据，且需要转换股票代码格式
    stock_pd = jq.get_all_securities().assign(code=lambda x: x.index)
    code_list = stock_pd['code'].apply(lambda x: str(x)[:6]).unique().tolist()

    # 日线级别数据保存，全部一起获取
    if frequency in ['d', 'daily', 'day']:
        insert_clickhouse(
            get_jq_bars(code_list,
                        start_time,
                        end_time,
                        frequency,
                        client=client), frequency, client)

    # 分钟级别数据保存，每个股票单独保存
    elif frequency in ['mim', 'minute']:
        for i in tqdm(range(len(code_list))):
            try:
                insert_clickhouse(
                    get_jq_bars(code_list[i],
                                start_time,
                                end_time,
                                frequency,
                                client=client), frequency, client)
            # TODO log输出
            except Exception as e:
                logger.warning(f"{code_list[i]}:error:{e}")
                # raise Exception('Insert minute data error', code_list[i])
                continue

    # 竞价数据，按日期保存
    elif frequency in ['auction', 'call_auction']:
        # 从交易日历获取交易日期
        date_range = get_trade_days(start_time, end_time)
        for i in tqdm(range(len(date_range))):
            try:
                insert_clickhouse(
                    get_jq_bars(code_list,
                                str(date_range[i])[:10],
                                str(date_range[i])[:10],
                                frequency,
                                client=client), frequency, client)
            # TODO log输出
            except Exception as e:
                logger.warning(f"{date_range[i]}:error:{e}")
                # raise Exception('Insert acution error', str(date_range[i])[:10])
                continue
    else:
        raise NotImplementedError


def save_trade_days(database='jqdata'):
    """
    从聚宽数据源更新交易日历

    Parameters
    ----------
    database : str, optional
        database名称, by default 'jqdata'
    """
    # 强制转换start_time, end_time时间改为9:00:00和17:00
    client = Client(host=config.clickhouse_IP)
    create_clickhouse_database(database, client)
    client = Client(host=config.clickhouse_IP, database=database)
    # 删除原表, 重新更新
    drop_click_table('trade_days', client)
    create_clickhouse_table('trade_days', client)
    insert_clickhouse(pd_to_tuplelist(get_jq_trade_days(), 'trade_days'),
                      'trade_days', client)


if __name__ == '__main__':
    # save_all_jqdata('2014-01-01 09:00:00',
    #                 '2021-05-08 17:00:00',
    #                 frequency='daily')
    # save_bars('2014-05-21 09:00:00',
    #           '2014-05-22 17:00:00',
    #           frequency='minute',
    #           database='test')
    save_trade_days(database='jqdata')
