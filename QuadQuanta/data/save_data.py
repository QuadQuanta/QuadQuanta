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

import jqdatasdk as jq
from clickhouse_driver import Client
from QuadQuanta.config import config
from QuadQuanta.data.clickhouse_api import (create_clickhouse_database,
                                            create_clickhouse_table,
                                            drop_click_table, insert_clickhouse,
                                            query_exist_date,
                                            query_exist_max_datetime)
from QuadQuanta.data.data_trans import pd_to_tuplelist
from QuadQuanta.data.get_data import (get_jq_bars, get_jq_trade_days,
                                      get_trade_days)
from QuadQuanta.utils.datetime_func import is_valid_date
from QuadQuanta.utils.logs import logger
from tqdm import tqdm

# here put the import lib


def save_bars(start_time=config.start_date,
              end_time='2014-01-10',
              frequency='daily',
              database='jqdata',
              continued=True):
    """
    保存起始时间内所有聚宽股票数据到clickhouse

    Parameters
    ----------
    start_time : str, optional
        开始时间, by default config.start_date
    end_time : str, optional
        结束时间, by default '2014-01-10'
    frequency : str, optional
        数据频率, by default 'daily'
    database : str, optional
        数据库名, by default 'jqdata'
    continued : bool, optional
        是否接着最大日期更新, by default True

    Raises
    ------
    Exception
        [description]
    Exception
        [description]
    """
    if is_valid_date(start_time) and is_valid_date(end_time):
        pass

    # 强制转换start_time, end_time时间改为9:00:00和17:00
    client = Client(host=config.clickhouse_IP)
    create_clickhouse_database(database, client)
    client = Client(host=config.clickhouse_IP, database=database)

    current_hour = datetime.datetime.now().hour
    today = datetime.datetime.today()
    # 交易日收盘前更新,只更新到昨日数据
    if str(today)[:10] <= end_time[:10]:
        end_time = str(today)[:10]
        if current_hour < 16:
            end_time = str(today - datetime.timedelta(1))[:10]

    # 统一日期格式
    try:
        time.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        start_time = start_time + ' 09:00:00'
    try:
        time.strptime(end_time, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        end_time = end_time + ' 17:00:00'


    # if current_hour < 16 and str(today)[:10] <= end_time[:10]:
    #     end_time = str(today - datetime.timedelta(1))[:10]
    #     end_time = end_time[:10] + ' 17:00:00'

    # 表不存在则创建相应表
    create_clickhouse_table(frequency, client)
    # 这种方式获取股票列表会有NAN数据，且需要转换股票代码格式
    stock_pd = jq.get_all_securities().assign(code=lambda x: x.index)
    code_list = stock_pd['code'].apply(lambda x: str(x)[:6]).unique().tolist()

    if continued:
        exist_max_datetime = query_exist_max_datetime(code_list, frequency,
                                                      client)[0][0]

        # 从最大datetime的次日开始
        if str(exist_max_datetime) > config.start_date:  # 默认'2014-01-01'
            start_time = str(exist_max_datetime + datetime.timedelta(hours=18))
        else:
            if start_time <= config.start_date:  # 默认'2014-01-01'
                start_time = config.start_date + ' 9:00:00'
            start_time = start_time

    if start_time <= end_time:
        date_range = get_trade_days(start_time, end_time)
        exist_date_range = query_exist_date(start_time=start_time,
                                            end_time=end_time,
                                            frequency=frequency,
                                            client=client)
        for i in tqdm(range(len(date_range))):
            time.sleep(0.1)
            if date_range[i] not in exist_date_range:
                # 分钟数据查询剩余流量
                if frequency in ['min', 'minute']:
                    spare_jqdata = jq.get_query_count()['spare']
                    if spare_jqdata // (240 * len(code_list)) < 1:
                        raise Exception('保存分钟数据流量不足')

                try:
                    insert_clickhouse(
                        get_jq_bars(code_list,
                                    str(date_range[i])[:10],
                                    str(date_range[i])[:10], frequency),
                        frequency, client)
                except Exception as e:
                    logger.warning(f"{date_range[i]}:error:{e}")
                    # raise Exception('Insert acution error', str(date_range[i])[:10])
                    continue
    else:
        raise Exception('日期段数据已保存或开始日期大于结束日期')


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
    # save_bars('2014-01-01 09:00:00',
    #           '2015-01-01 17:00:00',
    #           frequency='auction',
    #           database='test')
    save_bars('2020-02-01 09:00:00',
              '2021-01-01 17:00:00',
              frequency='minute',
              database='jqdata_test',continued=False)

    # save_trade_days(database='test')
