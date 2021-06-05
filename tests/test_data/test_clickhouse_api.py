#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   test_clickhouse_api.py
@Time    :   2021/06/04
@Author  :   levonwoo
@Version :   0.1
@Contact :   
@License :   (C)Copyright 2020-2021
@Desc    :   None
'''

# here put the import lib
import pytest
from QuadQuanta.data.clickhouse_api import *

data = [
    [None, '2020-06-03', '2020-06-03', 'daily', 'jqdata'],
    ['000001', '2020-06-03', '2020-06-03', 'daily', 'jqdata'],
    [['000001', '000002'], '2020-06-03', '2020-06-03', 'daily', 'jqdata'],
    [['000001', '000002'], '2020-06-01', '2020-06-03', 'daily', 'jqdata'],
]

data_N = [
    [0, None, '2020-06-03', 'daily', 'jqdata'],
    [1, '000001', '2020-06-03', 'daily', 'jqdata'],
    [1, ['000001', '000002'], '2020-06-03', 'daily', 'jqdata'],
    [1, ['000001', '000002'], '2020-06-03', 'daily', 'jqdata'],
]


@pytest.mark.parametrize('code, start_time, end_time, frequency, database',
                         data)
class TestQueryClickhouse():
    """
    测试clickhouse查询接口
    """
    def test_query_clickhouse(self, code, start_time, end_time, frequency,
                              database):
        """
        
        """
        print(query_clickhouse(code, start_time, end_time, frequency, database))


@pytest.mark.parametrize('count, code, end_time, frequency, database', data_N)
class TestQueryNClickhouse():
    """
    测试clickhouse查询接口
    """
    def test_query_N_clickhouse(self, count, code, end_time, frequency,
                                database):
        """

        """
        print(query_N_clickhouse(count, code, end_time, frequency, database))