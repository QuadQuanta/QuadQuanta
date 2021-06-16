#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   const.py
@Time    :   2021/06/15
@Author  :   levonwoo
@Version :   0.1
@Contact :   
@License :   (C)Copyright 2020-2021
@Desc    :   None
'''

# here put the import lib

import enum


class DataSource(enum.Enum):
    JQDATA = 'jqdata'
    CLICKHOUSE = 'clickhouse'


class Bar(enum.Enum):
    DAILY = 'daily'
    MINUTE = 'minute'
    AUCTION = 'auction'


if __name__ == '__main__':
    print(Bar.DAILY)
