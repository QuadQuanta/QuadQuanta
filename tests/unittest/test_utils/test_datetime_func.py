#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   test_datetime_func.py
@Time    :   2021/06/03
@Author  :   levonwoo
@Version :   0.1
@Contact :   
@License :   (C)Copyright 2020-2021
@Desc    :   None
'''

# here put the import lib
import py
import pytest
from QuadQuanta.utils.datetime_func import *


class TestDateConvertStamp():
    """
    日期字符串转换为浮点数测试
    """
    def test_empty_str(self):
        empty_str = ''
        with pytest.raises(ValueError):
            date_convert_stamp(empty_str)

    def test_letter_str(self):
        letter_str = 'kna'
        with pytest.raises(ValueError):
            date_convert_stamp(letter_str)

    def test_digital_str(self):
        digital_str = '214:'
        with pytest.raises(ValueError):
            date_convert_stamp(digital_str)

    def test_datetime_str(self):
        datetime_str = '2020-01-01 09:30:00'
        date_str = '2020-01-01'
        pytest.assume(
            date_convert_stamp(datetime_str) == date_convert_stamp(date_str))


class TestDatetimeConvertStamp():
    """
    日期时间字符串转换为浮点数测试
    """
    def test_empty_str(self):
        empty_str = ''
        with pytest.raises(ValueError):
            datetime_convert_stamp(empty_str)

    def test_letter_str(self):
        letter_str = 'kna'
        with pytest.raises(ValueError):
            datetime_convert_stamp(letter_str)

    def test_digital_str(self):
        digital_str = '214:'
        with pytest.raises(ValueError):
            datetime_convert_stamp(digital_str)

    def test_datetime_str(self):
        datetime_str = '2020-01-01 00:00:00'
        date_str = '2020-01-01'
        pytest.assume(
            datetime_convert_stamp(datetime_str) == datetime_convert_stamp(
                date_str))


class TestIsValidDate():
    """
    测试字符串日期是否合法
    """
    def test_empty_str(self):
        empty_str = ''
        pytest.assume(False == is_valid_date(empty_str))

    def test_letter_str(self):
        letter_str = 'kna'
        pytest.assume(False == is_valid_date(letter_str))

    def test_digital_str(self):
        digital_str = '214:'
        pytest.assume(False == is_valid_date(digital_str))

    def test_datetime_str(self):
        datetime_str = '2020-01-01 00:00:00'
        date_str = '2020-01-01'
        pytest.assume(True == is_valid_date(datetime_str))
        pytest.assume(True == is_valid_date(date_str))
