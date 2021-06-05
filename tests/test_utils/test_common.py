#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   test_common.py
@Time    :   2021/06/03
@Author  :   levonwoo
@Version :   0.1
@Contact :   
@License :   (C)Copyright 2020-2021
@Desc    :   None
'''

# here put the import lib
import pytest
from QuadQuanta.utils.common import *


class TestRemoveDuplicates():
    """
    removeDuplicates函数测试
    """
    def test_asclist(self):
        """
        测试升序列表
        """
        asc_list1 = ['a', 'b', 'c', 'c']
        asc_list2 = ['a', 'a', 'b', 'c', 'c']
        asc_list3 = ['a', 'a', 'b', 'c', 'c']
        pytest.assume(['a', 'b', 'c'] == removeDuplicates(asc_list1))
        pytest.assume(['a', 'b', 'c'] == removeDuplicates(asc_list2))
        pytest.assume(['a', 'b', 'c'] == removeDuplicates(asc_list3))

    def test_desclist(self):
        """
        测试降序列表
        """
        desc_list1 = [8, 7, 6, 5]
        desc_list2 = [8, 8, 7, 6, 5]
        desc_list3 = [8, 8, 7, 6, 6, 5, 5]
        pytest.assume([8, 7, 6, 5] == removeDuplicates(desc_list1))
        pytest.assume([8, 7, 6, 5] == removeDuplicates(desc_list2))
        pytest.assume([8, 7, 6, 5] == removeDuplicates(desc_list3))

    def test_samelist(self):
        """
        测试全部为相同元素的列表
        """
        same_list1 = ['a']
        same_list2 = ['a', 'a', 'a']
        same_list3 = [1, 1, 1, 1, 1]
        pytest.assume(['a'] == removeDuplicates(same_list1))
        pytest.assume(['a'] == removeDuplicates(same_list2))
        pytest.assume([1] == removeDuplicates(same_list3))

    def test_emptylist(self):
        """
        测试空元素列表
        """
        empty_list1 = []
        empty_list2 = ['']
        empty_list3 = ['', '', '']
        pytest.assume([] == removeDuplicates(empty_list1))
        pytest.assume([''] == removeDuplicates(empty_list2))
        pytest.assume([''] == removeDuplicates(empty_list3))


class TestIsSorted():
    """
    测试列表是否有序
    """
    def test_asclist(self):
        """
        测试升序列表
        """
        asc_list1 = ['a']
        asc_list2 = ['a', 'b', 'c']
        asc_list3 = ['a', 'a', 'b', 'c', 'c']
        asc_list4 = ['a', 'c', 'b', 'a', 'c']
        pytest.assume(True == is_sorted(asc_list1))
        pytest.assume(True == is_sorted(asc_list2))
        pytest.assume(True == is_sorted(asc_list3))
        pytest.assume(False == is_sorted(asc_list4))