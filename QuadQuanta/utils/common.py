#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   common.py
@Time    :   2021/05/07
@Author  :   levonwolf
@Version :   0.1
@Contact :   
@License :   (C)Copyright 2020-2021
@Desc    :   常用函数
'''

# here put the import lib


def removeDuplicates(duplist):
    """
    双指针法去除有序列表中的重复项

    Parameters
    ----------
    duplist : list
        待去重的有序列表

    Returns
    -------
    list
        [description]
    """
    n = len(duplist)
    if n <= 1:
        return duplist
    fast = slow = 1
    while fast < n:
        if duplist[fast] != duplist[fast - 1]:
            duplist[slow] = duplist[fast]
            slow += 1
        fast += 1
    return duplist[:slow]


def is_sorted(lst):
    """
    判断列表或字符串是否有序,不论升序还是降序

    Parameters
    ----------
    lst : list or str
        待判断的列表或字符串

    Returns
    -------
    bool
        有序返回True, 无序返回False

    Raises
    ------
    NotImplementedError
        [description]
    """
    if isinstance(lst, list) or isinstance(lst, str):
        if len(lst) <= 1:
            return True
        return all(x <= y for x, y in zip(lst, lst[1:])) or all(
            x >= y for x, y in zip(lst, lst[1:]))
    else:
        raise NotImplementedError


if __name__ == '__main__':
    pass
