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
        [description]

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


if __name__ == '__main__':
    pass
