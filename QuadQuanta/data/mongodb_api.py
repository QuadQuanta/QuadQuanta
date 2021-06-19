#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   mongodb_api.py
@Time    :   2021/05/29
@Author  :   levonwoo
@Version :   0.1
@Contact :   
@License :   (C)Copyright 2020-2021
@Desc    :   mongodb数据库API
'''

import pandas as pd
# here put the import lib
import pymongo
from pymongo.errors import DuplicateKeyError
from QuadQuanta.utils.logs import logger


def query_mongodb(db_name,
                  coll_name,
                  sql=None,
                  uri="mongodb://127.0.0.1:27017",
                  **kwargs):
    """
    mongodb数据库查询

    Parameters
    ----------
    coll_name : str
        集合名
    db_name : str
        数据库名
    sql : dict, optional
        查询语句, by default None, None表示返回所有
    uri : str, optional
        mongodb uri, by default 'mongodb://127.0.0.1:27017'

    Returns
    -------
    [type]
        format参数指定返回格式, 默认list,'pd'返回pandas.DataFrame

    Raises
    ------
    NotImplementedError
        [description]
    """
    client = pymongo.MongoClient(uri)
    collection = client[db_name][coll_name]
    if kwargs.get('format') == None:
        return list(collection.find(sql))
    elif kwargs.get('format') in ['pd', 'pandas']:
        return pd.DataFrame(list(collection.find(sql))).set_index('_id')
    else:
        raise NotImplementedError


def insert_mongodb(db_name,
                   coll_name,
                   documents,
                   uri="mongodb://127.0.0.1:27017"):
    """[summary]

    Parameters
    ----------
    db_name : str
        [description]
    coll_name : str
        集合名
    documents : list or dict
        文档列表
    uri : str, optional
        mongodb uri, by default 'mongodb://127.0.0.1:27017'
    """
    client = pymongo.MongoClient(uri)
    collection = client[db_name][coll_name]
    try:
        if isinstance(documents, list):
            collection.insert_many(documents)
        elif isinstance(documents, dict):
            collection.insert_one(documents)
        else:
            raise NotImplementedError
    except DuplicateKeyError:
        pass
    except Exception as e:
        logger.warning(e)


if __name__ == '__main__':
    data = query_mongodb('Stock', 'Nmodel', format='pd')
    print(data)
