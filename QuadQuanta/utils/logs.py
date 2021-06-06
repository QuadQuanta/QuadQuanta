#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   logger.py
@Time    :   2021/06/05
@Author  :   levonwoo
@Version :   0.1
@Contact :   
@License :   (C)Copyright 2020-2021
@Desc    :   日志配置模块
'''

# here put the import lib
import os
import sys
from loguru import logger

LOG_PATH = os.path.expanduser("~") + "/.QuadQuanta/log/"
if not os.path.exists(LOG_PATH):
    os.makedirs(LOG_PATH)

EXE_FILENAME = os.path.basename(sys.argv[0]).split(".py")[0]
LOG_FILE_NAME = LOG_PATH + EXE_FILENAME + "_{time}.log"

logger.add(LOG_FILE_NAME, level="INFO")

if __name__ == '__main__':
    pass