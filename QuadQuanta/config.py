#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   config.py
@Time    :   2021/06/15
@Author  :   levonwoo
@Version :   0.1
@Contact :   
@License :   (C)Copyright 2020-2021
@Desc    :   None
'''

# here put the import lib

import os
import sys

import jqdatasdk as jq
import yaml

from QuadQuanta.utils.logs import logger


class Config():
    def __init__(self):
        config_dirs = os.path.expanduser('~') + '/.QuadQuanta/'
        if not os.path.exists(config_dirs):
            os.makedirs(config_dirs)
        self.path = config_dirs

    def load_config_yaml(self):
        try:
            with open(self.path + 'config.yaml', 'r') as f:
                return yaml.safe_load(f.read())
        except IOError:
            with open(self.path + 'config.yaml', 'a+') as f:
                logger.info("创建配置文件成功, 请配置后运行")
                sys.exit()

    @property
    def jqusername(self):
        return self.get_jqusername()

    @property
    def jqpasswd(self):
        return self.get_jqpasswd()

    @property
    def clickhouse_IP(self):
        return self.get_clickhouse_ip()

    @property
    def clickhouse_user(self):
        return self.get_clickhouse_user()

    @property
    def clickhouse_password(self):
        return self.get_clickhouse_password()

    @property
    def start_date(self):
        return self.get_start_date()

    def get_jqusername(self):
        yaml_data = self.load_config_yaml()
        return yaml_data['jqdata']['username']

    def get_jqpasswd(self):
        yaml_data = self.load_config_yaml()
        return yaml_data['jqdata']['passwd']

    def get_clickhouse_ip(self):
        yaml_data = self.load_config_yaml()
        return yaml_data['clickhouse']['ip']

    def get_clickhouse_user(self):
        yaml_data = self.load_config_yaml()
        return yaml_data['clickhouse']['user']

    def get_clickhouse_password(self):
        yaml_data = self.load_config_yaml()
        return yaml_data['clickhouse']['password']

    def get_start_date(self):
        yaml_data = self.load_config_yaml()
        return yaml_data['start_date']


config = Config()
# TODO 判断yaml中数据合法性

jq.auth(config.jqusername, config.jqpasswd)
if __name__ == '__main__':
    # config = Config()
    print(config.jqusername)
    print(config.jqpasswd)
    print(config.clickhouse_IP)
    print(config.start_date)
