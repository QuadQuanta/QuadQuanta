import os
import sys

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
        except:
            with open(self.path + 'config.yaml', 'a+') as f:
                logger.info(f"创建配置文件成功, 请配置后运行")
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
        return yaml_data['clickhouse_IP']

    def get_start_date(self):
        yaml_data = self.load_config_yaml()
        return yaml_data['start_date']


config = Config()
# TODO 判断yaml中数据合法性

if __name__ == '__main__':
    # config = Config()
    print(config.jqusername)
    print(config.jqpasswd)
    print(config.clickhouse_IP)
    print(config.start_date)
