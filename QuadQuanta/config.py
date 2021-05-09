import yaml
import os

current_path = os.path.abspath(os.path.dirname(__file__))

global jqusername, jqpasswd, clickhouse_server, start_date


class Config():
    @staticmethod
    def load_personal_yaml():
        with open(current_path + '/personal.yaml', 'r') as f:
            return yaml.safe_load(f.read())

    @staticmethod
    def load_config_yaml():
        with open(current_path + '/config.yaml', 'r') as f:
            return yaml.safe_load(f.read())

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

    @classmethod
    def get_jqusername(cls):
        yaml_data = cls.load_personal_yaml()
        return yaml_data['jqdata']['username']

    @classmethod
    def get_jqpasswd(cls):
        yaml_data = cls.load_personal_yaml()
        return yaml_data['jqdata']['passwd']

    @classmethod
    def get_clickhouse_ip(cls):
        yaml_data = cls.load_personal_yaml()
        return yaml_data['clickhouse_IP']

    @classmethod
    def get_start_date(cls):
        yaml_data = cls.load_config_yaml()
        return yaml_data['start_date']


config = Config()
# TODO 判断yaml中数据合法性

if __name__ == '__main__':
    # config = Config()
    print(config.jqusername)
    print(config.jqpasswd)
    print(config.clickhouse_IP)
    print(config.start_date)
