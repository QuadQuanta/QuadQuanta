#-*-coding:utf-8-*-
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