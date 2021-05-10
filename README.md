# 基于聚宽数据源的本地clickhouse存储方案

## 简明使用教程

### 安装clickhouse

参考clickhouse官网

### 安装QuadQuanta

```
pip install QuadQuanta
```

### 使用示例

#### 下载源码

```
git clone
```

进入文件夹

```
$ cd ./QuadQuanta/QuadQuanta
```

创建`personal.yaml`配置文件，填写如下内容

#### 配置

`personal.yaml`文件

```
jqdata:
  username: 'yourusername' #聚宽账户
  passwd: 'yourpasswd'

clickhouse_server: '127.0.0.1' # clickhouse服务端地址
```

#### 更新数据

运行`./QuadQuanta/data/updata_daybar.py`即可更新从2014-01-01开始的日线数据

修改`config.yaml`文件`start_date`项可修改开始时间，参考聚宽数据源的最早数据时间

#### 示例

`./QuadQuanta/data/example.py`文件为一个简易的选股示例

### 数据字段

#### 日线，分钟线

|   fileds   | 类型 | 名称                                              | 注释           |
| :--------: | ---- | :------------------------------------------------ | -------------- |
|  datetime  |      | 数据开始时间                                      |                |
|    code    |      | 股票代码                                          | 六位纯数字代码 |
|    open    |      | 时间段开始时价格                                  |                |
|   close    |      | 时间段结束时价格                                  |                |
|    high    |      | 时间段中最高价                                    |                |
|    low     |      | 时间段中最低价                                    |                |
|   volume   |      | 时间段中的成交的股票数量                          |                |
|   amount   |      | 时间段中的成交的金额                              |                |
|    avg     |      | 时间段中的平均价                                  |                |
| high_limit |      | 当日涨停价                                        |                |
| low_limit  |      | 当日跌停价                                        |                |
| pre_close  |      | 前一个单位时间结束时的价格,按天则是前一天的收盘价 |                |
|    date    |      | 当日日期                                          |                |
| date_stamp |      | 日期时间戳                                        |                |