# 基于聚宽数据源的本地clickhouse存储方案

**测试阶段BUG多，勿用于生成环境**

- [x] 日线
- [x] 1分钟线
- [x] 集合竞价
- [ ] 复权


## 简明使用教程

### 安装clickhouse

参考clickhouse官网，暂时未设置用户名，密码认证

### 安装QuadQuanta

#### pip 安装

```
pip install QuadQuanta
```

#### 源码安装最新版

下载最新源码

```
git clone https://github.com/levonwoo/QuadQuanta.git
```

进入项目根目录

```
cd ./QuadQuanta
```

安装

```
python -m pip install -e .
```

### 使用示例

#### 下载源码

```
git clone https://github.com/levonwoo/QuadQuanta.git
```

#### 更新数据

运行`./QuadQuanta/data/updata_daybar.py`即可更新从2014-01-01开始的日线数据

首次会自动在用户目录创建`~/.QuadQuanta/config.yaml`文件，需要配置`config.yaml`文件后运行，以下是config.yaml文件示例

```yaml
# 数据下载开始日期
start_date: '2014-01-01'
#聚宽账户
jqdata:
  username: 'yourusername' 
  passwd: 'yourpasswd'
# clickhouse服务端地址
clickhouse_IP: '127.0.0.1'
```

修改`config.yaml`文件`start_date`项可修改开始时间，参考聚宽数据源的最早数据时间

#### 示例

`./QuadQuanta/examples/stock_picking.py`文件为一个简易的选股示例

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