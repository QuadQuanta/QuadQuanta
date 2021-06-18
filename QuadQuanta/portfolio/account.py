#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   account.py
@Time    :   2021/05/11
@Author  :   levonwoo
@Version :   0.1
@Contact :   
@License :   (C)Copyright 2020-2021
@Desc    :   账户模块
'''

# here put the import lib
import uuid

from QuadQuanta.portfolio.position import Position
from QuadQuanta.data.mongodb_api import insert_mongodb


class Account():
    """[summary]
    """
    def __init__(self,
                 username=None,
                 passwd=None,
                 model='backtest',
                 init_cash=100000,
                 account_id=None,
                 mongo_db='QuadQuanta',
                 mongo_col='account',
                 solid=False):
        self.init_cash = init_cash
        self.username = username
        self.passwd = passwd
        self.model = model
        self.available_cash = init_cash
        self.orders = {}
        self.positions = {}
        # 印花税
        self.stamp_duty = 0.001
        # 手续费
        self.handle_fee = 0.0001

        self.datetime = ""
        self.account_id = str(
            uuid.uuid4()) if account_id is None else account_id
        # mongodb数据库名和集合名
        self.mongo_db = mongo_db
        self.mongo_col = mongo_col
        # 是否固化到mongodb选项
        self.solid = solid

    def __repr__(self) -> str:
        return 'print account'

    @property
    def total_cash(self):
        return self.available_cash + self.frozen_cash

    @property
    def frozen_cash(self):
        return sum(
            [position.frozen_cash for position in self.positions.values()])

    @property
    def float_profit(self):
        return sum(
            [position.float_profit for position in self.positions.values()])

    @property
    def profit_ratio(self):
        return round(
            100 * (self.total_assets - self.init_cash) / self.init_cash, 2)

    @property
    def total_assets(self):
        """
        总资产
        """
        return self.total_cash + self.total_market_value

    @property
    def total_market_value(self):
        """
        股票总市值
        """
        return sum(
            [position.market_value for position in self.positions.values()])

    def send_order(self,
                   code,
                   volume,
                   price,
                   order_direction,
                   order_id=None,
                   order_time=None):
        """[summary]
        下单函数
        Parameters
        ----------
        code : str
            六位数股票代码
        volume : int
            股票数量
        price : float
            价格
        order_direction : [type]
            买入/卖出
        order_time : [type]
            下单时间
        """
        if order_time:
            self.datetime = order_time
        order_id = str(uuid.uuid4()) if order_id == None else order_id
        checked_order = self.order_check(code, volume, price, order_direction)
        checked_order['order_time'] = order_time
        checked_order['order_id'] = order_id
        self.orders[order_id] = checked_order
        return checked_order

    def order_check(self, code, volume, price, order_direction):
        """
        订单预处理, 账户逻辑，卖出数量小于可卖出数量，
        买入数量对应的金额小于资金余额，买入价格

        Parameters
        ----------
        code : [type]
            [description]
        volume : [type]
            [description]
        price : [type]
            [description]
        order_direction : [type]
            [description]

        """
        pos = self.get_position(code)
        pos.update_pos(price, self.datetime)
        if order_direction == 'buy':
            if self.available_cash >= volume * price:  # 可用资金大于买入需要资金
                volume = volume
            else:
                volume = 100 * int(self.available_cash // (100 * price))
            amount = volume * price * (1 + self.handle_fee)
            pos.frozen_cash += amount
            # 可用现金减少
            self.available_cash -= amount
            order = {
                'instrument_id': code,
                'price': price,
                'volume': volume,
                'amount': amount,  # 需要的资金
                'direction': order_direction,
                'last_msg': "已报",
            }

        elif order_direction == 'sell':
            if pos.volume_long_history >= volume:  # 可卖数量大于卖出数量
                volume = volume
            else:
                volume = pos.volume_long_history

            amount = volume * price * (1 - self.handle_fee - self.stamp_duty)
            # 历史持仓减少，冻结持仓增加
            pos.volume_long_history -= volume
            pos.volume_short_frozen += volume

            order = {
                'instrument_id': code,
                'price': price,
                'volume': volume,
                'amount': amount,
                'direction': order_direction,
                'last_msg': "已报",
            }

        else:
            raise NotImplementedError

        return order

    def cancel_order(self, order_id):
        """
        撤单, 释放冻结

        Parameters
        ----------
        order_id : uuid
            唯一订单id
        """
        pass

    def get_position(self, code=None) -> Position:
        """
        获取某个标的持仓对象

        Parameters
        ----------
        code : str
            标的代码
        """
        if code is None:
            return list(self.positions.values())[0]
        try:
            return self.positions[code]
        except KeyError:
            pos = Position(code)
            self.positions[code] = pos
            return self.positions[code]

    def make_deal(self, order):
        """
        撮合

        Parameters
        ----------
        order : [type]
            [description]
        """
        if isinstance(order, dict):
            self.process_deal(code=order['instrument_id'],
                              trade_price=order['price'],
                              trade_volume=order['volume'],
                              trade_amount=order['amount'],
                              order_direction=order['direction'],
                              order_id=order['order_id'],
                              order_time=order['order_time'])

    def process_deal(self,
                     code,
                     trade_price,
                     trade_volume,
                     trade_amount,
                     order_direction,
                     order_id=None,
                     order_time=None,
                     trade_id=None):
        pos = self.get_position(code)
        pos.update_pos(trade_price, order_time)
        if order_id in self.orders.keys():
            #
            order = self.orders[order_id]
            # 默认全部成交
            # 买入/卖出逻辑
            if order_direction == "buy":
                # 冻结资金转换为持仓
                pos.frozen_cash -= trade_amount
                pos.volume_long_today += trade_volume
                # 成本增加
                pos.position_cost += trade_amount
                pos.open_cost += trade_amount
            elif order_direction == "sell":
                # 冻结持仓转换为可用资金
                pos.volume_short_frozen -= trade_volume
                pos.volume_short_today += trade_volume
                self.available_cash += trade_amount

                # 成本减少
                pos.position_cost -= trade_amount
            else:
                raise NotImplementedError

    @property
    def account_info(self):
        return {
            'cash': self.total_cash,
            'market_value': self.total_market_value,
            'assert': self.total_assets
        }

    @property
    def positions_msg(self):
        return [
            position.static_message for position in self.positions.values()
            if position.volume_long + position.volume_short_today > 0
        ]

    @property
    def account_section(self):
        return {
            'account_id': self.account_id,
            'date': self.datetime,
            'account': self.account_info,
            'positions': self.positions_msg,
            'orders': self.orders,
        }

    def save_account_section(self):
        insert_mongodb(self.mongo_db, self.mongo_col, self.account_section)

    def settle(self):
        if self.solid:
            self.save_account_section()
        self.orders = {}
        for item in self.positions.values():
            item.settle()


if __name__ == "__main__":
    acc = Account('test', 'test')
    od = acc.send_order('000001',
                        100,
                        12,
                        'buy',
                        order_time='2020-01-10 09:32:00')
    acc.make_deal(od)

    od2 = acc.send_order('000002',
                         100,
                         12,
                         'buy',
                         order_time='2020-01-10 09:33:00')
    acc.make_deal(od2)
    print(acc.positions_msg)
    acc.settle()
    # print(pos)
    od3 = acc.send_order('000001',
                         100,
                         14,
                         'sell',
                         order_time='2020-01-11 09:34:00')
    acc.make_deal(od3)

    acc.settle()
    print(acc.positions_msg)
    # print(pos)
    # print(acc.total_market_value)
