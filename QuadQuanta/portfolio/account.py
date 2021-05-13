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


class Account():
    """[summary]
    """
    def __init__(
        self,
        username=None,
        passwd=None,
        model='backtest',
        total_cash=100000,
    ):

        self.username = username
        self.passwd = passwd
        self.model = model
        self.total_cash = total_cash
        self.frozen_cash = 0
        self.orders = {}
        self.positions = {}

    def __repr__(self) -> str:
        return 'print account'

    @property
    def available_cash(self):
        return self.total_cash - self.frozen_cash

    def send_order(self,
                   code,
                   volume,
                   price,
                   order_direction,
                   order_id=None,
                   datetime=None):
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
        datetime : [type]
            下单时间
        """
        order_id = str(uuid.uuid4()) if order_id == None else order_id
        if self.order_check(code, volume, price, order_direction):
            order = {
                'order_time': datetime,  # 下单时间
                'instrument_id': code,
                'price': price,
                'volume': volume,
                'amount': price * volume,  # 需要的资金
                'direction': order_direction,
                'order_id': order_id,
                'last_msg': "已报",
            }
            self.orders[order_id] = order
            return order

    def order_check(self, code, volume, price, order_direction):
        """
        订单预处理, 账户逻辑，卖出数量小于可卖出数量，买入数量对应的金额小于资金余额，买入价格

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
        res = False
        pos = self.get_position(code)
        if order_direction == 'buy':
            if self.available_cash >= volume * price:  # 可用资金大于买入需要资金
                self.frozen_cash += volume * price  # 冻结资金
                res = True
        elif order_direction == 'sell':
            if pos.volume_long_history >= volume:  # 可卖数量大于卖出数量
                pos.volume_long_frozen += volume
                res = True
        else:
            raise NotImplementedError

        return res

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
                              order_id=order['order_id'])

    def process_deal(self,
                     code,
                     trade_price,
                     trade_volume,
                     trade_amount,
                     order_direction,
                     order_id=None,
                     trade_id=None):
        if order_id in self.orders.keys():
            #
            order = self.orders[order_id]
            # 默认全部成交
            # 买入/卖出逻辑
            if order_direction == "buy":
                self.frozen_cash -= trade_amount
                self.total_cash -= trade_amount
                self.get_position(code).volume_long_today += trade_volume
            elif order_direction == "sell":
                self.get_position(code).volume_long_frozen -= trade_volume
                self.get_position(code).volume_long_history -= trade_volume
                self.total_cash += trade_amount
            else:
                raise NotImplementedError

    def settle(self):
        for item in self.positions.values():
            item.settle()


if __name__ == "__main__":
    acc = Account('test', 'test')
    od = acc.send_order('000001',
                        100,
                        12,
                        'buy',
                        datetime='2020-01-10 09:32:00')
    acc.make_deal(od)
    od2 = acc.send_order('000001',
                         100,
                         12,
                         'buy',
                         datetime='2020-01-10 09:33:00')
    acc.make_deal(od2)
    pos = acc.get_position()
    print(pos)
    acc.settle()
    od3 = acc.send_order('000001',
                         100,
                         12,
                         'sell',
                         datetime='2020-01-10 09:34:00')
    acc.make_deal(od3)
    print(pos)
