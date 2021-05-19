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
            init_cash=100000,
    ):
        self.init_cash = init_cash
        self.username = username
        self.passwd = passwd
        self.model = model
        self.available_cash = init_cash
        self.orders = {}
        self.positions = {}

    def __repr__(self) -> str:
        return 'print account'

    @property
    def total_cash(self):
        return self.available_cash + self.frozen_cash

    @property
    def frozen_cash(self):
        return sum([position.frozen_cash for position in self.positions.values()])

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
        res = False
        pos = self.get_position(code)
        pos.on_price_change(price)
        if order_direction == 'buy':
            if self.available_cash >= volume * price:  # 可用资金大于买入需要资金
                pos.frozen_cash += volume * price
                # 可用现金减少
                self.available_cash -= volume * price
                res = True
        elif order_direction == 'sell':
            if pos.volume_long_history >= volume:  # 可卖数量大于卖出数量
                # 历史持仓减少，冻结持仓增加
                pos.volume_long_history -= volume
                pos.volume_short_frozen += volume
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
        pos = self.get_position(code)
        pos.on_price_change(trade_price)
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
                pos.volume_long_history -= trade_volume
                pos.volume_short_today += trade_volume
                self.available_cash += trade_amount

                # 成本减少
                pos.position_cost -= trade_amount
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
    pos.on_price_change(13)

    acc.settle()
    print(pos)
    od3 = acc.send_order('000001',
                         100,
                         14,
                         'sell',
                         datetime='2020-01-10 09:34:00')
    acc.make_deal(od3)
    print(pos)
    print(acc.total_market_value)
    print(acc.total_assets)
