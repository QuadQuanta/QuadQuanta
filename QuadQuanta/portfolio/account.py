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

from QuadQuanta.portfolio import Position


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

        self.username = username
        self.passwd = passwd
        self.model = model
        self.init_cash = init_cash
        self.orders = {}
        self.positions = {}

    def __repr__(self) -> str:
        return 'print account'

    def send_order(self, code, volume, price, order_direction, order_id,
                   datetime):
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
        order_id = str(uuid.uuid4()) if order_id else order_id
        if self.order_check(code, volume, price, order_direction):
            order = {
                'order_time': datetime,  # 下单时间
                'instrument_id': code,
                'price': price,
                'volume': volume,
                'direction': order_direction,
                'last_msg': "已报",
            }
            self.orders[order_id] = order

    def order_check(self, code, volume, price, order_direction):
        raise NotImplementedError

    def cancel_order(self, order_id):
        """
        撤单, 释放冻结

        Parameters
        ----------
        order_id : uuid
            唯一订单id
        """
        pass

    def get_position(self, code):
        """
        获取某个标的持仓对象

        Parameters
        ----------
        code : str
            标的代码
        """
        try:
            return self.positions[code]
        except KeyError:
            return Position(code)
