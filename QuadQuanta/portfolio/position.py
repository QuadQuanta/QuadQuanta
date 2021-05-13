#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   position.py
@Time    :   2021/05/11
@Author  :   levonwoo
@Version :   0.1
@Contact :   
@License :   (C)Copyright 2020-2021
@Desc    :   股票持仓模型
'''

# here put the import lib
import uuid


class Position():
    def __init__(
        self,
        code='000001',
        volume_long_today=0,
        volume_long_history=0,
        position_cost=0,
        volume_long_frozen=0,
        positon_id=None,
    ):
        """
        持仓模型初始化
        """
        self.code = code
        self.position_id = str(
            uuid.uuid4()) if positon_id is None else positon_id  # 生成唯一持仓id

        self.volume_long_today = volume_long_today
        self.volume_long_history = volume_long_history
        self.position_cost = position_cost
        self.volume_long_frozen = volume_long_frozen
        self.last_price = 0  # 持仓最新价格

    def __repr__(self) -> str:
        return 'Positon: {} volume: {} avaliable:{} float_profit:{}'.format(
            self.code, self.volume_long, self.volume_long_history,
            self.float_profit)

    @property
    def volume_long(self):
        """
        实际持仓
        """
        return self.volume_long_today + self.volume_long_history - self.volume_long_frozen

    @property
    def float_profit(self):
        """
        浮动盈亏
        """
        return self.volume_long * self.last_price - self.position_cost

    def on_price_change(self, price):
        """
        更新价格

        Parameters
        ----------
        price : [type]
            [description]
        """
        self.last_price = price

    def settle(self):
        """
        收盘后结算
        """
        self.volume_long_history += self.volume_long_today
        self.volume_long_today = 0
