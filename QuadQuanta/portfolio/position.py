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
        volume_today_long=0,
        volume_history_long=0,
        position_cost=0,
        volume_long_frozen=0,
        positon_id=None,
    ) -> None:
        """
        持仓模型初始化
        """
        self.code = code
        self.position_id = str(
            uuid.uuid4()) if positon_id is None else positon_id  # 生成唯一持仓id

        self.volume_today_long = volume_today_long
        self.volume_history_long = volume_history_long
        self.position_cost = position_cost
        self.volume_long_frozen = volume_long_frozen
        self.last_price = 0  # 持仓最新价格

    def __repr__(self) -> str:
        return 'print position'

    @property
    def volume_long(self):
        """
        实际持仓
        """
        return self.volume_today_long + self.volume_history_long - self.volume_long_frozen

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
