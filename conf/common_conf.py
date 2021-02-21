# coding: utf-8

# 调试
DEBUG_MODE = False

USER_NAME = "15010149322"
PASSWORD = "149322"

PRICE_FIELDS = [
    "open",         # 开盘价
    "close",        # 收盘价
    "low",          # 最低价
    "high",         # 最高价
    "volume",       # 成交数
    "money",        # 成交金额
    "factor",       # 前复权因子, 我们提供的价格都是前复权后的, 但是利用这个值可以算出原始价格, 方法是价格除以 factor, 比如: close/factor
    "high_limit",   # 涨停价
    "low_limit",    # 跌停价
    "avg",          # 平均价, 等于 money/volume
    "pre_close",    # 前一个单位时间结束时的价格, 按天则是前一天的收盘价, 按分钟这是前一分钟的结束价格
    "paused",       # 是否停牌, 停牌时 open/close/low/high/pre_close 依然有值,都等于停牌前的收盘价, volume=money=0
 ]

TICK_FIELDS = [
    "time",
    "current",
    "high",
    "low",
    "volume",
    "money",
]


