from jqdatasdk import *
from conf.common_conf import *
from utils.utilization import *
import pandas as pd
from datetime import datetime, timedelta
import math


auth(USER_NAME, PASSWORD)
TRADE_DAYS = set(date.strftime("%Y-%m-%d") for date in get_all_trade_days().tolist())


def get_basic_attributes(security_list, start_date, end_date, price_fields, frequency, fq):
    # (field_size, unit_num, stock_num)
    panel = get_price(security=security_list,
                      start_date=start_date,
                      end_date=end_date,
                      fields=price_fields,
                      frequency=frequency,
                      fq=fq
                      )
    # (stock_num, field_size, unit_num)
    params = panel.values.swapaxes(1, 2).swapaxes(0, 1)
    return params


# 把开始和结束时间区间分割成多个 list
def get_span_list(start_date, end_date, span):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    date_pair_list = []
    current_date = start_date + timedelta(days=span-1)
    while current_date < end_date:
        date_pair_list.append([start_date.strftime("%Y-%m-%d"), current_date.strftime("%Y-%m-%d")])
        start_date += timedelta(days=span)
        current_date += timedelta(days=span)
    else:
        date_pair_list.append([start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")])
    return date_pair_list


# 把开始和结束时间区间转化成日期列表，以适配 alpha101、alpha191 入参
def get_date_list(start_date, end_date):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    date_list = []
    while start_date <= end_date:
        date_list.append(start_date.strftime("%Y-%m-%d"))
        start_date += timedelta(days=1)
    return date_list


def get_trade_day_list(start_date, end_date):
    date_list = get_trade_days(start_date=start_date, end_date=end_date).tolist()
    return [date.strftime("%Y-%m-%d") for date in date_list]


def get_adjacent_trade_day(date, n):
    if isinstance(date, str):
        date = datetime.strptime(date, "%Y-%m-%d")
    delta = -1 if n < 0 else 1
    adjacent_date = date + timedelta(days=n)

    for _ in range(len(TRADE_DAYS)):
        adjacent_date_str = adjacent_date.strftime("%Y-%m-%d")
        if adjacent_date_str in TRADE_DAYS:
            return adjacent_date_str
        else:
            adjacent_date += timedelta(days=delta)
    return None


def batchify(security_list, batch_size):
    num = len(security_list)
    batch_num = int(math.ceil(num / batch_size))
    batch_list = []
    for i in range(batch_num):
        batch = security_list[i * batch_size: (i + 1) * batch_size]
        batch_list.append(batch)
    return batch_list



