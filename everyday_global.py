# 每日大盘信息（不分股票）
import jqdatasdk as jq
from conf.common_conf import *
from conf.jq_factor_conf import *
from conf.security_conf import *
from utils.utilization import *
import pandas as pd
from datetime import datetime, timedelta
from multiprocessing import Pool
import time
import os


# 获取上海、深圳每日交易信息
# total_market_cap(总市值列表)、circulating_market_cap(流通市值列表)、volume(成交股数列表)、money(成交金额列表)、deal_number(交易笔数列表)、pe_average(平均市盈率列表)、turnover_ratio(换手率列表)
def get_sz_sh_trade_info(date):
    # 总市值、流通市值、成交股数、成交金额、交易笔数、平均市盈率、换手率
    fields = ["total_market_cap", "circulating_market_cap", "volume", "money", "deal_number", "pe_average", "turnover_ratio"]
    # 市场代码，分别为上海市场、深圳市场、深圳主板、中小企业板、创业板
    market_code_list = ["322001", "322004", "322005", "322006", "322007"]
    field_data_dict = {field: [] for field in fields}
    empty_df = pd.DataFrame(field_data_dict)
    table = jq.finance.STK_EXCHANGE_TRADE_INFO
    for market_code in market_code_list:
        cond1 = jq.finance.STK_EXCHANGE_TRADE_INFO.exchange_code == market_code
        cond2 = jq.finance.STK_EXCHANGE_TRADE_INFO.date == date
        query = jq.query(table).filter(cond1, cond2).limit(10)
        data_df = jq.finance.run_query(query)
        for field in fields:
            field_data_list = data_df[field].values.tolist()
            if len(field_data_list) != 1:
                return empty_df
            field_data_dict[field].append(field_data_list[0])
    result_df = pd.DataFrame({field: "|".join(list(map(lambda x: str(x), field_data_dict[field]))) for field in fields}, index=[0])
    return result_df


# 获取市场通每日交易信息
# buy_amount(买入金额列表)、buy_volume(买入股数列表)、sell_amount(卖出金额列表)、sell_volume(卖出股数列表)，前者人民币，后者港币
def get_sct_trade_info(date):
    # 买入金额、买入股数、卖出金额、卖出股数
    fields = ["buy_amount", "buy_volume", "sell_amount", "sell_volume"]
    # 市场通代码，分别为沪股通、港股通（沪）
    market_code_list = ["310001", "310003"]
    field_data_dict = {field: [] for field in fields}
    empty_df = pd.DataFrame(field_data_dict)
    table = jq.finance.STK_ML_QUOTA
    for market_code in market_code_list:
        cond1 = jq.finance.STK_ML_QUOTA.link_id == market_code
        cond2 = jq.finance.STK_ML_QUOTA.day == date
        query = jq.query(table).filter(cond1, cond2).limit(10)
        data_df = jq.finance.run_query(query)
        for field in fields:
            field_data_list = data_df[field].values.tolist()
            if len(field_data_list) != 1:
                return empty_df
            field_data_dict[field].append(field_data_list[0])
    result_df = pd.DataFrame({field: "|".join(list(map(lambda x: str(x), field_data_dict[field]))) for field in fields}, index=[0])
    return result_df


# 获取市场通每日汇率信息
# refer_bid_rate(买入参考汇率)、refer_ask_rate(卖出参考汇率)、settle_bid_rate(买入结算汇率)、settle_ask_rate(卖出结算汇率)
def get_sct_rate(date):
    fields = ["refer_bid_rate", "refer_ask_rate", "settle_bid_rate", "settle_ask_rate"]
    table = jq.finance.STK_EXCHANGE_LINK_RATE
    cond1 = jq.finance.STK_EXCHANGE_LINK_RATE.link_id == "310003"   # 港股通（沪）
    cond2 = jq.finance.STK_EXCHANGE_LINK_RATE.day == date
    query = jq.query(table).filter(cond1, cond2).limit(10)
    data_df = jq.finance.run_query(query)
    if len(data_df) != 1:
        return pd.DataFrame({field: [] for field in fields})
    result_df = data_df[fields]
    return result_df


def get_day_info(date):
    df_list = []
    df_list.append(get_sz_sh_trade_info(date))
    df_list.append(get_sct_trade_info(date))
    df_list.append(get_sct_rate(date))
    df = pd.concat(df_list, axis=1)
    print()


if __name__ == "__main__":
    auth(USER_NAME, PASSWORD)
    get_day_info("2015-03-02")