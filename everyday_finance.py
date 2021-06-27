# 每日每股财务信息
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


# 获取单一股票每日市值数据
# 传入date时, 查询指定日期date所能看到的最近(对市值表来说, 最近一天, 对其他表来说, 最近一个季度)的数据, 我们会查找上市公司在这个日期之前(包括此日期)发布的数据, 不会有未来函数
# 回测模块: 默认值会随着回测日期变化而变化, 等于context.current_dt的前一天(实际生活中我们只能看到前一天的财报和市值数据, 所以要用前一天)
def get_valuation(code_list, date):
    # 市盈率TTM、换手率、市净率、市销率TTM、市现率TTM、总股本、总市值、流通股本、流通市值、市盈率
    fields = ["pe_ratio", "turnover_ratio", "pb_ratio", "ps_ratio", "pcf_ratio", "capitalization", "market_cap", "circulating_cap", "circulating_market_cap", "pe_ratio_lyr"]
    table = jq.valuation
    cond = jq.valuation.code.in_(code_list)
    order = jq.valuation.code.asc()
    query = jq.query(table).filter(cond).order_by(order)
    data_df = jq.get_fundamentals(query, date=date)
    if data_df.empty:
        return pd.DataFrame({field: [] for field in ["security"] + fields + ["date"]})
    # 字段重命名
    result_df = data_df.rename(columns={"code": "security", "day": "date"})
    # 调整字段顺序
    result_df = result_df[["security"] + fields + ["date"]]
    return result_df


# code_list = ["000002.XSHE", "000012.XSHE", "000022.XSHE", "300015.XSHE", "300016.XSHE"]
# a1 = get_valuation(code_list, "2018-03-02")
# print()


# 获取单一股票每日财务指标数据
def get_finance_indicator(code_list, date):
    # https://www.joinquant.com/help/api/help#Stock:%E8%B4%A2%E5%8A%A1%E6%8C%87%E6%A0%87%E6%95%B0%E6%8D%AE
    fields = ["eps", "adjusted_profit", "operating_profit", "value_change_profit", "roe", "inc_return", "roa", "net_profit_margin", "gross_profit_margin", "expense_to_total_revenue", "operation_profit_to_total_revenue", "net_profit_to_total_revenue", "operating_expense_to_total_revenue", "ga_expense_to_total_revenue", "financing_expense_to_total_revenue", "operating_profit_to_profit", "invesment_profit_to_profit", "adjusted_profit_to_profit", "goods_sale_and_service_to_revenue", "ocf_to_revenue", "ocf_to_operating_profit", "inc_total_revenue_year_on_year", "inc_total_revenue_annual", "inc_revenue_year_on_year", "inc_revenue_annual", "inc_operation_profit_year_on_year", "inc_operation_profit_annual", "inc_net_profit_year_on_year", "inc_net_profit_annual", "inc_net_profit_to_shareholders_year_on_year", "inc_net_profit_to_shareholders_annual"]
    table = jq.indicator
    cond = jq.indicator.code.in_(code_list)
    order = jq.indicator.code.asc()
    query = jq.query(table).filter(cond).order_by(order)
    data_df = jq.get_fundamentals(query, date=date)
    if data_df.empty:
        return pd.DataFrame({field: [] for field in ["security"] + fields + ["date"]})
    # 字段重命名
    result_df = data_df.rename(columns={"code": "security", "day": "date"})
    # 调整字段顺序
    result_df = result_df[["security"] + fields + ["date"]]
    return result_df


# code_list = ["000002.XSHE", "000012.XSHE", "000022.XSHE", "300015.XSHE", "300016.XSHE"]
# a1 = get_finance_indicator(code_list, "2018-03-02")
# print()


# 获取股票每日财务指标数据
def get_income(code_list, date):
    # 净利润、营业利润、利润总额、归属于母公司股东的净利润、营业收入、营业总收入、营业总成本、基本每股收益、稀释每股收益
    fields = ["net_profit", "operating_profit", "total_profit", "np_parent_company_owners", "operating_revenue", "total_operating_revenue", "total_operating_cost", "basic_eps", "diluted_eps"]
    table = jq.income
    cond = jq.income.code.in_(code_list)
    order = jq.income.code.asc()
    query = jq.query(table).filter(cond).order_by(order)
    data_df = jq.get_fundamentals(query, date=date)
    if data_df.empty:
        return pd.DataFrame({field: [] for field in ["security"] + fields + ["date"]})
    # 字段重命名
    result_df = data_df.rename(columns={"code": "security", "day": "date"})
    # 调整字段顺序
    result_df = result_df[["security"] + fields + ["date"]]
    return result_df


# code_list = ["000002.XSHE", "000012.XSHE", "000022.XSHE", "300015.XSHE", "300016.XSHE"]
# a1 = get_income(code_list, "2018-03-02")
# print()




