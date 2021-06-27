# mysql 相关
import jqdatasdk as jq
from conf.common_conf import *
from conf.jq_factor_conf import *
from conf.security_conf import *
from conf.mysql_conf import *
from utils.utilization import *
import pandas as pd
from datetime import datetime, timedelta
from multiprocessing import Pool
import time
import os
from aperiodical_info import *
from everyday_finance import *
from everyday_industry_and_concept import *
from everyday_global import *
from everyday_price import *
import mysql.connector
from sqlalchemy import create_engine
import numpy as np


def mysql_connect():
    url = "mysql+mysqlconnector://{}:{}@{}:{}/{}?charset={}".format(MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE, MYSQL_CHARSET)
    engine = create_engine(url)
    return engine


def create_day_price_table(engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS `day_price` (
        `record_id` INT NOT NULL AUTO_INCREMENT COMMENT '自增id',
        `security` VARCHAR (15) NOT NULL COMMENT '股票代码',
        `open` TEXT DEFAULT NULL COMMENT '分钟开始价格',
        `close` TEXT DEFAULT NULL COMMENT '分钟结束价格',
        `low` TEXT DEFAULT NULL COMMENT '分钟最低价',
        `high` TEXT DEFAULT NULL COMMENT '分钟最高价',
        `avg` TEXT DEFAULT NULL COMMENT '分钟平均价',
        `factor` TEXT DEFAULT NULL COMMENT '复权因子',
        `volume` TEXT DEFAULT NULL COMMENT '分钟成交股数',
        `money` TEXT DEFAULT NULL COMMENT '分钟成交金额',
        `paused` TEXT DEFAULT NULL COMMENT '是否停牌',
        `date` DATE NOT NULL COMMENT '日期',
        PRIMARY KEY (`record_id`),
        UNIQUE `day_price_security_date_index` (`security`, `date`),
        INDEX `day_price_security_index` (`security`),
        INDEX `day_price_date_index` (`date`)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '每日行情';
    """
    engine.execute(create_table_sql)


def update_day_price_table(security_list, start_date, end_date):
    table_name = "day_price"
    engine = mysql_connect()
    create_day_price_table(engine)
    date_list = get_trade_day_list(start_date=start_date, end_date=end_date)
    retrieve_index_sql = "select security, date from {}".format(table_name)
    index_df = pd.read_sql_query(retrieve_index_sql, engine)
    index_set = set((index_df["security"] + " " + index_df["date"].map(str)).tolist())
    for date in date_list:
        for security in security_list:
            df = get_day_price(security, date)
            if len(df) == 0 or (df["security"] + " " + df["date"].map(str)).tolist()[0] in index_set:
                continue
            df.to_sql(table_name, engine, index=False, if_exists="append")


def batch_update_day_price_table(security_list, start_date, end_date):
    # 不限制返回数量
    table_name = "day_price"
    engine = mysql_connect()
    create_day_price_table(engine)
    date_list = get_trade_day_list(start_date=start_date, end_date=end_date)
    retrieve_index_sql = "select security, date from {}".format(table_name)
    index_df = pd.read_sql_query(retrieve_index_sql, engine)
    index_set = set((index_df["security"] + " " + index_df["date"].map(str)).tolist())
    now = time.time()
    for date in date_list:
        df = get_securities_day_price(security_list, date)
        if len(df) == 0:
            continue
        concat_index_col = df["security"] + " " + df["date"]
        keep_row = concat_index_col.apply(lambda x: True if x not in index_set else False)
        df = df[keep_row]
        if len(df) == 0:
            continue
        df.to_sql(table_name, engine, index=False, if_exists="append")
    cost_time = time.time() - now
    print(cost_time)


def create_day_mtss_table(engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS `day_mtss` (
        `record_id` INT NOT NULL AUTO_INCREMENT COMMENT '自增id',
        `security` VARCHAR (15) NOT NULL COMMENT '股票代码',
        `fin_value` DOUBLE DEFAULT NULL COMMENT '融资余额',
        `fin_buy_value` DOUBLE DEFAULT NULL COMMENT '融资买入额',
        `fin_refund_value` DOUBLE DEFAULT NULL COMMENT '融资偿还额',
        `sec_value` DOUBLE DEFAULT NULL COMMENT '融券余量',
        `sec_sell_value` DOUBLE DEFAULT NULL COMMENT '融券卖出量',
        `sec_refund_value` DOUBLE DEFAULT NULL COMMENT '融券偿还量',
        `fin_sec_value` DOUBLE DEFAULT NULL COMMENT '融资融券余额',
        `date` DATE NOT NULL COMMENT '日期',
        PRIMARY KEY (`record_id`),
        UNIQUE `day_mtss_security_date_index` (`security`, `date`),
        INDEX `day_mtss_security_index` (`security`),
        INDEX `day_mtss_date_index` (`date`)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '每日融资融券信息';
    """
    engine.execute(create_table_sql)


def batch_update_day_mtss_table(security_list, start_date, end_date):
    # 不限制返回数量
    table_name = "day_mtss"
    engine = mysql_connect()
    create_day_price_table(engine)
    date_list = get_trade_day_list(start_date=start_date, end_date=end_date)
    retrieve_index_sql = "select security, date from {}".format(table_name)
    index_df = pd.read_sql_query(retrieve_index_sql, engine)
    index_set = set((index_df["security"] + " " + index_df["date"].map(str)).tolist())
    for date in date_list:
        df = get_securities_day_mtss(security_list, date)
        if len(df) == 0:
            continue
        concat_index_col = df["security"] + " " + df["date"]
        keep_row = concat_index_col.apply(lambda x: True if x not in index_set else False)
        df = df[keep_row]
        if len(df) == 0:
            continue
        df.to_sql(table_name, engine, index=False, if_exists="append")


def create_day_call_auction_table(engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS `day_call_auction` (
        `record_id` INT NOT NULL AUTO_INCREMENT COMMENT '自增id',
        `security` VARCHAR (15) NOT NULL COMMENT '股票代码',
        `av` TEXT DEFAULT NULL COMMENT '五档卖量',
        `ap` TEXT DEFAULT NULL COMMENT '五档卖价',
        `bv` TEXT DEFAULT NULL COMMENT '五档买量',
        `bp` TEXT DEFAULT NULL COMMENT '五档买价',
        `date` DATE NOT NULL COMMENT '日期',
        PRIMARY KEY (`record_id`),
        UNIQUE `day_call_auction_security_date_index` (`security`, `date`),
        INDEX `day_call_auction_security_index` (`security`),
        INDEX `day_call_auction_date_index` (`date`)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '每日盘前集合竞价';
    """
    engine.execute(create_table_sql)


def batch_update_day_call_auction_table(security_list, start_date, end_date):
    # 不限制返回数量
    table_name = "day_call_auction"
    engine = mysql_connect()
    create_day_call_auction_table(engine)
    date_list = get_trade_day_list(start_date=start_date, end_date=end_date)
    retrieve_index_sql = "select security, date from {}".format(table_name)
    index_df = pd.read_sql_query(retrieve_index_sql, engine)
    index_set = set((index_df["security"] + " " + index_df["date"].map(str)).tolist())
    for date in date_list:
        df = get_securities_day_call_auction(security_list, date)
        if len(df) == 0:
            continue
        concat_index_col = df["security"] + " " + df["date"]
        keep_row = concat_index_col.apply(lambda x: True if x not in index_set else False)
        df = df[keep_row]
        if len(df) == 0:
            continue
        df.to_sql(table_name, engine, index=False, if_exists="append")


def create_day_money_flow_table(engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS `day_money_flow` (
        `record_id` INT NOT NULL AUTO_INCREMENT COMMENT '自增id',
        `security` VARCHAR (15) NOT NULL COMMENT '股票代码',
        `change_pct` DOUBLE DEFAULT NULL COMMENT '涨跌幅',
        `net_amount_main` DOUBLE DEFAULT NULL COMMENT '主力净额',
        `net_pct_main` DOUBLE DEFAULT NULL COMMENT '主力净占比',
        `net_amount_xl` DOUBLE DEFAULT NULL COMMENT '超大单净额',
        `net_pct_xl` DOUBLE DEFAULT NULL COMMENT '超大单净占比',
        `net_amount_l` DOUBLE DEFAULT NULL COMMENT '大单净额',
        `net_pct_l` DOUBLE DEFAULT NULL COMMENT '打单净占比',
        `net_amount_m` DOUBLE DEFAULT NULL COMMENT '中单净额',
        `net_pct_m` DOUBLE DEFAULT NULL COMMENT '中单净占比',
        `net_amount_s` DOUBLE DEFAULT NULL COMMENT '小单净额',
        `net_pct_s` DOUBLE DEFAULT NULL COMMENT '小单净占比',
        `date` DATE NOT NULL COMMENT '日期',
        PRIMARY KEY (`record_id`),
        UNIQUE `day_money_flow_security_date_index` (`security`, `date`),
        INDEX `day_money_flow_security_index` (`security`),
        INDEX `day_money_flow_date_index` (`date`)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '每日资金流向';
    """
    engine.execute(create_table_sql)


def batch_update_day_money_flow_table(security_list, start_date, end_date):
    # 不限制返回数量
    table_name = "day_money_flow"
    engine = mysql_connect()
    create_day_money_flow_table(engine)
    date_list = get_trade_day_list(start_date=start_date, end_date=end_date)
    retrieve_index_sql = "select security, date from {}".format(table_name)
    index_df = pd.read_sql_query(retrieve_index_sql, engine)
    index_set = set((index_df["security"] + " " + index_df["date"].map(str)).tolist())
    for date in date_list:
        df = get_securities_day_money_flow(security_list, date)
        if len(df) == 0:
            continue
        concat_index_col = df["security"] + " " + df["date"]
        keep_row = concat_index_col.apply(lambda x: True if x not in index_set else False)
        df = df[keep_row]
        if len(df) == 0:
            continue
        df.to_sql(table_name, engine, index=False, if_exists="append")


def create_day_st_table(engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS `day_st` (
        `record_id` INT NOT NULL AUTO_INCREMENT COMMENT '自增id',
        `security` VARCHAR (15) NOT NULL COMMENT '股票代码',
        `is_st` TINYINT DEFAULT NULL COMMENT '是否ST',
        `date` DATE NOT NULL COMMENT '日期',
        PRIMARY KEY (`record_id`),
        UNIQUE `day_st_security_date_index` (`security`, `date`),
        INDEX `day_st_security_index` (`security`),
        INDEX `day_st_date_index` (`date`)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '每日ST信息';
    """
    engine.execute(create_table_sql)


def batch_update_day_st_table(security_list, start_date, end_date):
    # 不限制返回数量
    table_name = "day_st"
    engine = mysql_connect()
    create_day_st_table(engine)
    date_list = get_trade_day_list(start_date=start_date, end_date=end_date)
    retrieve_index_sql = "select security, date from {}".format(table_name)
    index_df = pd.read_sql_query(retrieve_index_sql, engine)
    index_set = set((index_df["security"] + " " + index_df["date"].map(str)).tolist())
    for date in date_list:
        df = get_securities_day_st(security_list, date)
        if len(df) == 0:
            continue
        concat_index_col = df["security"] + " " + df["date"]
        keep_row = concat_index_col.apply(lambda x: True if x not in index_set else False)
        df = df[keep_row]
        if len(df) == 0:
            continue
        df.to_sql(table_name, engine, index=False, if_exists="append")


def create_day_sct_share_table(engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS `day_sct_share` (
        `record_id` INT NOT NULL AUTO_INCREMENT COMMENT '自增id',
        `security` VARCHAR (15) NOT NULL COMMENT '股票代码',
        `share_number` INT DEFAULT NULL COMMENT '持股数量',
        `share_ratio` DOUBLE DEFAULT NULL COMMENT '持股比例',
        `date` DATE NOT NULL COMMENT '日期',
        PRIMARY KEY (`record_id`),
        UNIQUE `day_sct_share_security_date_index` (`security`, `date`),
        INDEX `day_sct_share_security_index` (`security`),
        INDEX `day_sct_share_date_index` (`date`)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '每日沪股通（北向资金）持股';
    """
    engine.execute(create_table_sql)


def batch_update_day_sct_share_table(security_list, start_date, end_date):
    max_return_rows = 2000
    table_name = "day_sct_share"
    engine = mysql_connect()
    create_day_sct_share_table(engine)
    date_list = get_trade_day_list(start_date=start_date, end_date=end_date)
    retrieve_index_sql = "select security, date from {}".format(table_name)
    index_df = pd.read_sql_query(retrieve_index_sql, engine)
    index_set = set((index_df["security"] + " " + index_df["date"].map(str)).tolist())
    for date in date_list:
        batch_list = batchify(security_list, max_return_rows)
        for batch in batch_list:
            df = get_securities_sct_share(batch, date)
            if len(df) == 0:
                continue
            concat_index_col = df["security"] + " " + df["date"]
            keep_row = concat_index_col.apply(lambda x: True if x not in index_set else False)
            df = df[keep_row]
            if len(df) == 0:
                continue
            df.to_sql(table_name, engine, index=False, if_exists="append")


def create_day_industry_table(engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS `day_industry` (
        `record_id` INT NOT NULL AUTO_INCREMENT COMMENT '自增id',
        `security` VARCHAR (15) NOT NULL COMMENT '股票代码',
        `sw_l1` TEXT DEFAULT NULL COMMENT '申万一级行业',
        `sw_l2` TEXT DEFAULT NULL COMMENT '申万二级行业',
        `sw_l3` TEXT DEFAULT NULL COMMENT '申万三级行业',
        `jq_l1` TEXT DEFAULT NULL COMMENT '聚宽一级行业',
        `jq_l2` TEXT DEFAULT NULL COMMENT '聚宽二级行业',
        `zjw` TEXT DEFAULT NULL COMMENT '证监会行业',
        `date` DATE NOT NULL COMMENT '日期',
        PRIMARY KEY (`record_id`),
        UNIQUE `day_industry_security_date_index` (`security`, `date`),
        INDEX `day_industry_security_index` (`security`),
        INDEX `day_industry_date_index` (`date`)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '每日股票所属行业信息';
    """
    engine.execute(create_table_sql)


def batch_update_day_industry_table(security_list, start_date, end_date):
    # 不限制返回数量
    table_name = "day_industry"
    engine = mysql_connect()
    create_day_industry_table(engine)
    date_list = get_trade_day_list(start_date=start_date, end_date=end_date)
    retrieve_index_sql = "select security, date from {}".format(table_name)
    index_df = pd.read_sql_query(retrieve_index_sql, engine)
    index_set = set((index_df["security"] + " " + index_df["date"].map(str)).tolist())
    for date in date_list:
        df = get_securities_day_industry(security_list, date)
        if len(df) == 0:
            continue
        concat_index_col = df["security"] + " " + df["date"]
        keep_row = concat_index_col.apply(lambda x: True if x not in index_set else False)
        df = df[keep_row]
        if len(df) == 0:
            continue
        df.to_sql(table_name, engine, index=False, if_exists="append")


def create_day_concept_table(engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS `day_concept` (
        `record_id` INT NOT NULL AUTO_INCREMENT COMMENT '自增id',
        `security` VARCHAR (15) NOT NULL COMMENT '股票代码',
        `concept_list` TEXT DEFAULT NULL COMMENT '概念列表',
        `date` DATE NOT NULL COMMENT '日期',
        PRIMARY KEY (`record_id`),
        UNIQUE `day_concept_security_date_index` (`security`, `date`),
        INDEX `day_concept_security_index` (`security`),
        INDEX `day_concept_date_index` (`date`)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '每日股票所属概念信息';
    """
    engine.execute(create_table_sql)


def batch_update_day_concept_table(security_list, start_date, end_date):
    # 不限制返回数量
    table_name = "day_concept"
    engine = mysql_connect()
    create_day_concept_table(engine)
    date_list = get_trade_day_list(start_date=start_date, end_date=end_date)
    retrieve_index_sql = "select security, date from {}".format(table_name)
    index_df = pd.read_sql_query(retrieve_index_sql, engine)
    index_set = set((index_df["security"] + " " + index_df["date"].map(str)).tolist())
    for date in date_list:
        df = get_securities_day_concept(security_list, date)
        if len(df) == 0:
            continue
        concat_index_col = df["security"] + " " + df["date"]
        keep_row = concat_index_col.apply(lambda x: True if x not in index_set else False)
        df = df[keep_row]
        if len(df) == 0:
            continue
        df.to_sql(table_name, engine, index=False, if_exists="append")


def create_day_sz_sh_trade_info_table(engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS `day_sz_sh_trade_info` (
        `record_id` INT NOT NULL AUTO_INCREMENT COMMENT '自增id',
        `total_market_cap` TEXT DEFAULT NULL COMMENT '总市值',
        `circulating_market_cap` TEXT DEFAULT NULL COMMENT '流通市值',
        `volume` TEXT DEFAULT NULL COMMENT '成交股数',
        `money` TEXT DEFAULT NULL COMMENT '成交金额',
        `deal_number` TEXT DEFAULT NULL COMMENT '交易笔数',
        `pe_average` TEXT DEFAULT NULL COMMENT '平均市盈率',
        `turnover_ratio` TEXT DEFAULT NULL COMMENT '换手率',
        `date` DATE NOT NULL COMMENT '日期',
        PRIMARY KEY (`record_id`),
        UNIQUE `day_sz_sh_trade_info_date_index` (`date`)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '每日上海、深圳交易信息';
    """
    engine.execute(create_table_sql)


def batch_update_day_sz_sh_trade_info_table(start_date, end_date):
    # 不限制返回数量
    table_name = "day_sz_sh_trade_info"
    engine = mysql_connect()
    create_day_sz_sh_trade_info_table(engine)
    date_list = get_trade_day_list(start_date=start_date, end_date=end_date)
    retrieve_index_sql = "select date from {}".format(table_name)
    index_df = pd.read_sql_query(retrieve_index_sql, engine)
    index_set = set(index_df["date"].map(str).tolist())
    for date in date_list:
        df = get_sz_sh_trade_info(date)
        if len(df) == 0:
            continue
        index_col = df["date"]
        keep_row = index_col.apply(lambda x: True if x not in index_set else False)
        df = df[keep_row]
        if len(df) == 0:
            continue
        df.to_sql(table_name, engine, index=False, if_exists="append")


def create_day_sct_trade_info_table(engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS `day_sct_trade_info` (
        `record_id` INT NOT NULL AUTO_INCREMENT COMMENT '自增id',
        `buy_amount` TEXT DEFAULT NULL COMMENT '买入金额',
        `buy_volume` TEXT DEFAULT NULL COMMENT '买入股数',
        `sell_amount` TEXT DEFAULT NULL COMMENT '卖出金额',
        `sell_volume` TEXT DEFAULT NULL COMMENT '卖出股数',
        `date` DATE NOT NULL COMMENT '日期',
        PRIMARY KEY (`record_id`),
        UNIQUE `day_sct_trade_info_date_index` (`date`)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '每日沪股通、港股通交易信息';
    """
    engine.execute(create_table_sql)


def batch_update_day_sct_trade_info_table(start_date, end_date):
    # 不限制返回数量
    table_name = "day_sct_trade_info"
    engine = mysql_connect()
    create_day_sct_trade_info_table(engine)
    date_list = get_trade_day_list(start_date=start_date, end_date=end_date)
    retrieve_index_sql = "select date from {}".format(table_name)
    index_df = pd.read_sql_query(retrieve_index_sql, engine)
    index_set = set(index_df["date"].map(str).tolist())
    for date in date_list:
        df = get_sct_trade_info(date)
        if len(df) == 0:
            continue
        index_col = df["date"]
        keep_row = index_col.apply(lambda x: True if x not in index_set else False)
        df = df[keep_row]
        if len(df) == 0:
            continue
        df.to_sql(table_name, engine, index=False, if_exists="append")


def create_day_sct_rate_table(engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS `day_sct_rate` (
        `record_id` INT NOT NULL AUTO_INCREMENT COMMENT '自增id',
        `refer_bid_rate` DOUBLE DEFAULT NULL COMMENT '买入参考汇率',
        `refer_ask_rate` DOUBLE DEFAULT NULL COMMENT '卖出参考汇率',
        `settle_bid_rate` DOUBLE DEFAULT NULL COMMENT '买入结算汇率',
        `settle_ask_rate` DOUBLE DEFAULT NULL COMMENT '卖出结算汇率',
        `date` DATE NOT NULL COMMENT '日期',
        PRIMARY KEY (`record_id`),
        UNIQUE `day_sct_rate_date_index` (`date`)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '每日港股通（沪）每日汇率信息';
    """
    engine.execute(create_table_sql)


def batch_update_day_sct_rate_table(start_date, end_date):
    # 不限制返回数量
    table_name = "day_sct_rate"
    engine = mysql_connect()
    create_day_sct_rate_table(engine)
    date_list = get_trade_day_list(start_date=start_date, end_date=end_date)
    retrieve_index_sql = "select date from {}".format(table_name)
    index_df = pd.read_sql_query(retrieve_index_sql, engine)
    index_set = set(index_df["date"].map(str).tolist())
    for date in date_list:
        df = get_sct_rate(date)
        if len(df) == 0:
            continue
        index_col = df["date"]
        keep_row = index_col.apply(lambda x: True if x not in index_set else False)
        df = df[keep_row]
        if len(df) == 0:
            continue
        df.to_sql(table_name, engine, index=False, if_exists="append")


def create_day_valuation_table(engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS `day_valuation` (
        `record_id` INT NOT NULL AUTO_INCREMENT COMMENT '自增id',
        `security` VARCHAR (15) NOT NULL COMMENT '股票代码',
        `pe_ratio` DOUBLE DEFAULT NULL COMMENT '市盈率TTM',
        `turnover_ratio` DOUBLE DEFAULT NULL COMMENT '换手率',
        `pb_ratio` DOUBLE DEFAULT NULL COMMENT '市净率',
        `ps_ratio` DOUBLE DEFAULT NULL COMMENT '市销率TTM',
        `pcf_ratio` DOUBLE DEFAULT NULL COMMENT '市现率TTM',
        `capitalization` DOUBLE DEFAULT NULL COMMENT '总股本',
        `market_cap` DOUBLE DEFAULT NULL COMMENT '总市值',
        `circulating_cap` DOUBLE DEFAULT NULL COMMENT '流通股本',
        `circulating_market_cap` DOUBLE DEFAULT NULL COMMENT '流通市值',
        `pe_ratio_lyr` DOUBLE DEFAULT NULL COMMENT '市盈率',
        `date` DATE NOT NULL COMMENT '日期',
        PRIMARY KEY (`record_id`),
        UNIQUE `day_valuation_security_date_index` (`security`, `date`),
        INDEX `day_valuation_security_index` (`security`),
        INDEX `day_valuation_date_index` (`date`)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '每日股票市值数据';
    """
    engine.execute(create_table_sql)


def batch_update_day_valuation_table(security_list, start_date, end_date):
    max_return_rows = 2000
    table_name = "day_valuation"
    engine = mysql_connect()
    create_day_valuation_table(engine)
    date_list = get_trade_day_list(start_date=start_date, end_date=end_date)
    retrieve_index_sql = "select security, date from {}".format(table_name)
    index_df = pd.read_sql_query(retrieve_index_sql, engine)
    index_set = set((index_df["security"] + " " + index_df["date"].map(str)).tolist())
    for date in date_list:
        batch_list = batchify(security_list, max_return_rows)
        for batch in batch_list:
            df = get_valuation(batch, date)
            if len(df) == 0:
                continue
            concat_index_col = df["security"] + " " + df["date"]
            keep_row = concat_index_col.apply(lambda x: True if x not in index_set else False)
            df = df[keep_row]
            if len(df) == 0:
                continue
            df.to_sql(table_name, engine, index=False, if_exists="append")


def create_day_finance_indicator_table(engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS `day_finance_indicator` (
        `record_id` INT NOT NULL AUTO_INCREMENT COMMENT '自增id',
        `security` VARCHAR (15) NOT NULL COMMENT '股票代码',
        `eps` DOUBLE DEFAULT NULL COMMENT '每股收益EPS',
        `adjusted_profit` DOUBLE DEFAULT NULL COMMENT '扣除非经常损益后的净利润',
        `operating_profit` DOUBLE DEFAULT NULL COMMENT '经营活动净收益',
        `value_change_profit` DOUBLE DEFAULT NULL COMMENT '价值变动净收益',
        `roe` DOUBLE DEFAULT NULL COMMENT '净资产收益率ROE',
        `inc_return` DOUBLE DEFAULT NULL COMMENT '净资产收益率(扣除非经常损益)',
        `roa` DOUBLE DEFAULT NULL COMMENT '总资产净利率ROA',
        `net_profit_margin` DOUBLE DEFAULT NULL COMMENT '销售净利率',
        `gross_profit_margin` DOUBLE DEFAULT NULL COMMENT '销售毛利率',
        `expense_to_total_revenue` DOUBLE DEFAULT NULL COMMENT '营业总成本/营业总收入',
        `operation_profit_to_total_revenue` DOUBLE DEFAULT NULL COMMENT '营业利润/营业总收入',
        `net_profit_to_total_revenue` DOUBLE DEFAULT NULL COMMENT '净利润/营业总收入',
        `operating_expense_to_total_revenue` DOUBLE DEFAULT NULL COMMENT '营业费用/营业总收入',
        `ga_expense_to_total_revenue` DOUBLE DEFAULT NULL COMMENT '管理费用/营业总收入',
        `financing_expense_to_total_revenue` DOUBLE DEFAULT NULL COMMENT '财务费用/营业总收入',
        `operating_profit_to_profit` DOUBLE DEFAULT NULL COMMENT '经营活动净收益/利润总额',
        `invesment_profit_to_profit` DOUBLE DEFAULT NULL COMMENT '价值变动净收益/利润总额',
        `adjusted_profit_to_profit` DOUBLE DEFAULT NULL COMMENT '扣除非经常损益后的净利润/归属于母公司所有者的净利润',
        `goods_sale_and_service_to_revenue` DOUBLE DEFAULT NULL COMMENT '销售商品提供劳务收到的现金/营业收入',
        `ocf_to_revenue` DOUBLE DEFAULT NULL COMMENT '经营活动产生的现金流量净额/营业收入',
        `ocf_to_operating_profit` DOUBLE DEFAULT NULL COMMENT '经营活动产生的现金流量净额/经营活动净收益',
        `inc_total_revenue_year_on_year` DOUBLE DEFAULT NULL COMMENT '营业总收入同比增长率',
        `inc_total_revenue_annual` DOUBLE DEFAULT NULL COMMENT '营业总收入环比增长率',
        `inc_revenue_year_on_year` DOUBLE DEFAULT NULL COMMENT '营业收入同比增长率',
        `inc_revenue_annual` DOUBLE DEFAULT NULL COMMENT '营业收入环比增长率',
        `inc_operation_profit_year_on_year` DOUBLE DEFAULT NULL COMMENT '营业利润同比增长率',
        `inc_operation_profit_annual` DOUBLE DEFAULT NULL COMMENT '营业利润环比增长率',
        `inc_net_profit_year_on_year` DOUBLE DEFAULT NULL COMMENT '净利润同比增长率',
        `inc_net_profit_annual` DOUBLE DEFAULT NULL COMMENT '净利润环比增长率',
        `inc_net_profit_to_shareholders_year_on_year` DOUBLE DEFAULT NULL COMMENT '归属母公司股东的净利润同比增长率',
        `inc_net_profit_to_shareholders_annual` DOUBLE DEFAULT NULL COMMENT '归属母公司股东的净利润环比增长率',
        `date` DATE NOT NULL COMMENT '日期',
        PRIMARY KEY (`record_id`),
        UNIQUE `day_finance_indicator_security_date_index` (`security`, `date`),
        INDEX `day_finance_indicator_security_index` (`security`),
        INDEX `day_finance_indicator_date_index` (`date`)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '每日股票财务指标数据';
    """
    engine.execute(create_table_sql)


def batch_update_day_finance_indicator_table(security_list, start_date, end_date):
    max_return_rows = 2000
    table_name = "day_finance_indicator"
    engine = mysql_connect()
    create_day_finance_indicator_table(engine)
    date_list = get_trade_day_list(start_date=start_date, end_date=end_date)
    retrieve_index_sql = "select security, date from {}".format(table_name)
    index_df = pd.read_sql_query(retrieve_index_sql, engine)
    index_set = set((index_df["security"] + " " + index_df["date"].map(str)).tolist())
    for date in date_list:
        batch_list = batchify(security_list, max_return_rows)
        for batch in batch_list:
            df = get_finance_indicator(batch, date)
            if len(df) == 0:
                continue
            concat_index_col = df["security"] + " " + df["date"]
            keep_row = concat_index_col.apply(lambda x: True if x not in index_set else False)
            df = df[keep_row]
            if len(df) == 0:
                continue
            df.to_sql(table_name, engine, index=False, if_exists="append")


def create_day_income_table(engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS `day_income` (
        `record_id` INT NOT NULL AUTO_INCREMENT COMMENT '自增id',
        `security` VARCHAR (15) NOT NULL COMMENT '股票代码',
        `net_profit` DOUBLE DEFAULT NULL COMMENT '净利润',
        `operating_profit` DOUBLE DEFAULT NULL COMMENT '营业利润',
        `total_profit` DOUBLE DEFAULT NULL COMMENT '利润总额',
        `np_parent_company_owners` DOUBLE DEFAULT NULL COMMENT '归属于母公司股东的净利润',
        `operating_revenue` DOUBLE DEFAULT NULL COMMENT '营业收入',
        `total_operating_revenue` DOUBLE DEFAULT NULL COMMENT '营业总收入',
        `total_operating_cost` DOUBLE DEFAULT NULL COMMENT '营业总成本',
        `basic_eps` DOUBLE DEFAULT NULL COMMENT '基本每股收益',
        `diluted_eps` DOUBLE DEFAULT NULL COMMENT '稀释每股收益',
        `date` DATE NOT NULL COMMENT '日期',
        PRIMARY KEY (`record_id`),
        UNIQUE `day_income_security_date_index` (`security`, `date`),
        INDEX `day_income_security_index` (`security`),
        INDEX `day_income_date_index` (`date`)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '每日股票利润数据';
    """
    engine.execute(create_table_sql)


def batch_update_day_income_table(security_list, start_date, end_date):
    max_return_rows = 2000
    table_name = "day_income"
    engine = mysql_connect()
    create_day_income_table(engine)
    date_list = get_trade_day_list(start_date=start_date, end_date=end_date)
    retrieve_index_sql = "select security, date from {}".format(table_name)
    index_df = pd.read_sql_query(retrieve_index_sql, engine)
    index_set = set((index_df["security"] + " " + index_df["date"].map(str)).tolist())
    for date in date_list:
        batch_list = batchify(security_list, max_return_rows)
        for batch in batch_list:
            df = get_income(batch, date)
            if len(df) == 0:
                continue
            concat_index_col = df["security"] + " " + df["date"]
            keep_row = concat_index_col.apply(lambda x: True if x not in index_set else False)
            df = df[keep_row]
            if len(df) == 0:
                continue
            df.to_sql(table_name, engine, index=False, if_exists="append")



if __name__ == "__main__":
    auth(USER_NAME, PASSWORD)
    start_date = "2018-03-02"
    end_date = "2018-03-02"
    # security_list = SECURITY_LIST
    # security_list = SECURITY_LIST[:300]
    # security_list = ["603997.XSHG", "600469.XSHG", "600468.XSHG", "600467.XSHG", "600466.XSHG", "600470.XSHG"]
    security_list = ["300015.XSHE", "000002.XSHE", "000022.XSHE", "000012.XSHE", "300016.XSHE"]
    batch_update_day_income_table(security_list, start_date, end_date)
    print()