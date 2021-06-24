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
        INDEX `day_price_security_date` (`date`)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '每日行情表';
    """
    engine.execute(create_table_sql)


def update_day_price_table(security_list, start_date, end_date):
    table_name = "day_price"
    engine = mysql_connect()
    create_day_price_table(engine)
    date_list = get_trade_day_list(start_date=start_date, end_date=end_date)
    retrieve_index_sql = "select security, date from day_price"
    index_df = pd.read_sql_query(retrieve_index_sql, engine)
    index_set = set((index_df["security"] + " " + index_df["date"].map(str)).tolist())
    now = time.time()
    num = 0
    for date in date_list:
        for security in security_list:
            df = get_day_price(security, date)
            if len(df) == 0 or (df["security"] + " " + df["date"].map(str)).tolist()[0] in index_set:
                continue
            num += 1
            df.to_sql(table_name, engine, index=False, if_exists="append")
    cost_time = time.time() - now
    print(cost_time)
    print(cost_time / num)


def batch_update_day_price_table(security_list, start_date, end_date):
    # 不限制返回数量
    table_name = "day_price"
    engine = mysql_connect()
    create_day_price_table(engine)
    date_list = get_trade_day_list(start_date=start_date, end_date=end_date)
    retrieve_index_sql = "select security, date from day_price"
    index_df = pd.read_sql_query(retrieve_index_sql, engine)
    index_set = set((index_df["security"] + " " + index_df["date"].map(str)).tolist())
    now = time.time()
    for date in date_list:
        df = get_securities_day_price(security_list, date)
        concat_index_col = df["security"] + " " + df["date"]
        keep_row = concat_index_col.apply(lambda x: True if x not in index_set else False)
        df = df[keep_row]
        if len(df) == 0:
            continue
        df.to_sql(table_name, engine, index=False, if_exists="append")
    cost_time = time.time() - now
    print(cost_time)


if __name__ == "__main__":
    auth(USER_NAME, PASSWORD)
    start_date = "2018-03-02"
    end_date = "2018-03-02"
    # security_list = SECURITY_LIST
    # security_list = SECURITY_LIST[:300]
    security_list = ["300015.XSHE", "000002.XSHE", "000022.XSHE", "000012.XSHE"]
    # update_day_price_table(security_list, start_date, end_date)
    batch_update_day_price_table(security_list, start_date, end_date)
    print()