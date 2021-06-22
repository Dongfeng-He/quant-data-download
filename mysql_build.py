# 每日每股行情信息、融资融券信息、所属行业信息、资金流向信息等
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


def build_day_price(security_list, start_date, end_date):
    table_name = "day_price"
    engine = mysql_connect()
    date_list = get_trade_day_list(start_date=start_date, end_date=end_date)
    for date in date_list:
        for security in security_list:
            df = get_day_price(security, date)
            df.to_sql(table_name, engine, index=False, if_exists="append")
            print()




            # df_list.append(get_valuation([security], date).drop(["security", "date"], axis=1))
            # df_list.append(get_finance_indicator([security], date).drop(["security", "date"], axis=1))

            # TODO: 什么时候加上 security、date？






if __name__ == "__main__":
    auth(USER_NAME, PASSWORD)
    start_date = "2018-03-02"
    end_date = "2018-03-15"
    security_list = ["300015.XSHE", "000002.XSHE", "000022.XSHE", "000012.XSHE"]

    res = build_day_price(security_list, start_date, end_date)
    print()
