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


def mysql_connect():
    db = mysql.connector.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, passwd=MYSQL_PASSWORD,
                                 database=MYSQL_DATABASE, charset=MYSQL_CHARSET)
    return db, db.cursor()


def build_day_price(security_list, start_date, end_date):
    date_list = get_trade_day_list(start_date=start_date, end_date=end_date)
    for date in date_list:
        for security in security_list:
            df = get_day_price(security, date)
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
