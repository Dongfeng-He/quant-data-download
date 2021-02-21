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

auth(USER_NAME, PASSWORD)



table = jq.finance.STK_EXCHANGE_TRADE_INFO
cond1 = jq.finance.STK_EXCHANGE_TRADE_INFO.exchange_code == '322001'
cond2 = jq.finance.STK_EXCHANGE_TRADE_INFO.date == '2015-01-07'
query = jq.query(table).filter(cond1, cond2).limit(10)
df = jq.finance.run_query(query)
print()




table = jq.finance.STK_HOLDER_NUM
cond1 = jq.finance.STK_HOLDER_NUM.code == '000002.XSHE'
cond2 = jq.finance.STK_HOLDER_NUM.pub_date > '2015-01-01'
query = jq.query(table).filter(cond1, cond2).limit(10)
df = jq.finance.run_query(query)


security = "000001.XSHE"
# "300015.XSHE" 爱尔眼科
# "2015-03-01" 可测试返回空, "2015-03-01" 可测试正常数据
# "2019-07-15"
# date = "2015-03-01"
# date = "2015-03-02"
date = "2015-03-02"
res = get_day_info(security, date)
print()
