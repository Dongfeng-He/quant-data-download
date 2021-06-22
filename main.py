from jqdatasdk import *
from conf.common_conf import *
from utils.utilization import *
import pandas as pd
from datetime import datetime, timedelta


start_date = "2020-08-18"
end_date = "2020-08-19"     # 如果是 1m，开始时间和结束时间不能相同
frequency = "1m"     # 支持’Xd’,’Xm’, 当 X > 1时, field只支持 [‘open’, ‘close’, ‘high’, ‘low’, ‘volume’, ‘money’]
fq = None       # "pre": 前复权，None: 实际价格，"post": 后复权


if __name__ == "__main__":
    auth(USER_NAME, PASSWORD)

    # security_list = list(get_all_securities(types=['stock']).index)
    # num = get_query_count()

    # basic_attributes = get_basic_attributes(security_list, start_date, end_date, price_fields, frequency, fq)
    # df = pd.DataFrame(basic_attributes)
    # df.columns = price_fields
    #
    # df.to_csv('data.csv')

    security_list = ['000425.XSHE", "000428.XSHE']
    # tick 数据（付费）
    # tick_attributes = get_ticks(security=security_list[0], end_dt=end_date, start_dt=start_date, count=None, fields=tick_fields)

    print()






