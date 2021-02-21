from jqdatasdk import *
from conf.common_conf import *
from utils.utilization import *
import pandas as pd
from datetime import datetime, timedelta


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






