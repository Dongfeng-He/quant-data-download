# 每日每股行情信息、融资融券信息、所属行业信息、资金流向信息等
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


# 获取单一股票单日融资融券信息
# fin_value(融资余额), fin_buy_value(融资买入额), fin_refund_value(融资偿还额), sec_value(融券余量), sec_sell_value(融券卖出量), sec_refund_value(融券偿还量), fin_sec_value(融资融券余额)
def get_day_mtss(security, date):
    fields = ["fin_value", "fin_buy_value", "fin_refund_value", "sec_value", "sec_sell_value",
              "sec_refund_value", "fin_sec_value"]
    # start_date 能取到，end_date 取不到
    data_df = jq.get_mtss(security, start_date=date, end_date=date, fields=fields)
    if data_df.empty:
        return pd.DataFrame({col: [] for col in fields})
    data_df.index = [0]
    data_df.columns = fields
    return data_df


# 获取单一股票单日所属行业，每列以 "行业代码_行业名字" 表示
# sw_l1(申万一级行业), sw_l2(申万二级行业), sw_l3(申万三级行业), zjw(证监会行业), jq_l2(聚宽二级行业), jq_l1(聚宽一级行业)
def get_day_industry(security, date):
    fields = ["sw_l1", "sw_l2", "sw_l3", "zjw", "jq_l2", "jq_l1"]
    data_dict = jq.get_industry(security, date=date)
    # 即使在不开盘的日期也能有返回值
    if len(data_dict) == 0 or security not in data_dict:
        return pd.DataFrame({col: [] for col in fields})
    data_df = pd.DataFrame(columns=fields)
    for field in fields:
        industry = "{}_{}".format(data_dict[security][field]["industry_code"], data_dict[security][field]["industry_name"])
        data_df[field] = [industry]
    return data_df


# 获取单一股票单日所属概念板块，以 "概念代码1_概念名字1|概念代码2_概念名字2..." 表示
# jq_concept(聚宽概念板块列表)
def get_day_concept(security, date):
    data_dict = jq.get_concept(security, date=date)
    # 即使在不开盘的日期也能有返回值
    if len(data_dict) == 0 or security not in data_dict:
        return pd.DataFrame({"jq_concept": []})
    concept_list = ["{}_{}".format(d["concept_code"], d["concept_name"]) for d in data_dict[security]["jq_concept"]]
    concept = "|".join(concept_list)
    if len(concept) == 0:
        return pd.DataFrame({"jq_concept": []})
    data_df = pd.DataFrame({"jq_concept": [concept]})
    return data_df


# 获取单一股票单日盘前集合竞价数据，以 "1000|1100|10000|2000|1500" 形式表示
# av(五档卖量列表), ap(五档卖价列表), bv(五档买量列表), bp(五档买价列表)
def get_day_call_auction(security, date):
    av_fields = ["a1_v", "a2_v", "a3_v", "a4_v", "a5_v"]    # 五档卖量
    ap_fields = ["a1_p", "a2_p", "a3_p", "a4_p", "a5_p"]    # 五档卖价
    bv_fields = ["b1_v", "b2_v", "b3_v", "b4_v", "b5_v"]    # 五档买量
    bp_fields = ["b1_p", "b2_p", "b3_p", "b4_p", "b5_p"]    # 武当买价
    field_names = ["av", "ap", "bv", "bp"]
    field_lists = [av_fields, ap_fields, bv_fields, bp_fields]
    fields = av_fields + ap_fields + bv_fields + bp_fields
    data_df = jq.get_call_auction(security, start_date=date, end_date=date, fields=fields)
    if data_df.empty:
        return pd.DataFrame({col: [] for col in field_names})
    result_data_df = pd.DataFrame()
    for name, temp_fields in zip(field_names, field_lists):
        value_list = []
        for field in temp_fields:
            value_list.append(data_df[field].values.tolist()[0])
        value = "|".join(list(map(lambda x: str(x), value_list)))
        result_data_df[name] = [value]
    return result_data_df


# 获取单一股票单日资金流向
# change_pct(涨跌幅), net_amount_main(主力净额), net_pct_main(主力净占比), net_amount_xl(超大单净额), net_pct_xl(超大单净占比), net_amount_l(大单净额), net_pct_l(打单净占比), net_amount_m(中单净额), net_pct_m(中单净占比), net_amount_s(小单净额), net_pct_s(小单净占比)
def get_day_money_flow(security, date):
    # TODO: 疑问: 净占比有负数，且占比加起来不为100%
    fields = ["change_pct", "net_amount_main", "net_pct_main", "net_amount_xl", "net_pct_xl", "net_amount_l",
              "net_pct_l", "net_amount_m", "net_pct_m", "net_amount_s", "net_pct_s"]
    data_df = jq.get_money_flow(security, start_date=date, end_date=date, fields=fields)
    if data_df.empty:
        return pd.DataFrame({col: [] for col in fields})
    return data_df


# 获取单一股票单日分钟行情，返回结果不带股票名和日期，为了和其余特征横向拼接
# open(开始价列表), close(结束价列表), low(最低价列表), high(最高价列表), avg(平均价列表), factor(复权因子列表), volume(成交股数列表), money(成交金额列表), paused(是否停牌列表)
def get_day_price(security, date):
    date = datetime.strptime(date, "%Y-%m-%d")
    start_date = date.strftime("%Y-%m-%d")
    end_date = (date + timedelta(days=1)).strftime("%Y-%m-%d")
    fields = ["open", "close", "low", "high", "avg", "factor", "volume", "money", "paused"]
    # start_date 能取到，end_date 取不到
    data_df = jq.get_price(security, start_date=start_date, end_date=end_date, frequency='1m', fields=fields,
                           skip_paused=False, fq='post', panel=False)
    if data_df.empty:
        return pd.DataFrame({col: [] for col in fields})
    data_df["event_time"] = data_df.index
    data_df = data_df.sort_values(by=["event_time"], ascending=[True]).reset_index(drop=True)
    day_data_dict = {col: "|".join(list(map(lambda x: str(x), data_df[col].values.tolist()))) for col in fields}
    day_data_df = pd.DataFrame(day_data_dict, index=[0])
    return day_data_df


# 单股票单日是否为 st
# is_st(是否为 st)
def get_day_st(security, date):
    data_df = jq.get_extras("is_st", security, start_date=date, end_date=date, df=True)
    if data_df.empty:
        return pd.DataFrame({"is_st": []})
    data_df.index = [0]
    data_df.columns = ["is_st"]
    return data_df


# 获取单一股票单日沪股通（北向资金）持股数量、持股比例
# share_number(持股数量), share_ratio(持股比例)
def get_sct_share(security, date):
    # 持股数量、持股比例
    fields = ["share_number", "share_ratio"]
    table = jq.finance.STK_HK_HOLD_INFO
    cond1 = jq.finance.STK_HK_HOLD_INFO.link_id == "310001"   # 沪股通
    cond2 = jq.finance.STK_HK_HOLD_INFO.day == date
    cond3 = jq.finance.STK_HK_HOLD_INFO.code == security
    query = jq.query(table).filter(cond1, cond2, cond3).limit(10)
    data_df = jq.finance.run_query(query)
    if len(data_df) != 1:
        return pd.DataFrame({field: [] for field in fields})
    result_df = data_df[fields]
    return result_df


def get_day_info(security, date):
    df_list = []
    df_list.append(get_day_price(security, date))
    df_list.append(get_day_st(security, date))
    df_list.append(get_day_mtss(security, date))
    df_list.append(get_day_industry(security, date))
    df_list.append(get_day_concept(security, date))
    df_list.append(get_day_call_auction(security, date))
    df_list.append(get_day_money_flow(security, date))
    df_list.append(get_sct_share(security, date))
    df = pd.concat(df_list, axis=1)
    print()


security = "000001.XSHE"
# "300015.XSHE" 爱尔眼科
# "2015-03-01" 可测试返回空, "2015-03-01" 可测试正常数据
# "2019-07-15"
# date = "2015-03-01"
# date = "2015-03-02"
date = "2015-03-02"

date = "2018-03-02"
res = get_day_info(security, date)
print()
