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


# 获取单一股票单日融资融券信息
# fin_value(融资余额), fin_buy_value(融资买入额), fin_refund_value(融资偿还额), sec_value(融券余量), sec_sell_value(融券卖出量), sec_refund_value(融券偿还量), fin_sec_value(融资融券余额)
def get_day_mtss(security, date):
    fields = ["fin_value", "fin_buy_value", "fin_refund_value", "sec_value", "sec_sell_value",
              "sec_refund_value", "fin_sec_value"]
    # start_date 能取到，end_date 取不到
    data_df = jq.get_mtss(security, start_date=date, end_date=date, fields=fields)
    if data_df.empty:
        return pd.DataFrame({field: [] for field in ["security"] + fields + ["date"]})
    data_df["security"] = security
    data_df["date"] = date
    result_df = data_df[["security"] + fields + ["date"]]
    return result_df


def get_securities_day_mtss(security_list, date):
    fields = ["fin_value", "fin_buy_value", "fin_refund_value", "sec_value", "sec_sell_value",
              "sec_refund_value", "fin_sec_value"]
    # start_date 能取到，end_date 取不到
    data_df = jq.get_mtss(security_list, start_date=date, end_date=date)
    if data_df.empty:
        return pd.DataFrame({field: [] for field in ["security"] + fields + ["date"]})
    data_df = data_df.rename(columns={"sec_code": "security"})
    data_df["date"] = date
    result_df = data_df[["security"] + fields + ["date"]]
    return result_df


# a1 = get_day_mtss("300015.XSHE", "2018-03-02")
# a2 = get_securities_day_mtss(["300015.XSHE"], "2018-03-02")
# b = a1.equals(a2)
#
# code_list = ["000002.XSHE", "000012.XSHE", "000022.XSHE", "300015.XSHE", "300016.XSHE"]
# df_list = []
# for code in code_list:
#     df = get_day_mtss(code, "2018-03-02")
#     df_list.append(df)
# a1 = pd.concat(df_list, axis=0).reset_index(drop=True)
# a2 = get_securities_day_mtss(code_list, "2018-03-02").sort_values(by=["security"], ascending=[True]).reset_index(drop=True)
# b = a1.equals(a2)
# print()


# 获取单一股票单日盘前集合竞价数据，以 "1000|1100|10000|2000|1500" 形式表示
# av(五档卖量列表), ap(五档卖价列表), bv(五档买量列表), bp(五档买价列表)
def get_day_call_auction(security, date):
    av_fields = ["a1_v", "a2_v", "a3_v", "a4_v", "a5_v"]    # 五档卖量
    ap_fields = ["a1_p", "a2_p", "a3_p", "a4_p", "a5_p"]    # 五档卖价
    bv_fields = ["b1_v", "b2_v", "b3_v", "b4_v", "b5_v"]    # 五档买量
    bp_fields = ["b1_p", "b2_p", "b3_p", "b4_p", "b5_p"]    # 五档买价
    field_names = ["av", "ap", "bv", "bp"]
    field_lists = [av_fields, ap_fields, bv_fields, bp_fields]
    fields = av_fields + ap_fields + bv_fields + bp_fields
    data_df = jq.get_call_auction(security, start_date=date, end_date=date, fields=fields)
    if data_df.empty:
        return pd.DataFrame({field: [] for field in ["security"] + field_names + ["date"]})
    result_data_df = pd.DataFrame()
    for name, temp_fields in zip(field_names, field_lists):
        value_list = []
        for field in temp_fields:
            value_list.append(data_df[field].values.tolist()[0])
        value = "|".join(list(map(lambda x: str(x), value_list)))
        result_data_df[name] = [value]
    result_data_df["security"] = security
    result_data_df["date"] = date
    result_df = result_data_df[["security"] + field_names + ["date"]]
    return result_df


def get_securities_day_call_auction(security_list, date):
    av_fields = ["a1_v", "a2_v", "a3_v", "a4_v", "a5_v"]    # 五档卖量
    ap_fields = ["a1_p", "a2_p", "a3_p", "a4_p", "a5_p"]    # 五档卖价
    bv_fields = ["b1_v", "b2_v", "b3_v", "b4_v", "b5_v"]    # 五档买量
    bp_fields = ["b1_p", "b2_p", "b3_p", "b4_p", "b5_p"]    # 武当买价
    field_names = ["av", "ap", "bv", "bp"]
    field_lists = [av_fields, ap_fields, bv_fields, bp_fields]
    fields = av_fields + ap_fields + bv_fields + bp_fields
    data_df = jq.get_call_auction(security_list, start_date=date, end_date=date, fields=fields)
    if data_df.empty:
        return pd.DataFrame({field: [] for field in ["security"] + field_names + ["date"]})
    for name, tf in zip(field_names, field_lists):
        data_df[name] = data_df[tf[0]].map(str) + "|" + data_df[tf[1]].map(str) + "|" \
                        + data_df[tf[2]].map(str) + "|" + data_df[tf[3]].map(str) + "|" \
                        + data_df[tf[4]].map(str)
    data_df = data_df.rename(columns={"code": "security"})
    data_df["date"] = date
    result_df = data_df[["security"] + field_names + ["date"]]
    return result_df


# a1 = get_day_call_auction("300015.XSHE", "2018-03-02")
# a2 = get_securities_day_call_auction(["300015.XSHE"], "2018-03-02")
# b = a1.equals(a2)
#
# code_list = ["000002.XSHE", "000012.XSHE", "000022.XSHE", "300015.XSHE", "300016.XSHE"]
# df_list = []
# for code in code_list:
#     df = get_day_call_auction(code, "2018-03-02")
#     df_list.append(df)
# a1 = pd.concat(df_list, axis=0).reset_index(drop=True)
# a2 = get_securities_day_call_auction(code_list, "2018-03-02").sort_values(by=["security"], ascending=[True]).reset_index(drop=True)
# b = a1.equals(a2)
# print()


# 获取单一股票单日资金流向
# change_pct(涨跌幅), net_amount_main(主力净额), net_pct_main(主力净占比), net_amount_xl(超大单净额), net_pct_xl(超大单净占比), net_amount_l(大单净额), net_pct_l(打单净占比), net_amount_m(中单净额), net_pct_m(中单净占比), net_amount_s(小单净额), net_pct_s(小单净占比)
def get_day_money_flow(security, date):
    # TODO: 疑问: 净占比有负数，且占比加起来不为100%
    fields = ["change_pct", "net_amount_main", "net_pct_main", "net_amount_xl", "net_pct_xl", "net_amount_l",
              "net_pct_l", "net_amount_m", "net_pct_m", "net_amount_s", "net_pct_s"]
    data_df = jq.get_money_flow(security, start_date=date, end_date=date, fields=fields)
    if data_df.empty:
        return pd.DataFrame({field: [] for field in ["security"] + fields + ["date"]})
    data_df["security"] = security
    data_df["date"] = date
    result_df = data_df[["security"] + fields + ["date"]]
    return result_df


def get_securities_day_money_flow(security_list, date):
    # TODO: 疑问: 净占比有负数，且占比加起来不为100%
    fields = ["change_pct", "net_amount_main", "net_pct_main", "net_amount_xl", "net_pct_xl", "net_amount_l",
              "net_pct_l", "net_amount_m", "net_pct_m", "net_amount_s", "net_pct_s"]
    data_df = jq.get_money_flow(security_list, start_date=date, end_date=date)
    if data_df.empty:
        return pd.DataFrame({field: [] for field in ["security"] + fields + ["date"]})
    data_df = data_df.rename(columns={"sec_code": "security"})
    data_df["date"] = date
    result_df = data_df[["security"] + fields + ["date"]]
    return result_df


# a1 = get_day_money_flow("300015.XSHE", "2018-03-02")
# a2 = get_securities_day_money_flow(["300015.XSHE"], "2018-03-02")
# b = a1.equals(a2)
#
# code_list = ["000002.XSHE", "000012.XSHE", "000022.XSHE", "300015.XSHE", "300016.XSHE"]
# df_list = []
# for code in code_list:
#     df = get_day_money_flow(code, "2018-03-02")
#     df_list.append(df)
# a1 = pd.concat(df_list, axis=0).reset_index(drop=True)
# a2 = get_securities_day_money_flow(code_list, "2018-03-02").sort_values(by=["security"], ascending=[True]).reset_index(drop=True)
# b = a1.equals(a2)
# print()


# 获取单一股票单日分钟行情，返回结果不带股票名和日期，为了和其余特征横向拼接
# open(开始价列表), close(结束价列表), low(最低价列表), high(最高价列表), avg(平均价列表), factor(复权因子列表), volume(成交股数列表), money(成交金额列表), paused(是否停牌列表)
def get_securities_day_price(security_list, date):
    date_str = date
    date = datetime.strptime(date, "%Y-%m-%d")
    start_date = date.strftime("%Y-%m-%d")
    end_date = (date + timedelta(days=1)).strftime("%Y-%m-%d")
    fields = ["open", "close", "low", "high", "avg", "factor", "volume", "money", "paused"]
    # start_date 能取到，end_date 取不到
    data_df = jq.get_price(security_list, start_date=start_date, end_date=end_date, frequency='1m', fields=fields,
                           skip_paused=False, fq='post', panel=False)
    if data_df.empty:
        return pd.DataFrame({field: [] for field in ["security"] + fields + ["date"]})
    data_df["event_time"] = data_df.index
    data_df = data_df.sort_values(by=["code", "event_time"], ascending=[True, True]).reset_index(drop=True)
    code_info_dict = {}
    for field in fields:
        grouped_df = data_df.groupby(["code"])[field].apply(lambda x: "|".join(list(map(lambda e: str(e), x)))).reset_index()
        grouped_dict = grouped_df.T.to_dict()
        for index, code_field_dict in grouped_dict.items():
            code = code_field_dict["code"]
            field_info = code_field_dict[field]
            code_info_dict.setdefault(code, {})[field] = field_info
    data_list = []
    for code, code_dict in code_info_dict.items():
        code_dict["security"] = code
        code_dict["date"] = date_str
        data_list.append(code_dict)
    day_data_df = pd.DataFrame(data_list)
    result_df = day_data_df[["security"] + fields + ["date"]]
    return result_df


def get_day_price(security, date):
    date_str = date
    date = datetime.strptime(date, "%Y-%m-%d")
    start_date = date.strftime("%Y-%m-%d")
    end_date = (date + timedelta(days=1)).strftime("%Y-%m-%d")
    fields = ["open", "close", "low", "high", "avg", "factor", "volume", "money", "paused"]
    # start_date 能取到，end_date 取不到
    data_df = jq.get_price([security], start_date=start_date, end_date=end_date, frequency='1m', fields=fields,
                           skip_paused=False, fq='post', panel=False)
    if data_df.empty:
        return pd.DataFrame({field: [] for field in ["security"] + fields + ["date"]})
    data_df["event_time"] = data_df.index
    data_df = data_df.sort_values(by=["event_time"], ascending=[True]).reset_index(drop=True)
    day_data_dict = {col: "|".join(list(map(lambda x: str(x), data_df[col].values.tolist()))) for col in fields}
    day_data_df = pd.DataFrame(day_data_dict, index=[0])
    day_data_df["security"] = security
    day_data_df["date"] = date_str
    result_df = day_data_df[["security"] + fields + ["date"]]
    return result_df


# code_list = ["000002.XSHE", "000012.XSHE", "000022.XSHE", "300015.XSHE", "300016.XSHE"]
# df_list = []
# for code in code_list:
#     df = get_day_price(code, "2018-03-02")
#     df_list.append(df)
# a1 = pd.concat(df_list, axis=0).reset_index(drop=True)
# a2 = get_securities_day_price(code_list, "2018-03-02")
# b = a1.equals(a2)
#
# a1 = get_day_price("300015.XSHE", "2018-03-02")
# a2 = get_securities_day_price(["300015.XSHE"], "2018-03-02")
# b = a1.equals(a2)


# 单股票单日是否为 st
# is_st(是否为 st)
def get_day_st(security, date):
    data_df = jq.get_extras("is_st", security, start_date=date, end_date=date, df=True)
    if data_df.empty:
        return pd.DataFrame({col: [] for col in ["security", "is_st", "date"]})
    data_df.index = [0]
    data_df.columns = ["is_st"]
    data_df["security"] = security
    data_df["date"] = date
    result_df = data_df[["security", "is_st", "date"]]
    return result_df


def get_securities_day_st(security_list, date):
    data_df = jq.get_extras("is_st", security_list, start_date=date, end_date=date, df=True)
    if data_df.empty:
        return pd.DataFrame({col: [] for col in ["security", "is_st", "date"]})
    data_df = data_df.reset_index(drop=True).T
    data_df.columns = ["is_st"]
    data_df["security"] = data_df.index
    data_df["date"] = date
    data_df = data_df.reset_index(drop=True)
    result_df = data_df[["security", "is_st", "date"]]
    return result_df

# a1 = get_day_st("300015.XSHE", "2018-03-02")
# a2 = get_securities_day_st(["300015.XSHE"], "2018-03-02")
# b = a1.equals(a2)
#
# code_list = ["000002.XSHE", "000012.XSHE", "000022.XSHE", "300015.XSHE", "300016.XSHE"]
# df_list = []
# for code in code_list:
#     df = get_day_st(code, "2018-03-02")
#     df_list.append(df)
# a1 = pd.concat(df_list, axis=0).reset_index(drop=True)
# a2 = get_securities_day_st(code_list, "2018-03-02").sort_values(by=["security"], ascending=[True]).reset_index(drop=True)
# c = a1.equals(a2)
# print()


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
        return pd.DataFrame({field: [] for field in ["security"] + fields + ["date"]})
    data_df["security"] = security
    data_df["date"] = date
    result_df = data_df[["security"] + fields + ["date"]]
    return result_df


def get_securities_sct_share(security_list, date):
    # 持股数量、持股比例
    fields = ["share_number", "share_ratio"]
    table = jq.finance.STK_HK_HOLD_INFO
    cond1 = jq.finance.STK_HK_HOLD_INFO.link_id == "310001"   # 沪股通
    cond2 = jq.finance.STK_HK_HOLD_INFO.day == date
    cond3 = jq.finance.STK_HK_HOLD_INFO.code.in_(security_list)
    query = jq.query(table).filter(cond1, cond2, cond3).limit(10)
    data_df = jq.finance.run_query(query)
    if data_df.empty:
        return pd.DataFrame({field: [] for field in ["security"] + fields + ["date"]})
    data_df = data_df.rename(columns={"code": "security"})
    data_df["date"] = date
    result_df = data_df[["security"] + fields + ["date"]]
    return result_df


# a1 = get_sct_share("603997.XSHG", "2017-03-17")
# a2 = get_securities_sct_share(["603997.XSHG"], "2017-03-17")
# b = a1.equals(a2)
#
# code_list = sorted(["603997.XSHG", "600469.XSHG", "600468.XSHG", "600467.XSHG", "600466.XSHG"])
# df_list = []
# for code in code_list:
#     df = get_sct_share(code, "2017-03-17")
#     df_list.append(df)
# a1 = pd.concat(df_list, axis=0).reset_index(drop=True)
# a2 = get_securities_sct_share(code_list, "2017-03-17").sort_values(by=["security"], ascending=[True]).reset_index(drop=True)
# c = a1.equals(a2)
# print()

