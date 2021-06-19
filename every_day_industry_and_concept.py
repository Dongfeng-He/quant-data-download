# 每日行业概念信息
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


# 获取多股票每日所属行业信息
def get_security_industry(security_list, date):
    # 申万一级行业、申万二级行业、申万三级行业、聚宽一级行业、聚宽二级行业、证监会行业
    name_list = ["sw_l1", "sw_l2", "sw_l3", "jq_l1", "jq_l2", "zjw"]
    data_dict = jq.get_industry(security_list, date=date)
    data_df_list = []
    for security in data_dict:
        industry_dict = {"security": security}
        if len(data_dict[security]) == 0:
            continue
        for name in name_list:
            if name in data_dict[security]:
                industry_dict[name] = data_dict[security][name]["industry_name"]
            else:
                industry_dict[name] = "None"
        temp_df = pd.DataFrame(industry_dict, index=[0])
        data_df_list.append(temp_df)
    if not data_df_list:
        return pd.DataFrame({name: [] for name in ["security"] + name_list + ["date"]})
    concat_df = pd.concat(data_df_list, axis=0)
    result_df = concat_df.sort_values(by=["security"], ascending=True).reset_index(drop=True)
    result_df["date"] = date
    return result_df


# 获取多股票每日所属概念信息
def get_security_concept(security_list, date):
    # 申万一级行业、申万二级行业、申万三级行业、聚宽一级行业、聚宽二级行业、证监会行业
    data_dict = jq.get_concept(security_list, date=date)
    data_df_list = []
    for security in data_dict:
        if "jq_concept" not in data_dict[security] or len(data_dict[security]["jq_concept"]) == 0:
            continue
        concept_list_str = "|".join(list(map(lambda x: x["concept_name"], data_dict[security]["jq_concept"])))
        concept_dict = {"security": security, "concept_list": concept_list_str}
        temp_df = pd.DataFrame(concept_dict, index=[0])
        data_df_list.append(temp_df)
    if not data_df_list:
        return pd.DataFrame({col: [] for col in ["security", "concept_list", "date"]})
    concat_df = pd.concat(data_df_list, axis=0)
    result_df = concat_df.sort_values(by=["security"], ascending=True).reset_index(drop=True)
    result_df["date"] = date
    return result_df


# TODO：行业概念行情参考：https://www.joinquant.com/view/community/detail/16656
# TODO：其他有价值的数据：人民币外汇牌价(日级)、银行间拆借利率表（日级）、景气指数、获取基金持股信息、、

# a = get_security_industry(["300015.XSHE"], "2011-03-02")
a = get_security_industry(["300015.XSHE", "000001.XSHE"], "2000-03-02")
print()