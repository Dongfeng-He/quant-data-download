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


# TODO：行业概念行情参考：https://www.joinquant.com/view/community/detail/16656
# TODO：其他有价值的数据：人民币外汇牌价(日级)、银行间拆借利率表（日级）、景气指数、获取基金持股信息、、

if __name__ == "__main__":
    auth(USER_NAME, PASSWORD)
    # a = get_security_industry(["300015.XSHE"], "2011-03-02")
    a = get_security_industry(["300015.XSHE", "000001.XSHE"], "2000-03-02")
    print()