# 每股不定期更新的信息，要根据日期降序排列，确定每条信息的 start_date、end_date，不 limit，要考虑到返回条数限制
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


def xxx(code, period_start_date, period_end_date):
    # 买入金额、买入股数、卖出金额、卖出股数
    fields = ["buy_amount", "buy_volume", "sell_amount", "sell_volume"]
    # 市场通代码，分别为沪股通、港股通（沪）
    market_code_list = ["310001", "310003"]
    field_data_dict = {field: [] for field in fields}
    empty_df = pd.DataFrame(field_data_dict)
    table = jq.finance.STK_ML_QUOTA
    for market_code in market_code_list:
        cond1 = jq.finance.STK_ML_QUOTA.link_id == market_code
        cond2 = jq.finance.STK_ML_QUOTA.day == date
        query = jq.query(table).filter(cond1, cond2)
        data_df = jq.finance.run_query(query)
        for field in fields:
            field_data_list = data_df[field].values.tolist()
            if len(field_data_list) != 1:
                return empty_df
            field_data_dict[field].append(field_data_list[0])
    result_df = pd.DataFrame({field: "|".join(list(map(lambda x: str(x), field_data_dict[field]))) for field in fields}, index=[0])
    return result_df


# 获取单一股票不定期公布的员工信息，包括在职人数、退休人数、研究生比例、大学生比例、中专生比例
# 原始表里的 end_date 应该是统计时的结束日期，处理之后的 end_date 是这份数据有效的最后日期
def get_period_employee_info(code, period_start_date, period_end_date):
    # 在职人数、退休人数、研究生比例、大学生比例、中专生比例
    fields = ["employee", "retirement", "graduate_rate", "college_rate", "middle_rate"]
    table = jq.finance.STK_EMPLOYEE_INFO
    cond1 = jq.finance.STK_EMPLOYEE_INFO.code == code
    cond2 = jq.finance.STK_EMPLOYEE_INFO.pub_date <= period_end_date
    cond3 = jq.finance.STK_EMPLOYEE_INFO.pub_date >= period_start_date
    order = finance.STK_EMPLOYEE_INFO.pub_date.desc()
    query = jq.query(table).filter(cond1, cond2, cond3).order_by(order)
    data_df = jq.finance.run_query(query)
    if data_df.empty:
        return pd.DataFrame({field: [] for field in ["security"] + fields + ["start_date", "end_date"]})
    # 计算出每一条数据的有效期间
    date_list = data_df["pub_date"].values.tolist()     # 本身是 datetime.date 类型
    start_date_list = [(date + timedelta(days=1)).strftime("%Y-%m-%d") for date in date_list]
    end_date_list = ["2200-01-01"] + [date.strftime("%Y-%m-%d") for date in date_list[:-1]]
    result_df = data_df[fields]
    result_df["start_date"] = start_date_list
    result_df["end_date"] = end_date_list
    # 调整字段顺序
    result_df["security"] = code
    result_df = result_df[["security"] + fields + ["start_date", "end_date"]]
    return result_df


# 获取单一股票不定期公布的十大股东信息，包括股东名称、股东ID、股东类型、持股数量、持股比例
# 原始表里的 end_date 应该是统计时的结束日期，处理之后的 end_date 是这份数据有效的最后日期
def get_shareholder_top10(code, period_start_date, period_end_date):
    # 股东名称、股东ID、股东类型、持股数量、持股比例
    # TODO: 其实还有增减持的有效信息
    fields = ["shareholder_name", "shareholder_id", "shareholder_class", "share_number", "share_ratio"]
    table = jq.finance.STK_SHAREHOLDER_TOP10
    cond1 = jq.finance.STK_SHAREHOLDER_TOP10.code == code
    cond2 = jq.finance.STK_SHAREHOLDER_TOP10.change_reason == "定期报告"
    cond3 = jq.finance.STK_SHAREHOLDER_TOP10.pub_date <= period_end_date
    cond4 = jq.finance.STK_SHAREHOLDER_TOP10.pub_date >= period_start_date
    order1 = finance.STK_SHAREHOLDER_TOP10.pub_date.desc()
    order2 = finance.STK_SHAREHOLDER_TOP10.shareholder_rank.asc()
    query = jq.query(table).filter(cond1, cond2, cond3, cond4).order_by(order1, order2)
    data_df = jq.finance.run_query(query)
    if data_df.empty:
        return pd.DataFrame({field: [] for field in ["security"] + fields + ["start_date", "end_date"]})
    df_list = []
    for i, field in enumerate(fields):
        df = data_df.groupby(["pub_date", "end_date"])[field].apply(lambda x: "|".join(list(map(lambda e: str(e), x)))).reset_index()
        date_df = df[["pub_date", "end_date"]]
        field_df = df[[field]]
        if i == 0:
            df_list.append(date_df)
        df_list.append(field_df)
    concat_df = pd.concat(df_list, axis=1)
    concat_df = concat_df.sort_values(by=["pub_date", "end_date"], ascending=[False, False]).reset_index(drop=True)
    # pub_date 去重
    concat_df = concat_df.drop_duplicates(subset=["pub_date"], keep="first", inplace=False).reset_index(drop=True, inplace=False)
    # 计算出每一条数据的有效期间
    date_list = concat_df["pub_date"].tolist()     # group by 后是 Timestamp 类型
    start_date_list = [(date + timedelta(days=1)).strftime("%Y-%m-%d") for date in date_list]
    end_date_list = ["2200-01-01"] + [date.strftime("%Y-%m-%d") for date in date_list[:-1]]
    result_df = concat_df[fields]
    result_df["start_date"] = start_date_list
    result_df["end_date"] = end_date_list
    # 调整字段顺序
    result_df["security"] = code
    result_df = result_df[["security"] + fields + ["start_date", "end_date"]]
    return result_df


# 获取单一股票不定期公布的股东数量，包括股东数、A股股东数、B股股东数、H股股东数
# 原始表里的 end_date 应该是统计时的结束日期，处理之后的 end_date 是这份数据有效的最后日期
def get_shareholder_num(code, period_start_date, period_end_date):
    # 股东数、A股股东数、B股股东数、H股股东数
    fields = ["share_holders", "a_share_holders", "b_share_holders", "h_share_holders"]
    table = jq.finance.STK_HOLDER_NUM
    cond1 = jq.finance.STK_HOLDER_NUM.code == code
    cond2 = jq.finance.STK_HOLDER_NUM.pub_date <= period_end_date
    cond3 = jq.finance.STK_HOLDER_NUM.pub_date >= period_start_date
    order = finance.STK_HOLDER_NUM.pub_date.desc()
    query = jq.query(table).filter(cond1, cond2, cond3).order_by(order)
    data_df = jq.finance.run_query(query)
    if data_df.empty:
        return pd.DataFrame({field: [] for field in ["security"] + fields + ["start_date", "end_date"]})
    # 计算出每一条数据的有效期间
    date_list = data_df["pub_date"].values.tolist()     # 本身是 datetime.date 类型
    start_date_list = [(date + timedelta(days=1)).strftime("%Y-%m-%d") for date in date_list]
    end_date_list = ["2200-01-01"] + [date.strftime("%Y-%m-%d") for date in date_list[:-1]]
    result_df = data_df[fields]
    result_df["start_date"] = start_date_list
    result_df["end_date"] = end_date_list
    # 调整字段顺序
    result_df["security"] = code
    result_df = result_df[["security"] + fields + ["start_date", "end_date"]]
    return result_df


# 获取单一股票不定期公布的股东质押信息，包括出质人、质押数量、质押比例、解除质押数量、是否质押式回购
# 原始表里的 end_date 应该是统计时的结束日期，处理之后的 end_date 是这份数据有效的最后日期
def get_shareholder_pledge(code, period_start_date, period_end_date):
    # pledge_number、unpledged_number 互斥，其中一个会为 nan
    # 出质人、质押数量、质押比例、解除质押数量、是否质押式回购
    fields = ["pledgor", "pledge_number", "pledge_total_ratio", "unpledged_number", "is_buy_back"]
    table = jq.finance.STK_SHARES_PLEDGE
    cond1 = jq.finance.STK_SHARES_PLEDGE.code == code
    cond2 = jq.finance.STK_SHARES_PLEDGE.pub_date <= period_end_date
    cond3 = jq.finance.STK_SHARES_PLEDGE.pub_date >= period_start_date
    order = finance.STK_SHARES_PLEDGE.pub_date.desc()
    query = jq.query(table).filter(cond1, cond2, cond3).order_by(order)
    data_df = jq.finance.run_query(query)
    if data_df.empty:
        return pd.DataFrame({field: [] for field in ["security"] + fields + ["start_date", "end_date"]})
    # 计算出每一条数据的有效期间
    date_list = data_df["pub_date"].values.tolist()     # 本身是 datetime.date 类型
    start_date_list = [(date + timedelta(days=1)).strftime("%Y-%m-%d") for date in date_list]
    end_date_list = ["2200-01-01"] + [date.strftime("%Y-%m-%d") for date in date_list[:-1]]
    result_df = data_df[fields]
    result_df["start_date"] = start_date_list
    result_df["end_date"] = end_date_list
    # 调整字段顺序
    result_df["security"] = code
    result_df = result_df[["security"] + fields + ["start_date", "end_date"]]
    return result_df


# 获取单一股票不定期公布的股东增减持信息，包括增持还是减持、股东名、变动股数、变动比例、增减持价格
# 原始表里的 end_date 应该是统计时的结束日期，处理之后的 end_date 是这份数据有效的最后日期
def get_shareholder_share_change(code, period_start_date, period_end_date):
    # 增持还是减持、股东名、变动股数、变动比例、增减持价格
    fields = ["type", "shareholder_name", "change_number", "change_ratio", "price_ceiling"]
    table = jq.finance.STK_SHAREHOLDERS_SHARE_CHANGE
    cond1 = jq.finance.STK_SHAREHOLDERS_SHARE_CHANGE.code == code
    cond2 = jq.finance.STK_SHAREHOLDERS_SHARE_CHANGE.pub_date <= period_end_date
    cond3 = jq.finance.STK_SHAREHOLDERS_SHARE_CHANGE.pub_date >= period_start_date
    order = finance.STK_SHAREHOLDERS_SHARE_CHANGE.pub_date.desc()
    query = jq.query(table).filter(cond1, cond2, cond3).order_by(order)
    data_df = jq.finance.run_query(query)
    if data_df.empty:
        return pd.DataFrame({field: [] for field in ["security"] + fields + ["start_date", "end_date"]})
    # 计算出每一条数据的有效期间
    date_list = data_df["pub_date"].values.tolist()     # 本身是 datetime.date 类型
    start_date_list = [(date + timedelta(days=1)).strftime("%Y-%m-%d") for date in date_list]
    end_date_list = ["2200-01-01"] + [date.strftime("%Y-%m-%d") for date in date_list[:-1]]
    result_df = data_df[fields]
    result_df["start_date"] = start_date_list
    result_df["end_date"] = end_date_list
    # 调整字段顺序
    result_df["security"] = code
    result_df = result_df[["security"] + fields + ["start_date", "end_date"]]
    return result_df


# 获取单一股票不定期公布的股本变动信息，包括变动原因ID、变动原因
# 原始表里的 end_date 应该是统计时的结束日期，处理之后的 end_date 是这份数据有效的最后日期
def get_capital_change(code, period_start_date, period_end_date):
    # 变动原因ID、变动原因
    fields = ["change_reason_id", "change_reason"]
    table = jq.finance.STK_CAPITAL_CHANGE
    cond1 = jq.finance.STK_CAPITAL_CHANGE.code == code
    cond2 = jq.finance.STK_CAPITAL_CHANGE.pub_date <= period_end_date
    cond3 = jq.finance.STK_CAPITAL_CHANGE.pub_date >= period_start_date
    order = finance.STK_CAPITAL_CHANGE.pub_date.desc()
    query = jq.query(table).filter(cond1, cond2, cond3).order_by(order)
    data_df = jq.finance.run_query(query)
    if data_df.empty:
        return pd.DataFrame({field: [] for field in ["security"] + fields + ["start_date", "end_date"]})
    # 计算出每一条数据的有效期间
    date_list = data_df["pub_date"].values.tolist()     # 本身是 datetime.date 类型
    start_date_list = [(date + timedelta(days=1)).strftime("%Y-%m-%d") for date in date_list]
    end_date_list = ["2200-01-01"] + [date.strftime("%Y-%m-%d") for date in date_list[:-1]]
    result_df = data_df[fields]
    result_df["start_date"] = start_date_list
    result_df["end_date"] = end_date_list
    # 调整字段顺序
    result_df["security"] = code
    result_df = result_df[["security"] + fields + ["start_date", "end_date"]]
    return result_df


# a = get_capital_change("000002.XSHE", "2018-03-02", "2020-01-01")
a = get_capital_change("300015.XSHE", "2018-03-02", "2020-01-01")

a = get_shareholder_top10("300015.XSHE", "2011-03-02", "2020-01-01")
print()

