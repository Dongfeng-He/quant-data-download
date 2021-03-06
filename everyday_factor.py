# 每天每股的聚宽因子库因子、alpha101因子、alpha191因子
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
import math


# 打印所有聚宽因子库因子，以生成 FACTOR_NAME_LIST 列表
def display_all_jq_factors():
    all_factors = jq.get_all_factors()
    for index, row in all_factors.iterrows():
        factor_name = row["factor"]
        factor_intro = row["factor_intro"]
        category_intro = row["category_intro"]
        row_str = "\"{}\",\t\t# {}\t{}".format(factor_name, factor_intro, category_intro)
        print(row_str)


# 打印所有股票，以生成 SECURITY_LIST 列表
def display_all_securities():
    all_securities = get_all_securities(types=['stock'])
    for security_code, row in all_securities.iterrows():
        security_cn_name = row["display_name"]
        security_en_name = row["name"]
        row_str = "\"{}\",\t\t# {}\t{}".format(security_code, security_cn_name, security_en_name)
        print(row_str)


# 获取单个股票的所有聚宽因子库因子
def get_single_jq_factor_values(security, start_date, end_date):
    time_span_list = get_span_list(start_date, end_date, span=365)
    total_factor_df = pd.DataFrame()
    for temp_start_date, temp_end_date in time_span_list:
        # 单次请求因子值不能超过200000个，所以要做下切分，频率为天，每天05：00更新前一天数据
        factor_data = jq.get_factor_values(securities=[security], factors=FACTOR_NAME_LIST, start_date=temp_start_date, end_date=temp_end_date)
        date_df = pd.to_datetime(factor_data[FACTOR_NAME_LIST[0]].index, format='%d/%b/%Y:%H:%M:%S').strftime("%Y-%m-%d").to_series()
        factor_df_list = [date_df] + [factor_data[i] for i in FACTOR_NAME_LIST]
        factor_df = pd.concat(factor_df_list, axis=1)
        factor_df.columns = ["event_date"] + FACTOR_NAME_LIST
        total_factor_df = pd.concat([total_factor_df, factor_df], axis=0)
    total_factor_df.reset_index(drop=True, inplace=True)

    # 删除所有因子都为空的行
    is_null_list = total_factor_df.drop(columns=["event_date"]).apply(lambda row: row.isnull().all(), axis=1).values.tolist()
    drop_index_list = [index for index, is_null in enumerate(is_null_list) if is_null]
    total_factor_df.drop(labels=drop_index_list, inplace=True, axis=0)
    return total_factor_df


# 获取多股票多日的聚宽因子库因子
def get_multiple_jq_factor_values(security_list, start_date, end_date):
    date_list = get_date_list(start_date, end_date)
    factor_df = pd.DataFrame()
    for date in date_list:
        multiple_factor_df_list = []
        for security in security_list:
            multiple_factor_df = get_single_jq_factor_values(security, date, date)
            security_df = pd.DataFrame({"security": [security]}, index=[0])
            factor_df_list = [multiple_factor_df["event_date"], security_df] + [multiple_factor_df[i] for i in FACTOR_NAME_LIST]
            multiple_factor_df = pd.concat(factor_df_list, axis=1)
            multiple_factor_df.columns = ["event_date", "security"] + FACTOR_NAME_LIST
            multiple_factor_df_list.append(multiple_factor_df)
        temp_factor_df = pd.concat(multiple_factor_df_list, axis=0).reset_index(drop=True)
        is_null_list = temp_factor_df.drop(columns=["event_date", "security"]).apply(lambda row: row.isnull().all(), axis=1).values.tolist()
        drop_index_list = [index for index, is_null in enumerate(is_null_list) if is_null]
        temp_factor_df.drop(labels=drop_index_list, inplace=True, axis=0)
        if temp_factor_df.empty:
            continue
        factor_df = pd.concat([factor_df, temp_factor_df], axis=0)
    if factor_df.empty:
        return factor_df
    factor_df = factor_df.sort_values(by=["security", "event_date"], ascending=[True, True]).reset_index(drop=True)
    return factor_df


def get_multiple_jq_factor_values2(security_list, start_date, end_date):
    multiple_factor_df_list = []
    for security in security_list:
        multiple_factor_df = get_single_jq_factor_values(security, start_date, end_date)
        security_df = pd.DataFrame({"security": [security] * len(multiple_factor_df)}, index=list(range(len(multiple_factor_df))))
        factor_df_list = [multiple_factor_df["event_date"], security_df] + [multiple_factor_df[i] for i in FACTOR_NAME_LIST]
        multiple_factor_df = pd.concat(factor_df_list, axis=1)
        multiple_factor_df.columns = ["event_date", "security"] + FACTOR_NAME_LIST
        multiple_factor_df_list.append(multiple_factor_df)
    factor_df = pd.concat(multiple_factor_df_list, axis=0).reset_index(drop=True)
    is_null_list = factor_df.drop(columns=["event_date", "security"]).apply(lambda row: row.isnull().all(), axis=1).values.tolist()
    drop_index_list = [index for index, is_null in enumerate(is_null_list) if is_null]
    factor_df.drop(labels=drop_index_list, inplace=True, axis=0)
    if factor_df.empty:
        return factor_df
    factor_df = factor_df.sort_values(by=["security", "event_date"], ascending=[True, True]).reset_index(drop=True)
    return factor_df


def get_multiple_jq_factor_values3(security_list, start_date, end_date):
    date_list = get_date_list(start_date, end_date)
    total_factor_df = pd.DataFrame()
    for date in date_list:
        batch_size = 100
        for sub_index in range(math.ceil(len(security_list) / batch_size)):
            sub_security_list = security_list[sub_index * batch_size: (sub_index + 1) * batch_size]
            factor_data_dict = jq.get_factor_values(securities=sub_security_list, factors=FACTOR_NAME_LIST, start_date=date, end_date=date)
            if sum([0 if factor_name in factor_data_dict else 1 for factor_name in FACTOR_NAME_LIST]) != 0:
                continue    # TODO
            factor_data_list = [factor_data_dict[factor_name] for factor_name in FACTOR_NAME_LIST]
            multiple_factor_df = pd.concat(factor_data_list, axis=0)
            multiple_factor_df = multiple_factor_df.T
            multiple_factor_df.columns = FACTOR_NAME_LIST
            date_df = pd.DataFrame({"event_date": [date] * len(multiple_factor_df)}, index=sub_security_list)
            security_df = multiple_factor_df[FACTOR_NAME_LIST[0]].index.to_series()
            factor_df_list = [date_df, security_df] + [multiple_factor_df[i] for i in FACTOR_NAME_LIST]
            factor_df = pd.concat(factor_df_list, axis=1)
            factor_df.columns = ["event_date", "security"] + FACTOR_NAME_LIST
            total_factor_df = pd.concat([total_factor_df, factor_df], axis=0)
    if total_factor_df.empty:
        return total_factor_df
    total_factor_df = total_factor_df.reset_index(drop=True)
    is_null_list = total_factor_df.drop(columns=["event_date", "security"]).apply(lambda row: row.isnull().all(), axis=1).values.tolist()
    drop_index_list = [index for index, is_null in enumerate(is_null_list) if is_null]
    total_factor_df.drop(labels=drop_index_list, inplace=True, axis=0)
    if total_factor_df.empty:
        return total_factor_df
    total_factor_df = total_factor_df.sort_values(by=["security", "event_date"], ascending=[True, True]).reset_index(drop=True)
    return total_factor_df


# 获取单股票多日的 alpha101 所有因子
def get_single_alpha101_factor_values(security, start_date, end_date):
    index_list = [str(i).zfill(3) for i in range(1, 102) if i not in UNIMPLEMENTED_ALPHA101_LIST]
    date_list = get_date_list(start_date, end_date)
    factor_df = pd.DataFrame()
    for date in date_list:
        single_factor_df_list = [pd.DataFrame({"event_date": date}, index=[security])]
        for index_str in index_list:
            single_factor_df = eval("jq.alpha101.alpha_{}".format(index_str))(index=[security], enddate=date)
            single_factor_df_list.append(single_factor_df)
        temp_factor_df = pd.concat(single_factor_df_list, axis=1)
        is_null = temp_factor_df.drop(columns=["event_date"]).apply(lambda row: row.isnull().all(), axis=1).values.tolist()[0]
        if is_null:
            continue
        factor_df = pd.concat([factor_df, temp_factor_df], axis=0)
    if factor_df.empty:
        return factor_df
    factor_df.columns = ["event_date"] + ["alpha101_{}".format(index_str) for index_str in index_list]
    factor_df.reset_index(drop=True, inplace=True)
    return factor_df


# 获取多股票多日的 alpha101 所有因子
def get_multiple_alpha101_factor_values(security_list, start_date, end_date):
    index_list = [str(i).zfill(3) for i in range(1, 102) if i not in UNIMPLEMENTED_ALPHA101_LIST]
    date_list = get_date_list(start_date, end_date)
    factor_df = pd.DataFrame()
    for date in date_list:
        multiple_factor_df_list = [pd.DataFrame({"event_date": date, "security": security_list}, index=security_list)]
        for index_str in index_list:
            multiple_factor_df = eval("jq.alpha101.alpha_{}".format(index_str))(index=security_list, enddate=date)
            multiple_factor_df_list.append(multiple_factor_df)
        temp_factor_df = pd.concat(multiple_factor_df_list, axis=1).reset_index(drop=True)
        is_null_list = temp_factor_df.drop(columns=["event_date", "security"]).apply(lambda row: row.isnull().all(), axis=1).values.tolist()
        drop_index_list = [index for index, is_null in enumerate(is_null_list) if is_null]
        temp_factor_df.drop(labels=drop_index_list, inplace=True, axis=0)
        if temp_factor_df.empty:
            continue
        factor_df = pd.concat([factor_df, temp_factor_df], axis=0)
    if factor_df.empty:
        return factor_df
    factor_df.columns = ["event_date", "security"] + ["alpha101_{}".format(index_str) for index_str in index_list]
    factor_df = factor_df.sort_values(by=["security", "event_date"], ascending=[True, True]).reset_index(drop=True)
    return factor_df


# 获取单股票多日的 alpha191 所有因子
def get_single_alpha191_factor_values(security, start_date, end_date):
    index_list = [str(i).zfill(3) for i in range(1, 192) if i not in UNIMPLEMENTED_ALPHA191_LIST]
    date_list = get_date_list(start_date, end_date)
    factor_df = pd.DataFrame()
    for date in date_list:
        single_factor_df_list = [pd.DataFrame({"event_date": date}, index=[security])]
        for index_str in index_list:
            single_factor_df = eval("jq.alpha191.alpha_{}".format(index_str))(code=[security], end_date=date)
            single_factor_df_list.append(single_factor_df)
        temp_factor_df = pd.concat(single_factor_df_list, axis=1)
        is_null = temp_factor_df.drop(columns=["event_date"]).apply(lambda row: row.isnull().all(), axis=1).values.tolist()[0]
        if is_null:
            continue
        factor_df = pd.concat([factor_df, temp_factor_df], axis=0)
    if factor_df.empty:
        return factor_df
    factor_df.columns = ["event_date"] + ["alpha191_{}".format(index_str) for index_str in index_list]
    factor_df.reset_index(drop=True, inplace=True)
    return factor_df


# 获取多股票多日的 alpha191 所有因子
def get_multiple_alpha191_factor_values(security_list, start_date, end_date):
    index_list = [str(i).zfill(3) for i in range(1, 192) if i not in UNIMPLEMENTED_ALPHA191_LIST]
    date_list = get_date_list(start_date, end_date)
    factor_df = pd.DataFrame()
    for date in date_list:
        multiple_factor_df_list = [pd.DataFrame({"event_date": date, "security": security_list}, index=security_list)]
        for index_str in index_list:
            multiple_factor_df = eval("jq.alpha191.alpha_{}".format(index_str))(code=security_list, end_date=date)
            multiple_factor_df_list.append(multiple_factor_df)
        temp_factor_df = pd.concat(multiple_factor_df_list, axis=1).reset_index(drop=True)
        is_null_list = temp_factor_df.drop(columns=["event_date", "security"]).apply(lambda row: row.isnull().all(), axis=1).values.tolist()
        drop_index_list = [index for index, is_null in enumerate(is_null_list) if is_null]
        temp_factor_df.drop(labels=drop_index_list, inplace=True, axis=0)
        if temp_factor_df.empty:
            continue
        factor_df = pd.concat([factor_df, temp_factor_df], axis=0)
    if factor_df.empty:
        return factor_df
    factor_df.columns = ["event_date", "security"] + ["alpha191_{}".format(index_str) for index_str in index_list]
    factor_df = factor_df.sort_values(by=["security", "event_date"], ascending=[True, True]).reset_index(drop=True)
    return factor_df


# 将多日 alpha101 因子保存成 csv
def save_alpha101_factor(file_dir, security_list, start_date, end_date, span):
    time_span_list = get_span_list(start_date, end_date, span)
    for temp_start_date, temp_end_date in time_span_list:
        if temp_start_date == temp_end_date:
            file_name = "{}.csv".format(temp_start_date)
        else:
            file_name = "{}-{}.csv".format(temp_start_date, temp_end_date)
        factor_df = get_multiple_alpha101_factor_values(security_list, temp_start_date, temp_end_date)
        if factor_df.empty:
            continue
        factor_df.to_csv(os.path.join(file_dir, file_name))


# 将多日 alpha191 因子保存成 csv
def save_alpha191_factor(file_dir, security_list, start_date, end_date, span):
    time_span_list = get_span_list(start_date, end_date, span)
    for temp_start_date, temp_end_date in time_span_list:
        if temp_start_date == temp_end_date:
            file_name = "{}.csv".format(temp_start_date)
        else:
            file_name = "{}-{}.csv".format(temp_start_date, temp_end_date)
        factor_df = get_multiple_alpha191_factor_values(security_list, temp_start_date, temp_end_date)
        if factor_df.empty:
            continue
        factor_df.to_csv(os.path.join(file_dir, file_name))


# 将多日聚宽因子库因子保存成 csv
def save_jq_factor(file_dir, security_list, start_date, end_date, span):
    time_span_list = get_span_list(start_date, end_date, span)
    for temp_start_date, temp_end_date in time_span_list:
        if temp_start_date == temp_end_date:
            file_name = "{}.csv".format(temp_start_date)
        else:
            file_name = "{}-{}.csv".format(temp_start_date, temp_end_date)
        factor_df = get_multiple_jq_factor_values3(security_list, temp_start_date, temp_end_date)
        # factor_df = get_multiple_jq_factor_values(security_list, temp_start_date, temp_end_date)
        if factor_df.empty:
            continue
        factor_df.to_csv(os.path.join(file_dir, file_name))


if __name__ == "__main__":
    auth(USER_NAME, PASSWORD)
    download_type = "alpha191"     # alpha101 alpha191 jq_factor
    if download_type == "alpha101":
        file_dir = "/Users/hedongfeng/Desktop/jq_local_data/alpha101/2015-2021"
        security_list = SECURITY_LIST
        start_date = "2018-10-08"
        end_date = "2018-10-09"
        span = 1
        save_alpha101_factor(file_dir, security_list, start_date, end_date, span)
    elif download_type == "alpha191":
        file_dir = "/Users/hedongfeng/Desktop/jq_local_data/alpha191/2015-2021"
        security_list = SECURITY_LIST
        start_date = "2020-12-12"
        end_date = "2021-03-08"
        span = 1
        save_alpha191_factor(file_dir, security_list, start_date, end_date, span)
    elif download_type == "jq_factor":
        file_dir = "/Users/hedongfeng/Desktop/jq_local_data/jq_factor/2015-2021"
        security_list = SECURITY_LIST
        start_date = "2020-12-12"
        end_date = "2021-03-08"
        span = 1
        save_jq_factor(file_dir, security_list, start_date, end_date, span)



    # 多线程下载
    # security = "300015.XSHE"
    # security_list = ["000001.XSHE", "000002.XSHE", "300015.XSHE"]
    # start_date = "2019-01-03"
    # end_date = "2019-01-14"

    # now = time.time()
    # pool = Pool(processes=4)
    # apply_results = []
    # for i in range(4):
    #     apply_results.append(pool.apply_async(get_multiple_alpha191_factor_values, (security_list, start_date, end_date)))
    # pool.close()
    # pool.join()
    # result_list = []
    # for res in apply_results:
    #     result = res.get()
    #     result_list.append(result)
    # print(time.time() - now)
    # factor_values = get_single_jq_factor_values(security, start_date, end_date)
    # alpha_001 = jq.alpha101.alpha_001('2015-12-24',['000001.XSHE','000002.XSHE'])
    # a = eval("jq.alpha101.alpha_001")('2015-12-24',['000001.XSHE','000002.XSHE'])
    # get_single_alpha191_factor_values(security, start_date, end_date)
    # now = time.time()
    # get_multiple_alpha191_factor_values(security_list, start_date, end_date)
    # print(time.time() - now)
    # jq.alpha101.
    # print()