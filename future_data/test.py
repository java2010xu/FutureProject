# -*- coding: utf-8 -*-
"""
# @Time    : 2021/3/22 10:48
# @Author  : xuke
"""

"""
1 . 从Tick数据中得到21年1月所有活跃的期货品种（主力日均成交量大于100）的主力合约，的1分钟最新价，
以矩阵形式展示（纵轴第一列为日期yyyymmdd，第二列为时间hhmmss；横轴为品种），内容为分钟最新价，如某品种该分钟无交易，则用上一个价格填充。
同时需要第二个矩阵，横纵轴相同，内容为品种当日对应的主力合约。
"""


import os
import pandas as pd
import struct
import platform
from xydata import xyData


def tick_data(main_varieties, date):    # 主力合约,日期
    type_str = 'q3i2qiqiq4i'
    if platform.system() == "Linux":
        data = xyData("/mnt/XYData")
    else:
        data = xyData("\\\\shanghai\\XYData")
    try:
        # 简单用法
        result = data.Read("Tick", main_varieties, date)
        if result:
            print(main_varieties, date)
            print("Data size = ", len(result))
        else:
            print("Data does not exist.")

        # 将数据解成df
        size = len(result)
        line_size = struct.calcsize(type_str)
        data_list = []
        for i in range(0, size, line_size):
            line = result[i:(i + line_size)]
            if not line:
                break
            s = struct.Struct(type_str).unpack_from(line)
            data_list.append(s)
        df = pd.DataFrame(data_list)
        return df
    except Exception as e:
        print(e)


def get_main_contract(path):
    contract_list = os.listdir(path)
    contract_dict = {}
    date_list = []
    for contract in contract_list:
        file_path = path+'/'+contract
        df = pd.read_csv(file_path)
        if not df.empty:
            Jan = df[df.iloc[:, 3].astype(str).str.startswith('202101') | df.iloc[:, 5].astype(str).str.startswith('202101')]
            if Jan.iloc[:, -2].mean() > 100:
                date_list += Jan.iloc[:, 5].tolist()
                date_var = Jan.iloc[:, [5, 2]].copy()
                con = contract.split('_')[0]
                date_var.columns = ['date', con]
                date_var.set_index(keys='date', inplace=True)
                contract_dict.update(date_var.to_dict())
    date_list = list(set(date_list))
    return contract_dict, date_list


def get_data(contract_dict, date_list):
    for date in date_list[1:]:
        h_datas = []
        h_contracts = []
        for main_varieties in contract_dict.keys():
            df = tick_data(main_varieties, date)
            if df is None:
                continue
            data = df.iloc[:, [1, 2, 3]].copy()
            data.columns = ['date', 'time', main_varieties]
            data['time'] = data['time'].astype(str).map(lambda x: (6 - len(x)) * '0' + x if len(x) < 6 else x)
            data['datetime'] = pd.to_datetime(data['date'].map(str) + data['time'].map(str))
            data = data.drop(labels=['date', 'time'], axis=1)[['datetime', main_varieties]]
            data = data.set_index('datetime')
            data = data.resample('1min').last()
            data.sort_index(inplace=True)
            # 合约
            main_contract = data.copy()
            main_contract[main_varieties] = contract_dict[main_varieties][date]
            h_datas.append(data)
            h_contracts.append(main_varieties)
        datas = pd.concat(h_datas, axis=1)
        print(datas)


if __name__ == '__main__':
    path = '/mnt/mainlist'
    contract_dict, date_list = get_main_contract(path)
    get_data(contract_dict, date_list)
    print('..')