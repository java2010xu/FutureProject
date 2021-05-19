import os
import struct
import pandas as pd
import platform
from xydata import xyData

"""
1 . 从Tick数据中得到21年1月所有活跃的期货品种（主力日均成交量大于100）的主力合约，的1分钟最新价，
以矩阵形式展示（纵轴第一列为日期yyyymmdd，第二列为时间hhmmss；横轴为品种），内容为分钟最新价，如某品种该分钟无交易，则用上一个价格填充。
同时需要第二个矩阵，横纵轴相同，内容为品种当日对应的主力合约。
"""

def tick_data(main_varieties, date):
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


def get_main_var(path):
    # 获取主力合约,且一个月平均成交量大于100
    files = os.listdir(path)
    contract_dict = {}
    for file in files:
        file_path = f"{path}/{file}"
        df = pd.read_csv(file_path)
        if not df.empty:
            Jan = df[df.iloc[:, 5].astype(str).str.startswith('202101')]
            if Jan.iloc[:,-2].mean() > 100:
                date_var = Jan.iloc[:, [5, 2]].copy()
                var = file.split('_')[0]
                date_var.columns = ['date', var]
                date_var.set_index('date', inplace=True)
                contract_dict[var] = date_var.to_dict()[var]
    return contract_dict


def select_date(path):
    # 获取一月交易日历
    fileName = f"{path}/ag888_pctr.csv"
    df = pd.read_csv(fileName)
    Dec_tail = df[df.iloc[:, 3].astype(str).str.startswith('202012')][-1:]
    Jan = df[df.iloc[:, 3].astype(str).str.startswith('202101')]
    df = pd.concat([Dec_tail, Jan])
    date_list = df.iloc[:, 3].tolist()
    return date_list


def get_data(contract_dict, date_list):
    v_datas = []    # vstack   axis=0
    v_contracts = []
    for date in date_list[1:]:
        h_datas = []    # hstack  axis=1
        h_contracts = []
        for contract in contract_dict.keys():
            data = tick_data(contract, date)  # 调用tick_data函数获取data
            if data is None:
                continue
            data = data.iloc[:, [1, 2, 3]].copy()
            data.columns = ['date', 'time', contract]
            data['time'] = data['time'].astype(str).map(lambda x: (6 - len(x))*'0'+x if len(x) < 6 else x)
            data['datetime'] = pd.to_datetime(data['date'].map(str)+' '+data['time'].map(str))
            data.drop(labels=['date', 'time'], axis=1, inplace=True)
            data = data.set_index('datetime')
            data = data.resample('1min').agg('last')
            data.sort_index(inplace=True)
            # 合约
            main_contract = data.copy()
            main_contract[contract] = contract_dict[contract][date]
            h_datas.append(data)
            h_contracts.append(main_contract)
        datas = pd.concat(h_datas, axis=1)   # 一个交易日所有future_data
        contracts = pd.concat(h_contracts, axis=1)
        v_datas.append(datas)
        v_contracts.append(contracts)
    datas = pd.concat(v_datas, axis=0)   # 一个月所有future_data
    contracts = pd.concat(v_contracts, axis=0)
    datas.fillna(method='ffill', inplace=True)
    contracts.fillna(method='ffill', inplace=True)
    return datas, contracts


def create_time_index(date_list):
    l = []
    for i in range(1, len(date_list)):
        night_date = date_list[i-1]
        date = date_list[i]
        b = pd.to_datetime(str(night_date)) + pd.Timedelta('21:00:00')
        e = pd.to_datetime(str(date)) + pd.Timedelta('15:15:00')
        time_index = pd.Series(pd.date_range(b, e, freq='1min'))
        l.append(time_index)
    time_index = pd.concat(l)
    time_index = pd.DatetimeIndex(time_index)
    return time_index


if __name__ == '__main__':
    path = '/mnt/mainlist'  # 主力合约文件路径
    contract_dict = get_main_var(path)   # 合约列表
    date_list = select_date(path)   # 日期列表
    time_index = create_time_index(date_list)
    datas, contracts = get_data(contract_dict, date_list)
    datas.to_csv('Jan_future_data.csv', index=True)
    contracts.to_csv('Jan_future_contracts.csv', index=True)
