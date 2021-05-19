import os
import struct
import pandas as pd
import numpy as np
import platform
from xydata import xyData


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
    """获取2021年活跃的期货品种,判定为当前年平均成交量大于100"""
    contract_dict = {}
    try:
        files = os.listdir(path)
    except FileNotFoundError as dir_err:
        print('{}文件目录, 没有找到!, {}'.format(path, dir_err))
        raise dir_err
    for file in files:
        file_path = f"{path}/{file}"
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            print('期货品种:{}, 文件获取失败!')
            raise e
        if not df.empty:
            active = df[df.iloc[:, 5].astype(str).str.startswith('2021')]  # 2021年
            if active.iloc[:, -2].mean() > 100:    # 活跃的期货品种
                date_var = active.iloc[:, [5, 2]].copy()
                var = file.split('_')[0]
                date_var.columns = ['date', var]
                date_var.set_index('date', inplace=True)
                contract_dict[var] = date_var.to_dict()[var]
    main_888_list = list(contract_dict.keys())
    return main_888_list


def get_tick_data(main_888_list, myDate):
    """读取tick数据"""
    traget_dict = {}
    minute_vol_list = []
    change_number_list = []
    for contract in main_888_list:
        traget_dict[contract] = {}
        try:
            df = tick_data(contract, myDate)  # 调用tick_data函数获取data
        except Exception as e:
            print('期货合约:{}, xydata获取数据失败!'.format(contract))
            raise e
        if df is None:
            print('日期:{}, 期货品种:{}, 数据为空'.format(myDate, contract))
            traget_dict[contract].update({'买一量均值': np.nan, '卖一量均值': np.nan, '变动次数': np.nan})
            continue
        # 预处理DataFrame
        data = df.copy()
        data.columns = ['0', '日期', '时间', '最新价', '成交量', '成交额', '买一价', '买一量', '卖一价', '卖一量', '持仓量', '64', '最高', '最低']
        data['时间'] = data['时间'].astype(str).map(lambda x: (6 - len(x)) * '0' + x if len(x) < 6 else x)
        data = data.drop(labels=['0', '64'], axis=1)
        # 买一量均值和卖一量均值
        buy_mean, sell_mean = level1_tick_buy_sell_mean(data)
        # 每分钟成交量变动次数
        minute_vol_df, change_number_df = minute_vol_change_number(data, contract)
        minute_vol_list.append(minute_vol_df)
        change_number_list.append(change_number_df)
        traget_dict[contract].update({'买一量均值':buy_mean, '卖一量均值':sell_mean})
    minute_vol = pd.concat(minute_vol_list, axis=1)
    change_number = pd.concat(change_number_list, axis=1)
    return traget_dict, minute_vol, change_number


def level1_tick_buy_sell_mean(data):
    """主力合约Level1 Tick 的买一量均值和卖一量均值（剔除有一边量为0的Tick：代表涨跌停）"""
    buy_mean = data[~(data['买一量'] == 0) | (data['卖一量'] == 0)][['买一量']].mean().squeeze()
    sell_mean = data[~(data['买一量'] == 0) | (data['卖一量'] == 0)][['卖一量']].mean().squeeze()
    return buy_mean, sell_mean


def minute_vol_change_number(data, contract):
    """每分钟成交量，每分钟盘口价格变动次数（和前一个Tick相比，只要买一价和卖一价其中一个发生变化，则计为一次变动。"""
    minute_vol_dict = {}
    change_number_dict = {}
    data['日期时间'] = pd.to_datetime(data['日期'].map(str) + ' ' + data['时间'].map(str))
    data = data.drop(labels=['日期', '时间'], axis=1)
    data = data.set_index('日期时间')
    data['变动'] = np.where((data['买一价'].diff(1) != 0) | (data['卖一价'].diff(1) != 0), True, False)
    minute_vol_dict.update({contract: data['成交量'].diff(1).resample('1min').sum().to_dict()})
    change_number_dict.update({contract: data['变动'].resample('1min').sum().to_dict()})
    minute_vol_df = pd.DataFrame(minute_vol_dict)
    change_number_df = pd.DataFrame(change_number_dict)
    return minute_vol_df, change_number_df


def get_daily_data(main_888_list, myDate, path):
    """读取日K数据"""
    target_dict = {}
    for file in main_888_list:
        target_dict[file] = {}
        file_path = path + file +'_pctr.csv'
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            print('文件读取失败!')
            raise e
        data = df.copy()
        data.columns = ['00', '品种', '主力合约', '下一个交易日期', '01', '日期', '开', '高', '低', '收', '结', '成交量', '持仓量']
        if data is None or data[data['日期'] == myDate].empty:    # 数据为空, 日期内期货品种未上市
            print('日期:{}, 期货品种:{}, 数据为空'.format(myDate, file))
            # 缺失数据默认为最小整形(np.nan)
            target_dict[file].update({'主力合约':np.nan, '收盘价':np.nan, '当日收益率':np.nan, '成交量':np.nan, '十日成交量':np.nan, '当日振幅':np.nan, '十日均振幅':np.nan})
            continue
        main_contract = data[data['日期'] == myDate]['主力合约'].squeeze()
        yield_, close_ = daily_yield(data, myDate)
        vol, vol10 = daily_10_vol(data, myDate)
        amplitude, amplitude10 = daily_amplitude(data, myDate)
        target_dict[file].update({'主力合约':main_contract, '收盘价':close_, '当日收益率':yield_, '成交量':vol,'十日成交量':vol10, '当日振幅':amplitude,'十日均振幅':amplitude10})
    return target_dict


def daily_yield(data, myDate):
    """主力合约:当日收盘价，当日收益率(今收-昨收)/昨收"""
    trade_dt = pd.read_csv('/home/xuk/FutuerProject/future_data/calendar.csv')
    up_idx = trade_dt[trade_dt['TRADE_DAYS'] == myDate].index[0] - 1
    up_date = trade_dt.iloc[up_idx]['TRADE_DAYS'].astype(int)
    data['昨收'] = data['收'].shift(1)
    data['昨主'] = data['主力合约'].shift(1)
    if data[data['日期'] == myDate]['主力合约'].iloc[0] != data[data['日期'] == myDate]['昨主'].iloc[0]:
        up_date_contract = data[data['日期'] == myDate]['主力合约'].iloc[0]
        path = '/mnt/Future/FutureData/all_contract/' + str(up_date) + '/' + up_date_contract + '.csv'
        up_close = pd.read_csv(path)['new'].iloc[-1]
        data['上收'] = up_close
    else:
        data['上收'] = 0
    # 如果当日主力合约等于昨日主力合约，正常计算, 否则收益率等于(开盘价-收盘价/开盘价)
    data['当日收益率'] = np.where(data[data['日期'] == myDate]['主力合约'] == data[data['日期'] == myDate]['昨主'],
                               (data['收'] - data['昨收']) / data['昨收'] * 100,
                               (data['收'] - data['上收']) / data['上收'] * 100)
    yield_ = data[data['日期'] == myDate]['当日收益率'].squeeze()
    close_ = data[data['日期'] == myDate]['收'].squeeze()
    return yield_, close_


def daily_amplitude(data, myDate):
    """主力合约:当日振幅(当日日数据的(最高-最低)/收盘),10日日均振幅"""
    data['振幅'] = (data['高'] - data['低']) / data['收']
    amplitude = data[data['日期'] == myDate]['振幅'].squeeze()
    # 指定日期, 前交易日少于十天, 有几天算几天的平均振幅
    data['十日均振幅'] = np.where(data[data['日期'] == myDate].index[0] >= 10, data['振幅'].rolling(10).mean(), data['振幅'].rolling(int(data[data['日期'] == myDate].index[0]) + 1).mean())
    amplitude10 = data[data['日期'] == myDate][['十日均振幅']].squeeze()
    return amplitude, amplitude10


def daily_10_vol(data, myDate):
    """主力合约:成交量,近10日成交量"""
    vol = data[data['日期'] == myDate]['成交量'].squeeze()
    # 不足十日,十日成交量为nan
    data['最近十日成交量'] = data['成交量'].rolling(10).sum()
    vol10 = data[data['日期'] == myDate]['最近十日成交量'].squeeze()
    return vol, vol10


if __name__ == '__main__':
    myDate = 20210507
    path = '/mnt/Future/mainlist/'
    main_888_list = get_main_var(path)  # 活跃期货品种列表
    daily_dict = get_daily_data(main_888_list, myDate, path)    # 日K数据
    tick_dict, minute_vol, change_number =get_tick_data(main_888_list, myDate)  # tick数据,分钟tick数据
    df = pd.concat([pd.DataFrame(daily_dict), pd.DataFrame(tick_dict)]).T
    df[['当日收益率', '当日振幅', '十日均振幅', '买一量均值', '卖一量均值']] = df[['当日收益率', '当日振幅', '十日均振幅', '买一量均值', '卖一量均值']].astype(float).round(3)


    # # 添充空值为0
    # minute_vol = minute_vol.fillna(0).astype(int)
    # change_number = change_number.fillna(0).astype(int)
    # minute_vol.columns = df['主力合约'].tolist()
    # change_number.columns = df['主力合约'].tolist()

    # 保存文件
    df.to_csv('/home/xuk/FutuerProject/future_data/{}_活跃期货品种指标.csv'.format(myDate), encoding='utf_8_sig')
    minute_vol.to_csv('/home/xuk/FutuerProject/future_data/{}_每分钟成交量.csv'.format(myDate), encoding='utf_8_sig')
    change_number.to_csv('/home/xuk/FutuerProject/future_data/{}_每分钟变动次数.csv'.format(myDate), encoding='utf_8_sig')
