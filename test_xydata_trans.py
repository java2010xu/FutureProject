import pandas as pd
import numpy as np
import platform
from xydata.xyData import XYData
import struct

if platform.system() == "Linux":
    data = XYData("/mnt/XYData")
else:
    data = XYData("\\\\shanghai\\XYData")

market_col = ['TURNOVER', 'HIGH', 'LOW', 'MATCH',
                   'ASK_PRICE1', 'ASK_PRICE2', 'ASK_PRICE3', 'ASK_PRICE4', 'ASK_PRICE5',
                   'ASK_PRICE6', 'ASK_PRICE7', 'ASK_PRICE8', 'ASK_PRICE9', 'ASK_PRICE10',
                   'BID_PRICE1', 'BID_PRICE2', 'BID_PRICE3', 'BID_PRICE4', 'BID_PRICE5',
                   'BID_PRICE6', 'BID_PRICE7', 'BID_PRICE8', 'BID_PRICE9', 'BID_PRICE10',
                   'ASK_VOL1', 'ASK_VOL2', 'ASK_VOL3', 'ASK_VOL4', 'ASK_VOL5',
                   'ASK_VOL6', 'ASK_VOL7', 'ASK_VOL8', 'ASK_VOL9', 'ASK_VOL10',
                   'BID_VOL1', 'BID_VOL2', 'BID_VOL3', 'BID_VOL4', 'BID_VOL5',
                   'BID_VOL6', 'BID_VOL7', 'BID_VOL8', 'BID_VOL9', 'BID_VOL10',
                   'VOLUME', 'WINDCODE', 'TIME']

target_columns = ['S_INFO_WINDCODE', 'TRADE_DT', 'TIME','TURNOVER', 'HIGH', 'LOW', 'MATCH',
                      'ASK_PRICE1', 'ASK_PRICE2', 'ASK_PRICE3', 'ASK_PRICE4', 'ASK_PRICE5',
                      'ASK_PRICE6', 'ASK_PRICE7', 'ASK_PRICE8', 'ASK_PRICE9', 'ASK_PRICE10',
                      'BID_PRICE1', 'BID_PRICE2', 'BID_PRICE3', 'BID_PRICE4', 'BID_PRICE5',
                      'BID_PRICE6', 'BID_PRICE7', 'BID_PRICE8', 'BID_PRICE9', 'BID_PRICE10',
                      'ASK_VOL1', 'ASK_VOL2', 'ASK_VOL3', 'ASK_VOL4', 'ASK_VOL5',
                      'ASK_VOL6', 'ASK_VOL7', 'ASK_VOL8', 'ASK_VOL9', 'ASK_VOL10',
                      'BID_VOL1', 'BID_VOL2', 'BID_VOL3', 'BID_VOL4', 'BID_VOL5',
                      'BID_VOL6', 'BID_VOL7', 'BID_VOL8', 'BID_VOL9', 'BID_VOL10',
                      'VOLUME']

market_typ = '=1q44I32sI'


#  将windcode 转换为原始code
def s_info_windcode_to_code(s_info_windcode):
    if s_info_windcode[-2:] == 'SH':
        code = 10000000 + int(s_info_windcode[0:6])
    elif s_info_windcode[-2:] == 'SZ':
        code = 20000000 + int(s_info_windcode[0:6])
    else:
        raise ValueError('s_info_windcode error:{}'.format(s_info_windcode))
    return str(code)


# 读取xydata中的 market 和trans
def read_xydata(windcode, trade_dt):
    trade_dt = trade_dt.strftime("%Y%m%d")
    try:
        #  使用xydata 读取数据
        # buffer = data.Read("Tick", s_info_windcode_to_code(windcode), trade_dt)
        buffer = data.Read("Market_EX", s_info_windcode_to_code(windcode), trade_dt)

        #将数据解成df
        size = len(buffer)
        line_size = struct.calcsize(market_typ)
        data_list = []
        for i in range(0, size, line_size):
            line = buffer[i:(i + line_size)]
            if not line:
                break
            s = struct.Struct(market_typ).unpack_from(line)
            data_list.append(s)
        df = pd.DataFrame(data_list, columns=market_col)
        return df
    except Exception as e:
       print(e)

def convert_df(df, date):
    """
    一天一个股票的market 数据在3M左右
    VOLUMUE 单位为股, TURNOVER 单位为 千元
    """
    df['S_INFO_WINDCODE'] = df['WINDCODE'].astype(str).str[2:11]
    df['TIME'] = pd.to_datetime(date.strftime('%Y%m%d') + (df['TIME']//1000).astype(str).str.rjust(6, '0'))
    df['TRADE_DT'] = date
    df['MATCH'] = df['MATCH'] / 10000
    df['HIGH'] = df['HIGH'] / 10000
    df['LOW'] = df['LOW'] / 10000
    df['ASK_PRICE1'] = df['ASK_PRICE1'] / 10000
    df['ASK_PRICE2'] = df['ASK_PRICE2'] / 10000
    df['ASK_PRICE3'] = df['ASK_PRICE3'] / 10000
    df['ASK_PRICE4'] = df['ASK_PRICE4'] / 10000
    df['ASK_PRICE5'] = df['ASK_PRICE5'] / 10000
    df['ASK_PRICE6'] = df['ASK_PRICE6'] / 10000
    df['ASK_PRICE7'] = df['ASK_PRICE7'] / 10000
    df['ASK_PRICE8'] = df['ASK_PRICE8'] / 10000
    df['ASK_PRICE9'] = df['ASK_PRICE9'] / 10000
    df['ASK_PRICE10'] = df['ASK_PRICE10'] / 10000
    df['BID_PRICE1'] = df['BID_PRICE1'] / 10000
    df['BID_PRICE2'] = df['BID_PRICE2'] / 10000
    df['BID_PRICE3'] = df['BID_PRICE3'] / 10000
    df['BID_PRICE4'] = df['BID_PRICE4'] / 10000
    df['BID_PRICE5'] = df['BID_PRICE5'] / 10000
    df['BID_PRICE6'] = df['BID_PRICE6'] / 10000
    df['BID_PRICE7'] = df['BID_PRICE7'] / 10000
    df['BID_PRICE8'] = df['BID_PRICE8'] / 10000
    df['BID_PRICE9'] = df['BID_PRICE9'] / 10000
    df['BID_PRICE10'] = df['BID_PRICE10'] / 10000
    df['TURNOVER'] = df['TURNOVER'] / 1000
    df['VOLUME'] = df['VOLUME'] / 100
    df['ASK_VOL1'] = df['ASK_VOL1'] / 100
    df['ASK_VOL2'] = df['ASK_VOL2'] / 100
    df['ASK_VOL3'] = df['ASK_VOL3'] / 100
    df['ASK_VOL4'] = df['ASK_VOL4'] / 100
    df['ASK_VOL5'] = df['ASK_VOL5'] / 100
    df['ASK_VOL6'] = df['ASK_VOL6'] / 100
    df['ASK_VOL7'] = df['ASK_VOL7'] / 100
    df['ASK_VOL8'] = df['ASK_VOL8'] / 100
    df['ASK_VOL9'] = df['ASK_VOL9'] / 100
    df['ASK_VOL10'] = df['ASK_VOL10'] / 100
    df['BID_VOL1'] = df['BID_VOL1'] / 100
    df['BID_VOL2'] = df['BID_VOL2'] / 100
    df['BID_VOL3'] = df['BID_VOL3'] / 100
    df['BID_VOL4'] = df['BID_VOL4'] / 100
    df['BID_VOL5'] = df['BID_VOL5'] / 100
    df['BID_VOL6'] = df['BID_VOL6'] / 100
    df['BID_VOL7'] = df['BID_VOL7'] / 100
    df['BID_VOL8'] = df['BID_VOL8'] / 100
    df['BID_VOL9'] = df['BID_VOL9'] / 100
    df['BID_VOL10'] = df['BID_VOL10'] / 100
    df = df[target_columns]
    return df


if __name__ == '__main__':
    df = read_xydata('000001.SZ', pd.to_datetime('20210402'))
    df = convert_df(df, pd.to_datetime('20210402'))
    print(df)
