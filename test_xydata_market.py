import pandas as pd
import numpy as np
import platform
from xydata.xyData import XYData
import struct

if platform.system() == "Linux":
    data = XYData("/mnt/XYData")
else:
    data = XYData("\\\\shanghai\\XYData")

market_col = ['WINDCODE', 'CODE', 'ACTION_DAY', 'TRADE_DT', 'TIME', 'STATUS', 'PRE_CLOSE', 'OPEN', 'HIGH', 'LOW',
                   'MATCH', 'ASK_PRICE1', 'ASK_PRICE2', 'ASK_PRICE3', 'ASK_PRICE4', 'ASK_PRICE5', 'ASK_PRICE6',
                   'ASK_PRICE7', 'ASK_PRICE8', 'ASK_PRICE9', 'ASK_PRICE10', 'ASK_VOL1', 'ASK_VOL2', 'ASK_VOL3',
                   'ASK_VOL4', 'ASK_VOL5', 'ASK_VOL6', 'ASK_VOL7', 'ASK_VOL8', 'ASK_VOL9', 'ASK_VOL10', 'BID_PRICE1',
                   'BID_PRICE2', 'BID_PRICE3', 'BID_PRICE4', 'BID_PRICE5', 'BID_PRICE6', 'BID_PRICE7', 'BID_PRICE8',
                   'BID_PRICE9', 'BID_PRICE10', 'BID_VOL1', 'BID_VOL2', 'BID_VOL3', 'BID_VOL4', 'BID_VOL5',
                   'BID_VOL6', 'BID_VOL7', 'BID_VOL8', 'BID_VOL9', 'BID_VOL10', 'NUM_TRADE', 'VOLUME',
                   'TURNOVER', 'TOTA_BID_VOL', 'TOTA_ASK_VOL', 'WEIGHTED_AVG_BID_PRICE',
                   'WEIGHTED_AVG_ASK_PRICE', 'IOPV', 'YIELD_TO_MATURITY', 'HIGH_LIMITED', 'LOW_LIMITED',
                   'PREFIX', 'SYL1', 'SYL2', 'SD2']

target_columns = ['S_INFO_WINDCODE', 'TRADE_DT', 'TIME', 'STATUS', 'PRE_CLOSE', 'OPEN', 'HIGH', 'LOW',
                  'MATCH', 'ASK_PRICE1', 'ASK_PRICE2', 'ASK_PRICE3', 'ASK_PRICE4', 'ASK_PRICE5', 'ASK_PRICE6',
                  'ASK_PRICE7', 'ASK_PRICE8', 'ASK_PRICE9', 'ASK_PRICE10', 'ASK_VOL1', 'ASK_VOL2', 'ASK_VOL3',
                  'ASK_VOL4', 'ASK_VOL5', 'ASK_VOL6', 'ASK_VOL7', 'ASK_VOL8', 'ASK_VOL9', 'ASK_VOL10', 'BID_PRICE1',
                  'BID_PRICE2', 'BID_PRICE3', 'BID_PRICE4', 'BID_PRICE5', 'BID_PRICE6', 'BID_PRICE7', 'BID_PRICE8',
                  'BID_PRICE9', 'BID_PRICE10', 'BID_VOL1', 'BID_VOL2', 'BID_VOL3', 'BID_VOL4', 'BID_VOL5',
                  'BID_VOL6', 'BID_VOL7', 'BID_VOL8', 'BID_VOL9', 'BID_VOL10', 'NUM_TRADE', 'VOLUME',
                  'TURNOVER', 'TOTA_BID_VOL', 'TOTA_ASK_VOL', 'WEIGHTED_AVG_BID_PRICE',
                  'WEIGHTED_AVG_ASK_PRICE', 'IOPV', 'YIELD_TO_MATURITY', 'HIGH_LIMITED', 'LOW_LIMITED',
                  'PREFIX', 'SYL1', 'SYL2', 'SD2']

market_typ = '=32s32s4iIIIII10I10I10I10IIqqqqIIiiII4siii'


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
        buffer = data.Read("Market", s_info_windcode_to_code(windcode), trade_dt)

        #将数据解成df
        size = len(buffer)
        line_size = struct.calcsize(market_typ)
        data_list = []
        for i in range(0, size, line_size):
            line = buffer[i:(i + line_size)]
            if not line:
                break
            # if len(line) == 168:    # 自己加的
            #     continue
            s = struct.Struct(market_typ).unpack_from(line)
            data_list.append(s)
        df = pd.DataFrame(data_list, columns=market_col)
        return df
    except Exception as e:
       print(e)

def convert_market_ex(df):
    type_str = '=1q44I32sI'
    src_columns = ['TURNOVER', 'HIGH', 'LOW', 'MATCH',
                   'ASK_PRICE1', 'ASK_PRICE2', 'ASK_PRICE3', 'ASK_PRICE4', 'ASK_PRICE5',
                   'ASK_PRICE6', 'ASK_PRICE7', 'ASK_PRICE8', 'ASK_PRICE9', 'ASK_PRICE10',
                   'BID_PRICE1', 'BID_PRICE2', 'BID_PRICE3', 'BID_PRICE4', 'BID_PRICE5',
                   'BID_PRICE6', 'BID_PRICE7', 'BID_PRICE8', 'BID_PRICE9', 'BID_PRICE10',
                   'ASK_VOL1', 'ASK_VOL2', 'ASK_VOL3', 'ASK_VOL4', 'ASK_VOL5',
                   'ASK_VOL6', 'ASK_VOL7', 'ASK_VOL8', 'ASK_VOL9', 'ASK_VOL10',
                   'BID_VOL1', 'BID_VOL2', 'BID_VOL3', 'BID_VOL4', 'BID_VOL5',
                   'BID_VOL6', 'BID_VOL7', 'BID_VOL8', 'BID_VOL9', 'BID_VOL10',
                   'VOLUME', 'WINDCODE', 'TIME']


def convert_df(df):
    """
    一天一个股票的market 数据在3M左右
     VOLUMUE 单位为股, TURNOVER 单位为 千元
    """
    df['S_INFO_WINDCODE'] = df['WINDCODE'].astype(str).str[2:11]
    df['TIME'] = pd.to_datetime((df['TRADE_DT'] * 1000000 + (df['TIME'] / 1000).astype('int32')).astype(str))
    df['TRADE_DT'] = pd.to_datetime(df['TRADE_DT'].astype(str))
    df['OPEN'] = df['OPEN'] / 10000
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
    df['HIGH_LIMITED'] = df['HIGH_LIMITED'] / 10000
    df['LOW_LIMITED'] = df['LOW_LIMITED'] / 10000
    df['WEIGHTED_AVG_BID_PRICE'] = df['WEIGHTED_AVG_BID_PRICE'] / 10000 # 加权平均卖价
    df['WEIGHTED_AVG_ASK_PRICE'] = df['WEIGHTED_AVG_ASK_PRICE'] / 10000 # 加权平均买价
    df['PRE_CLOSE'] = df['PRE_CLOSE'] / 10000   # 前收盘价
    df['TURNOVER'] = df['TURNOVER'] / 1000
    df = df[target_columns]
    return df


if __name__ == '__main__':
    df = read_xydata('000001.SZ', pd.to_datetime('20210420'))
    df = convert_df(df)

    print(df)