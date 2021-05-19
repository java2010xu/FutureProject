import pandas as pd
import numpy as np
import platform
from xydata.xyData import XYData
import struct


if platform.system() == "Linux":
    data = XYData("/mnt/XYData")
else:
    data = XYData("\\\\shanghai\\XYData")

src_columns = ['LOCAL_TIME', 'TRADE_DATE', 'TIME', 'MATCH', 'VOLUME', 'AMOUNT', 'ASK1',
               'ASK1_VOLUME', 'BID1', 'BID1_VOLUME', 'OPENINT', 'CODE', 'HIGH', 'LOW']

target_columns = ['S_INFO_WINDCODE', 'TRADE_DT', 'TIME', 'MATCH', 'VOLUME', 'AMOUNT', 'ASK1',
                  'ASK1_VOLUME', 'BID1', 'BID1_VOLUME', 'OPENINT', 'HIGH', 'LOW']

type_str = 'q3i2qiqiq4i'


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
        # 使用xydata 读取数据
        buffer = data.Read("Tick", s_info_windcode_to_code(windcode), trade_dt)

        # 将数据解成df
        size = len(buffer)
        line_size = struct.calcsize(type_str)
        data_list = []
        for i in range(0, size, line_size):
            line = buffer[i:(i + line_size)]
            if not line:
                break
            s = struct.Struct(type_str).unpack_from(line)
            data_list.append(s)
        df = pd.DataFrame(data_list, columns=src_columns)
        return df
    except Exception as e:
       print(e)


def convert_col(df):
    df['S_INFO_WINDCODE'] = np.where(df['CODE'].astype(str).str[:2] == '10', df['CODE'].astype(str).str[2:] + '.SH',
                                     df['CODE'].astype(str).str[2:] + '.SZ')
    df['TRADE_DT'] = pd.to_datetime(df['TRADE_DATE'].astype(str))
    df['TIME'] = pd.to_datetime(df['TRADE_DATE'].astype(str).str.cat(df['TIME'].astype(str).str.zfill(6)))
    df['MATCH'] = df['MATCH'] / 1000
    df['HIGH'] = df['HIGH'] / 1000
    df['LOW'] = df['LOW'] / 1000
    df['ASK1'] = df['ASK1'] / 1000
    df['BID1'] = df['BID1'] / 1000
    df['AMOUNT'] = df['AMOUNT'] / 1000
    df = df[target_columns]
    # df = df.rename(columns=lambda x: x.data_type + '_' + x if x not in ['S_INFO_WINDCODE', 'TRADE_DT', 'TIME'] else x)
    return df


if __name__ == '__main__':
    df = read_xydata('000001.SZ', pd.to_datetime('20200930'))
    df = convert_col(df)

    print(df)