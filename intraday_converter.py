import numpy as np
import pandas as pd
import struct


# 由于xydata底层兼容了代码，因此该函数不需要了
# def s_info_windcode_to_code(s_info_windcode):
#     if s_info_windcode[-2:] == 'SH':
#         code = 10000000 + int(s_info_windcode[0:6])
#     elif s_info_windcode[-2:] == 'SZ':
#         code = 20000000 + int(s_info_windcode[0:6])
#     else:
#         raise ValueError('s_info_windcode error:{}'.format(s_info_windcode))
#     return str(code)


class IntradayConverter(object):
    type_str = ''
    src_columns = []
    target_columns = []

    def __init__(self, datasource):
        self.datasource = datasource
        self.data_type = self.__class__.__name__[:-len('Converter')]

    def get_data_list(self, code, date):
        buffer = self.datasource.Read(self.data_type, code, date.strftime('%Y%m%d'))
        try:
            size = len(buffer)
        except TypeError:
            return None

        line_size = struct.calcsize(self.type_str)
        data_list = []
        for i in range(0, size, line_size):
            line = buffer[i:(i + line_size)]
            if not line:
                break
            s = struct.Struct(self.type_str).unpack_from(line)
            data_list.append(s)
        return data_list

    def get_df(self, code, date):
        l = self.get_data_list(code, date)
        df = pd.DataFrame(l, columns=self.src_columns)
        return df


class K60Converter(IntradayConverter):
    type_str = '3iq6i2qi5q'
    src_columns = ['CODE', 'TRADE_DATE', 'TIME', 'LOCAL_TIME', 'FREQ', 'IDX', 'OPEN', 'HIGH', 'LOW', 'CLOSE',
                   'VOLUME', 'AMOUNT', 'OPENINT', 'DELTAA', 'DELTAV', 'DELTAO', 'EX1', 'EX2']
    target_columns = ['S_INFO_WINDCODE', 'TRADE_DT', 'TIME', 'FREQ', 'IDX', 'OPEN', 'HIGH', 'LOW', 'CLOSE',
                      'VOLUME', 'AMOUNT', 'OPENINT', 'DELTAA', 'DELTAV', 'DELTAO', 'EX1', 'EX2']

    def get_df(self, code, date):
        df = super().get_df(code, date)
        df['S_INFO_WINDCODE'] = np.where(df['CODE'].astype(str).str[:2] == '10', df['CODE'].astype(str).str[2:] + '.SH',
                                         df['CODE'].astype(str).str[2:] + '.SZ')
        df['TIME'] = pd.to_datetime(df['TRADE_DATE'].astype(str).str.cat(df['TIME'].astype(str).str.zfill(6)))
        df['TRADE_DT'] = pd.to_datetime(df['TRADE_DATE'].astype(str))
        df['OPEN'] = df['OPEN'] / 1000
        df['HIGH'] = df['HIGH'] / 1000
        df['LOW'] = df['LOW'] / 1000
        df['CLOSE'] = df['CLOSE'] / 1000
        df['AMOUNT'] = df['AMOUNT'] / 1000
        df['DELTAA'] = df['DELTAA'] / 1000
        df['VOLUME'] = df['VOLUME'] / 100
        df['DELTAV'] = df['DELTAV'] / 100
        df = df[self.target_columns]
        df = df.rename(columns=lambda x: self.data_type.upper() + '_' + x if x not in ['S_INFO_WINDCODE', 'TRADE_DT', 'TIME'] else x)
        return df


class TickConverter(IntradayConverter):
    type_str = 'q3i2qiqiq4i'
    src_columns = ['LOCAL_TIME', 'TRADE_DATE', 'TIME', 'MATCH', 'VOLUME', 'AMOUNT', 'ASK1',
                   'ASK1_VOLUME', 'BID1', 'BID1_VOLUME', 'OPENINT', 'CODE', 'HIGH', 'LOW']
    target_columns = ['S_INFO_WINDCODE', 'TRADE_DT', 'TIME', 'MATCH', 'VOLUME', 'AMOUNT', 'ASK1',
                      'ASK1_VOLUME', 'BID1', 'BID1_VOLUME', 'OPENINT', 'HIGH', 'LOW']

    def get_df(self, code, date):
        df = super().get_df(code, date)
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
        df['VOLUME'] = df['VOLUME'] / 100
        df['BID1_VOLUME'] = df['BID1_VOLUME'] / 100
        df['ASK1_VOLUME'] = df['ASK1_VOLUME'] / 100
        df = df[self.target_columns]
        df = df.rename(columns=lambda x: self.data_type.upper() + '_' + x if x not in ['S_INFO_WINDCODE', 'TRADE_DT', 'TIME'] else x)
        return df


class MarketConverter(IntradayConverter):
    """
    一天一个股票的market 数据在3M左右
     VOLUMUE 单位为手,   TURNOVER 单位为 千元
    """
    type_str = '=32s32s4iIIIII10I10I10I10IIqqqqIIiiII4siii'
    src_columns = ['WINDCODE', 'CODE', 'ACTION_DAY', 'TRADE_DT', 'TIME', 'STATUS', 'PRE_CLOSE', 'OPEN', 'HIGH', 'LOW',
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

    def get_df(self, code, date):
        df = super().get_df(code, date)
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
        df['WEIGHTED_AVG_BID_PRICE'] = df['WEIGHTED_AVG_BID_PRICE'] / 10000
        df['WEIGHTED_AVG_ASK_PRICE'] = df['WEIGHTED_AVG_ASK_PRICE'] / 10000
        df['PRE_CLOSE'] = df['PRE_CLOSE'] / 10000
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
        df = df[self.target_columns]
        df = df.rename(columns=lambda x: self.data_type.upper() + '_' + x if x not in ['S_INFO_WINDCODE', 'TRADE_DT', 'TIME'] else x)
        return df


class Market_EXConverter(IntradayConverter):
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

    def get_df(self, code, date):
        df = super().get_df(code, date)

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
        df = df[self.target_columns]
        df = df.rename(columns=lambda x: self.data_type.upper() + '_' + x if x not in ['S_INFO_WINDCODE', 'TIME'] else x)
        return df


class TransConverter(IntradayConverter):
    """
      一天一个股票的trans数据在3M左右
      VOLUMUE 单位为手,   TURNOVER 单位为 千元
      """
    type_str = '=32s32s3iIiIiccii'

    src_columns = ['WINDCODE', 'CODE', 'TRADE_DT', 'TIME', 'INDEX', 'PRICE', 'VOLUME',
                   'TURNOVER', 'BSFLAG', 'ORDER_KIND', 'FUNCTION_CODE', 'ASK_ORDER', 'BID_ORDER']
    target_columns = ['S_INFO_WINDCODE', 'TRADE_DT', 'TIME', 'INDEX', 'PRICE', 'VOLUME', 'TURNOVER',
                      'BSFLAG', 'ORDER_KIND', 'FUNCTION_CODE', 'ASK_ORDER', 'BID_ORDER']

    def get_df(self, code, date):
        df = super().get_df(code, date)
        df['TIME'] = pd.to_datetime((df['TRADE_DT'] * 1000000 + (df['TIME'] / 1000).astype('int32')).astype(str))
        df['TRADE_DT'] = pd.to_datetime(df['TRADE_DT'].astype(str))
        df['S_INFO_WINDCODE'] = np.where(df['WINDCODE'].astype(str).str[2:3] == '6',
                                         df['WINDCODE'].astype(str).str[2:8] + '.SH',
                                         df['WINDCODE'].astype(str).str[2:8] + '.SZ')
        # df['S_INFO_WINDCODE'] = df['S_INFO_WINDCODE'].astype(str).str[2:11]
        df['PRICE'] = df['PRICE'] / 10000
        df['TURNOVER'] = df['TURNOVER'] / 1000
        df['VOLUME'] = df['VOLUME'] / 100
        df['ORDER_KIND'] = df['ORDER_KIND'].str.decode('UTF-8', 'ignore')
        df['FUNCTION_CODE'] = df['FUNCTION_CODE'].str.decode('UTF-8', 'ignore')
        df = df[self.target_columns]
        df = df.rename(columns=lambda x: self.data_type.upper() + '_' + x if x not in ['S_INFO_WINDCODE', 'TRADE_DT', 'TIME'] else x)
        return df


# class TransHCConverter(IntradayConverter):
#     """
#       一天一个股票的trans数据在3M左右
#       VOLUMUE 单位为股,   TURNOVER 单位为 千元
#       """
#     type_str = '=32s32s3iccc4i'
#     src_columns = ['WINDCODE', 'CODE', 'TRADE_DT', 'TIME', 'INDEX', 'FUNCTION_CODE', 'ORDER_KIND',
#                    'BSFLAG', 'PRICE', 'VOLUME', 'ASK_ORDER', 'BID_ORDER']
#     target_columns = ['S_INFO_WINDCODE', 'TRADE_DT', 'TIME', 'INDEX', 'PRICE', 'VOLUME', 'TURNOVER',
#                       'BSFLAG', 'ORDER_KIND', 'FUNCTION_CODE', 'ASK_ORDER', 'BID_ORDER']
#
#     def get_df(self, code, date):
#         df = super().get_df(code, date)
#         df['TRADE_DT'] = date
#         df['TIME'] = df['TIME'].transform(lambda x: date + pd.Timedelta(hours=x// 10000000, minutes=x % 10000000 // 100000, seconds=x % 100000 // 1000))
#         df['TRADE_DT'] = pd.to_datetime(df['TRADE_DT'].astype(str))
#         df['S_INFO_WINDCODE'] = np.where(df['WINDCODE'].astype(str).str[2:3] == '6',
#                                          df['WINDCODE'].astype(str).str[2:8] + '.SH',
#                                          df['WINDCODE'].astype(str).str[2:8] + '.SZ')
#         # df['S_INFO_WINDCODE'] = df['S_INFO_WINDCODE'].astype(str).str[2:11]
#         df['PRICE'] = df['PRICE'] / 10000
#         df['TURNOVER'] = df['PRICE'] * df['VOLUME'] / 1000
#         df['ORDER_KIND'] = df['ORDER_KIND'].str.decode('UTF-8', 'ignore')
#         df['FUNCTION_CODE'] = df['FUNCTION_CODE'].str.decode('UTF-8', 'ignore')
#         df = df[self.target_columns]
#         df = df.rename(columns=lambda x: self.data_type.upper() + '_' + x if x not in ['S_INFO_WINDCODE', 'TRADE_DT', 'TIME'] else x)
#         return df


class IndexConverter(IntradayConverter):
    type_str = '=32s32siiiiiiiqqi'

    src_columns = ['WINDCODE', 'CODE', 'OP_DT', 'TRADE_DT', 'TIME', 'OPEN', 'HIGH', 'LOW',
                   'MATCH', 'VOLUME', 'AMOUNT', 'PRE_CLOSE']
    target_columns = ['S_INFO_WINDCODE', 'TRADE_DT', 'TIME', 'OPEN', 'HIGH', 'LOW',
                      'MATCH', 'VOLUME', 'AMOUNT', 'PRE_CLOSE']

    def get_df(self, code, date):
        df = super().get_df(code, date)
        df['TIME'] = pd.to_datetime((df['TRADE_DT'] * 1000000 + (df['TIME'] / 1000).astype('int32')).astype(str))
        df['TRADE_DT'] = pd.to_datetime(df['TRADE_DT'].astype(str))
        # xydata S_INFO_WINDCODE
        df['S_INFO_WINDCODE'] = df['WINDCODE'].str.decode('UTF-8', 'ignore').str[:10]
        df['OPEN'] = df['OPEN'] / 10000
        df['HIGH'] = df['HIGH'] / 10000
        df['LOW'] = df['LOW'] / 10000
        df['MATCH'] = df['MATCH'] / 10000
        df['AMOUNT'] = df['AMOUNT'] / 10
        df['PRE_CLOSE'] = df['PRE_CLOSE'] / 10000
        df = df[self.target_columns]
        df = df.rename(columns=lambda x: self.data_type.upper() + '_' + x if x not in ['S_INFO_WINDCODE', 'TRADE_DT', 'TIME'] else x)
        return df


class OrderConverter(IntradayConverter):
    type_str = '32s32siiiiicc'

    src_columns = ['WINDCODE', 'CODE', 'OP_DT', 'TIME', 'ORDER', 'PRICE', 'VOLUME',
                   'ORDERKIND', 'FUNCTIONCODE']
    target_columns = ['S_INFO_WINDCODE', 'TRADE_DT', 'TIME', 'ORDER', 'PRICE', 'VOLUME',
                      'ORDERKIND', 'FUNCTIONCODE']

    def get_df(self, code, date):
        df = super().get_df(code, date)
        # xydata S_INFO_WINDCODE
        df['S_INFO_WINDCODE'] = df['WINDCODE'].str.decode('UTF-8', 'ignore').str.strip(b'\x00'.decode('utf-8'))
        df['TIME'] = pd.to_datetime((df['OP_DT'] * 1000000 + (df['TIME'] / 1000).astype('int32')).astype(str))
        df['TRADE_DT'] = pd.to_datetime(df['OP_DT'].astype(str))
        df['PRICE'] = df['PRICE'] / 10000
        df['ORDERKIND'] = df['ORDERKIND'].str.decode('UTF-8', 'ignore')
        df['FUNCTIONCODE'] = df['FUNCTIONCODE'].str.decode('UTF-8', 'ignore')
        df = df[self.target_columns]
        df = df.rename(columns=lambda x: self.data_type.upper() + '_' + x if x not in ['S_INFO_WINDCODE', 'TRADE_DT', 'TIME'] else x)
        return df
