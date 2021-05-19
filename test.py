from intraday_converter import *
from xydata.xyData import XYData


if __name__ == '__main__':
    data = XYData("/mnt/XYData")
    codes = ['002460.SZ', '128126.SZ']
    date = '20210416'
    for code in codes:
        mk = Market_EXConverter(data)
        mk_df = mk.get_df(code, pd.to_datetime(date))
        mk_df.to_csv(f'./download/0.25秒tick_{code}_{date}.csv')

        ts = TransConverter(data)
        ts_df = ts.get_df(code, pd.to_datetime(date))
        ts_df.to_csv(f'./download/成交逐笔_{code}_{date}.csv')

        od = OrderConverter(data)
        od_df = od.get_df(code, pd.to_datetime(date))
        od_df.to_csv(f'./download/委托逐笔_{code}_{date}.csv')

    print('...')
    print('...')
    print('...')
    print('...')