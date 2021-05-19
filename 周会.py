import pandas as pd
import sqlalchemy


def get_data(sql1):
    sql_url = 'mysql+pymysql://alpha:XYuser@2021@192.168.2.225:3306/wind' # 数据库地址
    ng = sqlalchemy.create_engine(sql_url)
    table = pd.read_sql(sql1, ng)
    return table

if __name__ == '__main__':
    start, end = '20210331', '20210331'
    sql1 = f"select * from wind.AShareEODDerivativeIndicator where TRADE_DT between {start} and {end}"  # 中国A股日行情估值指标
    df = get_data(sql1)
    df.drop(labels=['OBJECT_ID', 'OPDATE', 'OPMODE'], axis=1, inplace=True)
    print('a')