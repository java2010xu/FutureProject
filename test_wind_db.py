import pandas as pd
import sqlalchemy


def get_data(sql1):
    sql_url = 'mysql+pymysql://test:test2019!@192.168.2.22:3306/wind'  # 数据库地址
    ng = sqlalchemy.create_engine(sql_url)
    # sql = f"select S_INFO_WINDCODE, TRADE_DT, S_DQ_TRADESTATUS from wind.AShareEODPrices where TRADE_DT between {start} and {end}"
    table = pd.read_sql(sql1, ng)
    return table

if __name__ == '__main__':
    start, end = '20201215', '20201215'
    sql1 = f"select * from wind.AShareBalanceSheet where ANN_DT BETWEEN {start} and {end}"    # 中国A股资产负债表[AShareBalanceSheet]
    sql2 = f"select * from wind.AShareIncome where ANN_DT BETWEEN {start} and {end}"        # 中国A股利润表[AShareIncome]
    sql3 = f"select * from wind.AShareCashFlow where ANN_DT BETWEEN {start} and {end}"    # 中国A股现金流量表[AShareCashFlow]
    AShareBalanceSheet = get_data(sql1)
    AShareIncome = get_data(sql2)
    AShareCashFlow = get_data(sql3)
    print('a')