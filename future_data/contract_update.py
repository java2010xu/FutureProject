import pandas as pd
import sqlalchemy

"""
新品种上市
"""
def get_data(sql1):
    sql_url = 'mysql+pymysql://alpha:XYuser@2021@192.168.2.225:3306/wind'  # 数据库地址
    ng = sqlalchemy.create_engine(sql_url)
    table = pd.read_sql(sql1, ng)
    return table

def get_new_contract():
    # todo 路径 '\\192.168.2.3\DataToolSet\tradinghours.csv'
    path = "/home/xuk/FutuerProject/resours/tradinghours.csv"

    # todo 日期动态调整
    date = pd.to_datetime('20210223')
    sql1 = "SELECT * FROM wind.CFuturesDescription WHERE FS_INFO_TYPE=1;"    # 中国期货基本资料[CFuturesDescription]

    CFuturesDescription = get_data(sql1)   # 中国期货基本资料
    # 最后交易日期 >= 当前日期 且 上市日期 >= 当前日期
    new_sccode = CFuturesDescription[(pd.to_datetime(CFuturesDescription['S_INFO_DELISTDATE']) >= date) & (pd.to_datetime(CFuturesDescription['S_INFO_LISTDATE']) >= date)]
    new_sccode_list = new_sccode['FS_INFO_SCCODE'].unique().tolist()
    tradinghours = pd.read_csv(path, encoding='GBK')
    tradinghours = tradinghours[tradinghours['品种'] != '股票']
    old_sccode = tradinghours['品种'].unique().tolist()    # 当前上市的所有期货品种

    l = []
    for new_code in new_sccode_list:
        if not new_code.lower() in [old_code.lower() for old_code in old_sccode]:  # 列表解析
            l.append(new_code)

    # 即将上市的新合约
    new_contract = new_sccode[new_sccode['FS_INFO_SCCODE'].isin(l)].sort_values(by='S_INFO_LISTDATE').reset_index(drop=True)
    new_contract = new_contract[['S_INFO_EXCHMARKET','S_INFO_NAME','FS_INFO_SCCODE','S_INFO_LISTDATE','OPDATE']].copy() # ,'FROM_DAY'
    new_contract.loc[:, 'FROM_DAY'] = pd.to_datetime(new_contract['S_INFO_LISTDATE']) - date
    new_contract.loc[:, 'FROM_DAY'] = new_contract['FROM_DAY'].astype(str).str.replace('00:00:00.000000000','')
    html(new_contract)

def save_new_contract(new_contract):
    # new_contract.to_html('abc')# '/home/xuk/FutuerProject/future_data/contract.html'
    # new_contract.to_csv('/home/xuk/FutuerProject/future_data/new_contract.csv', encoding='utf_8_sig')
    pass
    # todo 发邮件
    # print('即将上市的期货新品种:{},上市日期:{},距离天数:{}'.format(new_contract['S_INFO_NAME'], new_contract['S_INFO_LISTDATE'], ))


def html(new_contract):
    pd.set_option('colheader_justify', 'center')  # FOR TABLE <th>
    html_string = '''
    <html>
      <head><title>HTML Pandas Dataframe with CSS</title></head>
      <link rel="stylesheet" type="text/css" href="df_style.css"/>
      <body>
        {table}
      </body>
    </html>.
    '''
    # OUTPUT AN HTML FILE
    with open('new_contract.html', 'w') as f:
        f.write(html_string.format(table=new_contract.to_html(classes='mystyle')))
    import os
    print(os.getcwd())


if __name__ == '__main__':
    get_new_contract()


