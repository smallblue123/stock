import requests
import pandas as pd
from datetime import datetime, timedelta

def fetch_stock_daily(stock_no, days=5):
    today = datetime.today()
    result = []
    for i in range(days):
        date = (today - timedelta(days=i)).strftime('%Y%m%d')
        url = f'https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={date}&stockNo={stock_no}'
        resp = requests.get(url)
        if resp.status_code == 200:
            data = resp.json()
            if 'data' in data:
                for row in data['data']:
                    result.append({
                        'date': row[0],
                        'open': row[3],
                        'high': row[4],
                        'low': row[5],
                        'close': row[6],
                        'volume': row[1]
                    })
    df = pd.DataFrame(result)
    return df

def fetch_stock_basic_info():
    url = 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=2'
    # 直接用 pandas 讀取網頁表格
    df = pd.read_html(url, header=0)[0]
    # 清理資料，只保留股票代號、名稱、產業類別
    df = df[df['有價證券代號及名稱'].notnull()]
    df = df[df['有價證券代號及名稱'].str.contains('　')]  # 全形空格分隔代號與名稱
    df[['stock_no', 'name']] = df['有價證券代號及名稱'].str.split('　', expand=True)
    df = df[['stock_no', 'name', '產業別']]
    df = df.rename(columns={'產業別': 'industry'})
    df = df.reset_index(drop=True)
    return df

# 測試
if __name__ == "__main__":
    df_daily = fetch_stock_daily('2330', days=5)
    print(df_daily)
    df_basic = fetch_stock_basic_info()
    print(df_basic.head())