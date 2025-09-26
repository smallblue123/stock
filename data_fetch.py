import requests
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO

# 上市股票
# 台灣證券交易所股票日資料
def fetch_twse_stock_daily(stock_no, start_date, end_date=None):
    """
    取得指定股票在指定日期或日期區間的日資料。
    - 若只輸入 start_date，則輸出該月的資料。
    - 日期格式: 'YYYYMMDD'
    """
    result = []

    # 轉成 datetime 物件
    start = datetime.strptime(start_date, "%Y%m%d")
    if end_date:
        end = datetime.strptime(end_date, "%Y%m%d")
    else:
        # 如果沒有給 end_date，則取該月最後一天
        if start.month == 12:
            end = datetime(start.year + 1, 1, 1) - pd.Timedelta(days=1)
        else:
            end = datetime(start.year, start.month + 1, 1) - pd.Timedelta(days=1)

    # 產生每個月的第一天
    months = []
    current = datetime(start.year, start.month, 1)
    while current <= end:
        months.append(current.strftime("%Y%m01"))
        # 下個月
        if current.month == 12:
            current = datetime(current.year + 1, 1, 1)
        else:
            current = datetime(current.year, current.month + 1, 1)
    # 逐月抓資料
    for date in months:
        url = f'https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={date}&stockNo={stock_no}'
        resp = requests.get(url)
        if resp.status_code == 200:
            data = resp.json()
            if 'data' in data:
                for row in data['data']:
                    # 日期格式轉換
                    roc_date = row[0]
                    y, m, d = roc_date.split('/')
                    year = int(y) + 1911
                    row_date = f"{year}-{m}-{d}"
                    row_datetime = datetime.strptime(row_date, "%Y-%m-%d")

                    if start <= row_datetime <= end:
                        result.append({
                            "日期": row[0],
                            "開盤價": row[3],
                            '最高價': row[4],
                            '最低價': row[5],
                            '收盤價': row[6],
                            '成交量': row[1]
                        })
    df = pd.DataFrame(result)
    return df

# 上市股票基本資料
def fetch_twse_stock_basic_info():
    url = 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=2'
    resp = requests.get(url)
    resp.encoding = 'big5'
    # 用 StringIO 包裝 HTML 字串
    df = pd.read_html(StringIO(resp.text), header=0)[0]
    # 清理資料，只保留股票代號、名稱、產業類別
    df = df[df['有價證券代號及名稱'].notnull()]
    df = df[df['有價證券代號及名稱'].str.contains('　')]  # 全形空格分隔代號與名稱
    df[['證券代號', '證券名稱']] = df['有價證券代號及名稱'].str.split('　', expand=True)
    df = df[['證券代號', '證券名稱', '產業別']]
    # df = df.rename(columns={'產業別': 'industry'})
    df = df.reset_index(drop=True)
    return df

# 融資融券餘額
def fetch_twse_margin_trading(stock_no, date):
    """
    取得指定日期融資融券餘額
    date 格式: 'YYYYMMDD'
    """
    url = f'https://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date={date}&selectType=ALL'
    resp = requests.get(url)
    if resp.status_code == 200:
        data = resp.json()
        if 'tables' in data:
            for table in data['tables']:
                for row in table['data']:
                    # 欄位 ["代號","名稱","買進","賣出","現金償還","前日餘額","今日餘額","次一營業日限額",
                    # 上:融資 下:融卷     "買進","賣出","現券償還","前日餘額","今日餘額","次一營業日限額","資券互抵","註記"]
                    if row[0].strip() == stock_no:
                        return {
                            '日期': date,
                            '股票代號': row[0],
                            '股票名稱': row[1],
                            "融資增減": int(row[2].replace(',', ''))-int(row[3].replace(',', '')),
                            '融資餘額': row[6].replace(',', ''),
                            '融券餘額': row[12].replace(',', ''),
                            "融卷增減": int(row[8].replace(',', ''))-int(row[9].replace(',', ''))
                        }
    return None

# 三大法人買賣超
def fetch_twse_institutional_investors(stock_no, date):
    """
    取得指定日期三大法人買賣超
    date 格式: 'YYYYMMDD'
    """
    url = f'https://www.twse.com.tw/fund/T86?response=json&date={date}&selectType=ALL'
    resp = requests.get(url)
    if resp.status_code == 200:
        data = resp.json()
        if 'data' in data:
            for row in data['data']:
                if row[0].strip() == stock_no:
                    return {
                        '日期': date,
                        '外資買賣超': round(int(row[4].replace(',', ''))/1000),
                        '投信買賣超': round(int(row[7].replace(',', ''))/1000),
                        '自營商買賣超': round(int(row[10].replace(',', ''))/1000)
                    }
    return None

# --------------------------------------------------------------------------------

# 上櫃股票
# 台灣櫃買中心股票日資料
def fetch_otc_stock_daily(stock_no, start_date, end_date=None):
    """
    取得指定上櫃股票在指定日期或日期區間的日資料。
    - 若只輸入 start_date，則輸出該日的資料。
    - 日期格式: 'YYYYMMDD'
    """
    result = []
    # 轉成 datetime 物件
    start = datetime.strptime(start_date, "%Y%m%d")
    if end_date:
        end = datetime.strptime(end_date, "%Y%m%d")
    else:
        end = start
        
    # 逐日抓資料（櫃買API只能抓單日）
    current = start
    while current <= end:
        date_str = current.strftime("%Y%m%d")
        url = f'https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?d={date_str}&s={stock_no}&json=1'
        resp = requests.get(url)
        if resp.status_code == 200:
            data = resp.json()
            print(data)
            # 櫃買API回傳欄位通常是'aaData'
            if 'aaData' in data:
                for row in data['aaData']:
                    # row: [日期, 成交股數, 成交金額, 開盤價, 最高價, 最低價, 收盤價, 漲跌, 成交筆數]
                    result.append({
                        "日期": row[0],
                        "開盤價": row[3],
                        "最高價": row[4],
                        "最低價": row[5],
                        "收盤價": row[6],
                        "成交量": row[1]
                    })
        current += timedelta(days=1)
    df = pd.DataFrame(result)
    return df

# 測試
if __name__ == "__main__":
    # df = fetch_twse_stock_daily('2330', '20250101', '20250328')
    # print(df)
    df_basic = fetch_twse_stock_basic_info()
    print(df_basic.head())
    # margin_data = fetch_twse_margin_trading('0050', '20250331')
    # print(margin_data)
    # institutional_data = fetch_twse_institutional_investors('0050', '20250924')
    # print(institutional_data)
    # df = fetch_otc_stock_daily('006201', '20250101', '20250201')
    # print(df)
