import yfinance as yf
import pandas as pd
import numpy as np
import mysql.connector
from datetime import datetime, timedelta

# ==========================================
# 資料庫設定
# ==========================================
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "81997337rich",  # 請記得修改密碼
    "database": "stock_db"
}

# ==========================================
# 1. Fetcher: 負責去 Yahoo 抓資料
# ==========================================
def fetch_stock_history(stock_code, market_type, period="1mo"):
    """
    從 Yahoo Finance 抓取個股歷史資料
    stock_code: 股票代號 (如 2330)
    market_type: 市場別 (上市/上櫃)
    period: 抓取長度 (1d, 5d, 1mo, 3mo, 1y, max)
    """
    # 1. 轉換代號為 Yahoo 格式
    # 上市 -> .TW, 上櫃 -> .TWO
    suffix = ".TW" if market_type == "上市" else ".TWO"
    ticker = f"{stock_code}{suffix}"
    
    print(f"正在抓取 {ticker} 最近 {period} 的資料...")
    
    try:
        # 下載資料 (auto_adjust=False 代表不自動修正，我们要保留原始 Close 和 Adj Close)
        df = yf.download(ticker, period=period, auto_adjust=False, progress=False)
        
        if df.empty:
            print(f"警告: {ticker} 抓取不到資料")
            return pd.DataFrame()

        # 2. 整理欄位 (Yahoo 回傳的是 MultiIndex，需要攤平)
        # 如果欄位是 MultiIndex (例如 ('Close', '2330.TW'))，取第一層
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df = df.reset_index() # 把 Date 變成欄位
        
        # 3. 重新命名以對應資料庫
        # Yahoo欄位: Date, Open, High, Low, Close, Adj Close, Volume
        df = df.rename(columns={
            'Date': 'date',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Adj Close': 'adj_close',
            'Volume': 'volume'
        })
        
        # 4. 補算資料庫需要的額外欄位
        # (1) 漲跌價與漲跌幅 (跟昨天比)
        df['change_price'] = df['close'].diff()
        df['change_pct'] = df['close'].pct_change() * 100
        
        # (2) 成交金額 (Turnover)
        # Yahoo 不提供精確成交金額，這裡先用「收盤價 * 成交量」做估算，僅供測試
        # 正式環境應該要去證交所抓精確值
        df['turnover'] = df['close'] * df['volume']

        # 5. 處理 NaN (第一筆資料的漲跌會是 NaN)
        df = df.fillna(0)
        
        # 6. 日期轉字串
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        print(f"成功取得 {len(df)} 筆 K 線資料")
        return df

    except Exception as e:
        print(f"抓取失敗: {e}")
        return pd.DataFrame()

# ==========================================
# 2. Saver: 負責寫入資料庫
# ==========================================
def save_daily_prices(stock_id, df):
    """
    將 K 線資料寫入 daily_prices 表
    """
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        sql = """
        INSERT INTO daily_prices (
            stock_id, date, open, high, low, close, adj_close, 
            volume, turnover, change_price, change_pct
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open = VALUES(open),
            high = VALUES(high),
            low = VALUES(low),
            close = VALUES(close),
            adj_close = VALUES(adj_close),
            volume = VALUES(volume),
            turnover = VALUES(turnover),
            change_price = VALUES(change_price),
            change_pct = VALUES(change_pct);
        """
        
        # 準備資料: 每一列都要加上 stock_id
        data_to_insert = []
        for _, row in df.iterrows():
            data_to_insert.append((
                stock_id,
                row['date'],
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row['adj_close'],
                row['volume'],
                row['turnover'],
                row['change_price'],
                row['change_pct']
            ))
            
        cursor.executemany(sql, data_to_insert)
        conn.commit()
        print(f"資料庫寫入成功！共更新 {cursor.rowcount} 筆記錄 (ID: {stock_id})")

    except mysql.connector.Error as err:
        print(f"資料庫錯誤: {err}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# ==========================================
# 3. Task: 測試任務 (只跑台積電)
# ==========================================
def run_test_task():
    print("=== 開始測試抓取日 K 線 (目標: 2330 台積電) ===")
    
    # 步驟 1: 先從資料庫查出台積電的 ID 和 市場別
    # (因為寫入 daily_prices 需要 internal id，不能只用 code)
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    target_code = '2330' # 測試對象
    
    cursor.execute("SELECT id, code, market FROM stocks WHERE code = %s", (target_code,))
    stock_row = cursor.fetchone()
    conn.close()
    
    if not stock_row:
        print(f"錯誤: 資料庫找不到股票 {target_code}，請先執行 update_stock_list.py")
        return

    stock_id, stock_code, stock_market = stock_row
    print(f"資料庫對應: {stock_code} {stock_market} -> ID: {stock_id}")

    # 步驟 2: 抓取最近 1 個月的資料
    df_prices = fetch_stock_history(stock_code, stock_market, period="1mo")
    
    # 步驟 3: 顯示部分數據檢查
    if not df_prices.empty:
        print("\n抓取數據預覽:")
        print(df_prices[['date', 'close', 'adj_close', 'volume']].tail(3))
        
        # 步驟 4: 存入資料庫
        save_daily_prices(stock_id, df_prices)
    else:
        print("未取得資料，跳過存檔")

    print("=== 測試結束 ===")

if __name__ == "__main__":
    run_test_task()