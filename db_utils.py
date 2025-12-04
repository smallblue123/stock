# db_utils.py
import mysql.connector
import pandas as pd
import numpy as np
from config import DB_CONFIG

def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)

def get_all_stock_tickers():
    """
    回傳 DataFrame，包含: [stock_id, code, market, ticker]
    ticker 是 Yahoo 格式 (2330.TW)
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 直接在 SQL 組出 Yahoo Ticker
        sql = """
        SELECT id as stock_id, code, market,
               CASE 
                   WHEN market = '上市' THEN CONCAT(code, '.TW')
                   WHEN market = '上櫃' THEN CONCAT(code, '.TWO')
                   ELSE CONCAT(code, '.TW')
               END as ticker
        FROM stocks
        """
        cursor.execute(sql)

        # 1. 抓取資料 (List of Tuples)
        data = cursor.fetchall()
        # 2. 抓取欄位名稱
        columns = [desc[0] for desc in cursor.description]
        # 3. 手動轉成 DataFrame
        df = pd.DataFrame(data, columns=columns)
        return df
    finally:
        if conn: conn.close()

def save_daily_prices(df):
    """
    批次寫入 K 線資料
    """
    if df.empty: return

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        sql = """
        INSERT INTO daily_prices (
            stock_id, date, open, high, low, close, adj_close, 
            volume, turnover, change_price, change_pct
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open = VALUES(open), high = VALUES(high), low = VALUES(low),
            close = VALUES(close), adj_close = VALUES(adj_close),
            volume = VALUES(volume), turnover = VALUES(turnover),
            change_price = VALUES(change_price), change_pct = VALUES(change_pct);
        """
        
        # 確保欄位順序與補值
        cols = ['stock_id', 'date', 'open', 'high', 'low', 'close', 
                'adj_close', 'volume', 'turnover', 'change_price', 'change_pct']
        
        # 處理 NaN -> None
        data = [tuple(x) for x in df[cols].replace({np.nan: None}).to_numpy()]
        
        cursor.executemany(sql, data)
        conn.commit()
        print(f"[DB] 成功寫入 {len(data)} 筆資料")
        
    except Exception as e:
        print(f"[DB] 寫入失敗: {e}")
    finally:
        if conn: conn.close()