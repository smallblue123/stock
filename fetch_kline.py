import yfinance as yf
import pandas as pd
import time
import numpy as np
import warnings
from datetime import datetime, timedelta

# 匯入我們之前寫好的資料庫工具
# 請確保 db_utils.py 和 config.py 都在同一個目錄下
from db_utils import get_all_stock_tickers, save_daily_prices

# 忽略 Pandas 的 FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

# ==========================================
# 1. 核心資料處理邏輯 (清洗工廠)
# ==========================================
def restructure_yfinance_data(df_wide, ticker_map_df):
    """
    將 Yahoo 下載的寬表格 (Wide Format) 轉換為 資料庫需要的長表格 (Long Format)
    並計算漲跌幅、估算成交金額
    """
    if df_wide.empty:
        return pd.DataFrame()
     
    # --- 步驟 A: 寬轉長 (Stacking) ---
    # 判斷是否為多檔股票 (MultiIndex)
    if isinstance(df_wide.columns, pd.MultiIndex):
        # 將 Ticker 層級堆疊到 Index，變成長表格
        df_long = df_wide.stack(level=1).reset_index().rename(columns={'Ticker': 'ticker'})
    
    # fixme: 處理單檔股票時的特殊情況
    else:
        # 單檔股票的情況
        df_long = df_wide.reset_index()
        # 單檔時 yfinance 不會給 ticker 欄位
        if 'ticker' not in df_long.columns and not ticker_map_df.empty:
             pass
    
    # --- 步驟 B: 欄位標準化 ---
    # 重新命名以對應資料庫欄位
    df_long = df_long.rename(columns={
        'Date': 'date', 
        'Open': 'open', 
        'High': 'high', 
        'Low': 'low', 
        'Close': 'close', 
        'Adj Close': 'adj_close', 
        'Volume': 'volume'
    })

    # 刪除因為停牌導致收盤價為空的資料
    df_long = df_long.dropna(subset=['close'])
    
    # --- 步驟 C: 數值計算 ---
    # 1. 估算成交金額 (Turnover)
    df_long['turnover'] = df_long['close'] * df_long['volume']
    
    # 2. 計算漲跌 (Change)
    # 利用 groupby 確保是針對「同一檔股票」的前後日計算
    df_long['change_price'] = df_long.groupby('ticker')['close'].diff()
    df_long['change_pct'] = df_long.groupby('ticker')['close'].pct_change() * 100
    
    # 3. 填補計算產生的 NaN (每檔股票的第一筆資料無法跟昨天比，會是 NaN)
    df_long[['change_price', 'change_pct']] = df_long[['change_price', 'change_pct']].fillna(0)
    
    # --- 步驟 D: 合併 Stock ID ---
    # 將 Yahoo Ticker (2330.TW) 轉換為資料庫的 stock_id (1)
    df_final = df_long.merge(ticker_map_df[['ticker', 'stock_id']], on='ticker', how='inner')
    
    # --- 步驟 E: 日期格式化 ---
    df_final['date'] = df_final['date'].dt.strftime('%Y-%m-%d')
    
    return df_final


# ==========================================
# 2. 批次處理單元 (執行工廠)
# ==========================================
def process_batch(tickers, ticker_map_df, start=None, end=None, period=None):
    """
    通用處理函式：下載 -> 清洗 -> 存檔
    """
    try:
        print(f"下載 {len(tickers)} 檔股票資料... (Period: {period if period else f'{start}~{end}'})")
        
        # 呼叫 yfinance 下載
        # group_by='ticker' 是為了讓資料結構固定，方便 stack()
        # threads=True 開啟多執行緒下載加速
        df_wide = yf.download(
            tickers, 
            start=start, end=end, period=period,
            interval="1d", 
            auto_adjust=False, 
            progress=False, 
            threads=True,
            timeout=20
        )

        # 清洗數據
        df_final = restructure_yfinance_data(df_wide, ticker_map_df)
        
        # 寫入資料庫
        if not df_final.empty:
            save_daily_prices(df_final)
        else:
            print("下載成功但無有效資料 (可能全部停牌或休市)")
            
    except Exception as e:
        print(f"批次處理失敗: {e}")


# ==========================================
# 任務 1: 每日更新 (Daily Update)
# ==========================================
def run_daily_update_task():
    """
    每天收盤後執行。
    策略：抓取最近 '4天' 的資料。
    理由：讓 diff() 算出今天的漲跌幅 (今天 - 昨天)。
    """
    print("\n[任務] 開始每日更新 (抓取最近 4 天以計算漲跌)...")
    
    # 1. 取得所有股票名單
    df_map = get_all_stock_tickers()
    if df_map.empty:
        print("無股票列表，請先執行 update_stock_list.py")
        return

    all_tickers = df_map['ticker'].tolist()
    
    # 2. 執行批次下載
    # 每日更新資料量小，可以一次處理較多檔 (例如 500)
    CHUNK_SIZE = 100
    
    for i in range(0, len(all_tickers), CHUNK_SIZE):
        batch = all_tickers[i:i+CHUNK_SIZE]
        print(f"--- 進度: {i}/{len(all_tickers)} ---")
        
        # 關鍵：這裡用 period='5d'，讓程式能算出漲跌
        process_batch(batch, df_map, period="4d")
        
    print("每日更新完成！(資料庫已自動處理重複資料)")


# ==========================================
# 任務 2: 歷史回補 (Full Backfill)
# ==========================================
def run_full_backfill_task():
    """
    第一次建置時執行。
    策略：分小批次，抓 10 年，中間休息避免被封鎖。
    """
    print("\n[任務] 開始歷史資料回補 (這會花費較長時間)...")
    
    df_map = get_all_stock_tickers()
    all_tickers = df_map['ticker'].tolist()
    
    # 策略設定
    CHUNK_SIZE = 10   # 幾檔一批
    SLEEP_SECONDS = 2 # 每批休息 2 秒
    
    for i in range(0, len(all_tickers), CHUNK_SIZE):
        batch = all_tickers[i:i+CHUNK_SIZE]
        print(f"--- 進度: {i}/{len(all_tickers)} (休息 {SLEEP_SECONDS}s) ---")
        
        # 抓取最長歷史 (或指定 "10y")
        process_batch(batch, df_map, period="max") 
        
        # 禮貌性暫停
        time.sleep(SLEEP_SECONDS)
        
    print("歷史資料回補完成！")


# ==========================================
# 主程式入口
# ==========================================
if __name__ == "__main__":
    # --- 使用說明 ---
    # 1. 第一次建立資料庫：
    run_full_backfill_task()
    
    # 2. 每天收盤更新資料
    # run_daily_update_task()