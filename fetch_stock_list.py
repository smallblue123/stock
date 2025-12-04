import requests
import pandas as pd
import numpy as np
import mysql.connector
from io import StringIO
from datetime import datetime

# ==========================================
# 資料庫設定
# ==========================================
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "81997337rich",
    "database": "stock_db"
}

# ==========================================
# 1. Fetcher 層 (負責抓取與清洗)
# ==========================================
def get_twse_tpex_list():
    """
    [Fetcher] 從證交所與櫃買中心抓取股票清單
    回傳: 清洗完成的 DataFrame
    """
    print("開始下載證交所與櫃買中心資料...")

    # 1. 下載 上市 (TWSE, Mode=2)
    try:
        url_twse = 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=2'
        resp_twse = requests.get(url_twse)
        resp_twse.encoding = 'big5hkscs' # 證交所使用 Big5 編碼
        df_twse = pd.read_html(StringIO(resp_twse.text), header=0)[0]
    except Exception as e:
        print(f"下載上市資料失敗: {e}")
        return pd.DataFrame()

    # 2. 下載 上櫃 (TPEX, Mode=4)
    try:
        url_tpex = 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=4'
        resp_tpex = requests.get(url_tpex)
        resp_tpex.encoding = 'big5hkscs'
        df_tpex = pd.read_html(StringIO(resp_tpex.text), header=0)[0]
    except Exception as e:
        print(f"下載上櫃資料失敗: {e}")
        return pd.DataFrame()

    # 3. 合併上市與上櫃資料
    df = pd.concat([df_twse, df_tpex], ignore_index=True)

    # 4. 處理「股票類別」
    # 說明：ISIN 網頁的分類標題是寫在某一整列 (例如整列都是 "股票")
    # 抓出這些標題，填入新的 category 欄位
    category_col = pd.Series(np.nan, index=df.index, dtype='object')

    for idx, row in df.iterrows():
        non_nan = row.dropna()
        # 如果該列有值，且所有欄位的值都相同 -> 代表它是「標題列」
        if len(non_nan) > 0 and (non_nan == non_nan.iloc[0]).all():
            category_col[idx] = non_nan.iloc[0]

    # 將抓到的標題 (如 "股票", "ETF") 往下填滿 (Forward Fill)
    df['category'] = category_col.ffill()

    # 5. 資料初步清洗
    # 5-1. 移除標題列本身
    # (如果第一欄的內容 == 分類名稱，代表這行是標題，不要保留)
    df = df[df.iloc[:, 0] != df['category']]
    
    # 5-2. 排除格式不符的列
    # 正常的股票代號名稱格式為: "2330　台積電" (中間包含全形空白)
    # 這裡只保留符合此格式的列
    df = df[df.iloc[:, 0].str.contains('　', na=False)].copy()

    # 6. 拆分代號與名稱
    # 來源: 第 0 欄 ('有價證券代號及名稱')
    # n=1 代表只切第一刀，expand=True 代表拆分成兩個獨立欄位
    split_data = df.iloc[:, 0].str.split('　', n=1, expand=True)

    # 7. 組裝最終資料表 (Final DataFrame)
    # 直接從對應位置 (iloc) 抓取資料，建立乾淨的表格
    # -----------------------------------------------------------
    # 原始 HTML 表格欄位索引 (Index) 對照說明:
    # [0]: 有價證券代號及名稱 (已拆分為 split_data)
    # [1]: 國際證券辨識號碼   (ISIN Code，不使用)
    # [2]: 上市日             (list_date)
    # [3]: 市場別             (market)
    # [4]: 產業別             (industry)
    # [5]: CFICode            (不使用)
    # [6]: 備註               (不使用)
    # -----------------------------------------------------------
    final_df = pd.DataFrame({
        'code':      split_data[0].str.strip(),      # 代號
        'name':      split_data[1].str.strip(),      # 名稱
        'market':    df.iloc[:, 3].str.strip(),      # 第 3 欄: 市場別
        'industry':  df.iloc[:, 4].str.strip(),      # 第 4 欄: 產業別
        'category':  df['category'],                 # 計算出的分類
        'list_date': df.iloc[:, 2]                   # 第 2 欄: 上市日
    })

    # 8. 過濾黑名單 (Blacklist Filtering)
    exclude_categories = ['上市認購(售)權證', '上櫃認購(售)權證']
    
    # 使用 "~" (NOT) 加上 .isin() 來保留「不在黑名單內」的資料
    final_df = final_df[~final_df['category'].isin(exclude_categories)]

    # 9. 格式化日期與處理空值
    # 日期轉格式: YYYY/MM/DD -> YYYY-MM-DD
    final_df['list_date'] = pd.to_datetime(final_df['list_date'], errors='coerce').dt.strftime('%Y-%m-%d')
    
    # 產業別如果是 NaN，轉為空字串
    final_df['industry'] = final_df['industry'].fillna('')

    print(f"資料清洗完成，共取得 {len(final_df)} 筆資料 (已過濾權證)")
    return final_df


# ==========================================
# 2. Saver 層 (負責寫入資料庫)
# ==========================================
def save_stock_list_to_db(df):
    """
    [Saver] 將股票清單寫入 `stocks` 表格
    使用 UPSERT (ON DUPLICATE KEY UPDATE)
    """
    if df.empty:
        print("DataFrame 為空，跳過寫入。")
        return

    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        sql = """
        INSERT INTO stocks (code, name, market, industry, category, list_date)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            market = VALUES(market),
            industry = VALUES(industry),
            category = VALUES(category),
            list_date = VALUES(list_date);
        """
        
        # 將 DataFrame 轉為 Tuple 列表 
        # replace({np.nan: None}) 將 Pandas 的 NaN 轉為 SQL 的 NULL
        data = [tuple(x) for x in df.replace({np.nan: None}).to_numpy()]
        
        # 執行批次寫入
        cursor.executemany(sql, data)
        conn.commit()
        print(f"成功寫入/更新 {cursor.rowcount} 筆資料到 stock_db.stocks")

    except mysql.connector.Error as err:
        print(f"資料庫錯誤: {err}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# ==========================================
# 3. Task 層 (任務指揮官)
# ==========================================
def update_stock_list_task():
    """
    [Task] 執行更新股票清單的完整流程
    """
    print("\n" + "="*40)
    print("任務開始: 更新股票清單")
    print("="*40)
    
    # 步驟 1: 抓取
    df = get_twse_tpex_list()
    
    # 步驟 2: 存檔
    if not df.empty:
        save_stock_list_to_db(df)
        
    print("="*40 + "\n")


# ==========================================
# 主程式入口
# ==========================================
if __name__ == "__main__":
    update_stock_list_task()