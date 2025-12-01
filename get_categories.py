import requests
import pandas as pd
import numpy as np
from io import StringIO

def check_categories():
    print("正在抓取上市與上櫃資料中...")

    # 1. 下載上市資料
    try:
        url_twse = 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=2'
        resp_twse = requests.get(url_twse)
        resp_twse.encoding = 'big5hkscs'
        df_twse = pd.read_html(StringIO(resp_twse.text), header=0)[0]
    except Exception as e:
        print(f"上市資料抓取失敗: {e}")
        return

    # 2. 下載上櫃資料
    try:
        url_tpex = 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=4'
        resp_tpex = requests.get(url_tpex)
        resp_tpex.encoding = 'big5hkscs'
        df_tpex = pd.read_html(StringIO(resp_tpex.text), header=0)[0]
    except Exception as e:
        print(f"上櫃資料抓取失敗: {e}")
        return

    # 3. 合併
    df = pd.concat([df_twse, df_tpex], ignore_index=True)

    # 4. 提取分類邏輯 (整列數值相同的為標題)
    category_col = pd.Series(np.nan, index=df.index, dtype='object')
    
    for idx, row in df.iterrows():
        non_nan = row.dropna()
        if len(non_nan) > 0 and (non_nan == non_nan.iloc[0]).all():
            category_col[idx] = non_nan.iloc[0]

    # 填滿分類
    df['category'] = category_col.ffill()

    # 5. 移除標題列本身 (只保留該分類底下的商品)
    # 這樣統計出來的數量才是「商品數量」，而不是包含標題列
    df_items = df[df.iloc[:, 0] != df['category']]

    # 6. 統計並列印結果
    print("-" * 30)
    print("【分類統計結果】")
    print("-" * 30)
    
    # 計算每個分類的數量
    counts = df_items['category'].value_counts()
    
    for cat, count in counts.items():
        print(f"{cat}: {count} 筆")

    print("-" * 30)
    print(f"總共有 {len(counts)} 種分類")

if __name__ == "__main__":
    check_categories()