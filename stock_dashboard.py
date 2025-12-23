import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from db_utils import get_db_connection

# streamlit run app.py

# 1. 設定網頁標題
st.set_page_config(page_title="台股 K 線分析", layout="wide")
st.title("台股歷史股價")

# 2. 側邊欄：選擇股票
st.sidebar.header("篩選條件")

@st.cache_data(ttl=3600) # 快取機制：1小時內不用重新查資料庫
def load_stock_list():
    conn = get_db_connection()
    # 撈出股票清單
    sql = "SELECT id, code, name FROM stocks ORDER BY code"
    df = pd.read_sql(sql, conn)
    conn.close()
    # 組合顯示名稱，例如 "2330 台積電"
    df['label'] = df['code'] + " " + df['name']
    return df

df_stocks = load_stock_list()
selected_stock_label = st.sidebar.selectbox("選擇股票", df_stocks['label'])

# 找出使用者選了哪一檔 (取出 stock_id)
selected_stock_row = df_stocks[df_stocks['label'] == selected_stock_label].iloc[0]
stock_id = selected_stock_row['id']
stock_code = selected_stock_row['code']
stock_name = selected_stock_row['name']

# 3. 讀取該股票的 K 線資料
@st.cache_data(ttl=60) # K線資料快取 60 秒
def load_kline_data(stock_id, days=365):
    conn = get_db_connection()
    # 抓最近 N 天的資料
    sql = f"""
        SELECT date, open, high, low, close, volume 
        FROM daily_prices 
        WHERE stock_id = {stock_id} 
        ORDER BY date DESC 
        LIMIT {days}
    """
    df = pd.read_sql(sql, conn)
    conn.close()
    
    # 整理資料格式 (mplfinance 需要 Date Index)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date').sort_index()
    return df

# 使用者選擇天數
days_to_show = st.sidebar.slider("顯示天數", 30, 3650, 180)

# 載入資料
df_kline = load_kline_data(stock_id, days=days_to_show)

if not df_kline.empty:
    st.subheader(f"{stock_code} {stock_name} - 日 K 線圖")
    df_kline.index = df_kline.index.strftime('%Y-%m-%d')
    
    # 1. 建立子圖表 (上層是 K 線，下層是成交量)
    # shared_xaxes=True 代表上下圖表共用 X 軸 (日期)，縮放時會連動
    fig = make_subplots(
        rows=2, cols=1, 
        shared_xaxes=True, 
        vertical_spacing=0.03, 
        subplot_titles=(f'{stock_code} {stock_name}', '成交量'),
        row_heights=[0.7, 0.3] # K線佔 70% 高度, 成交量佔 30%
    )

    # 2. 加入 K 線圖 (Candlestick)
    fig.add_trace(go.Candlestick(
        x=df_kline.index,
        open=df_kline['open'],
        high=df_kline['high'],
        low=df_kline['low'],
        close=df_kline['close'],
        name='K線'
    ), row=1, col=1)

    # 3. 加入成交量圖 (Bar)
    # 這裡做一個小技巧：根據漲跌設定顏色 (收盤 >= 開盤 用紅色，否則綠色)
    colors = ['red' if row['close'] >= row['open'] else 'green' for index, row in df_kline.iterrows()]
    
    fig.add_trace(go.Bar(
        x=df_kline.index,
        y=df_kline['volume'],
        marker_color=colors,
        name='成交量'
    ), row=2, col=1)

    # 4. 設定圖表樣式 (移除下方預設的 Range Slider，因為有點佔空間)
    fig.update_layout(
        xaxis_rangeslider_visible=False,
        height=600, # 設定圖表高度
        hovermode="x unified" # 【關鍵】開啟十字準線與統一資訊框
    )
    
    fig.update_xaxes(
        type='category',      
        tickmode='auto',      # 讓 Plotly 自動決定顯示幾個日期標籤，避免太擠
        nticks=20,            # 限制 X 軸大約顯示 20 個標籤
        hoverformat=""        
    )

    # 設定 Y 軸格式 (讓成交量不要顯示科學記號，改用一般數字)
    fig.update_yaxes(title_text="股價", row=1, col=1)
    fig.update_yaxes(title_text="成交量", fixedrange=True, row=2, col=1)

    # 5. 在 Streamlit 顯示互動圖表
    st.plotly_chart(fig, use_container_width=True)

    # 顯示原始數據表格 (可折疊)
    with st.expander("查看詳細數據"):
        st.dataframe(df_kline.sort_index(ascending=False))
else:
    st.warning("此股票目前沒有 K 線資料，請先執行爬蟲程式。")