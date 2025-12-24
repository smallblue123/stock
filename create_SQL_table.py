import mysql.connector
from mysql.connector import Error
import copy
from config import DB_CONFIG


def create_safe_database():
    conn = None
    cursor = None
    try:
        server_config = copy.copy(DB_CONFIG)
        if "database" in server_config:
            DB_NAME = DB_CONFIG.get("database", "default_db_name")
            # 在連線到 Server 時必須移除 database 鍵，否則會嘗試連線到一個可能不存在的 DB
            del server_config["database"]


        # ---------------------------------------------------------
        # 1. 連接 MySQL Server 並建立資料庫
        # ---------------------------------------------------------
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 建立資料庫
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME} CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;")
        print(f"資料庫 {DB_NAME} 準備就緒")
        
        cursor.close()
        conn.close()

        # ---------------------------------------------------------
        # 2. 連接指定資料庫開始建表
        # ---------------------------------------------------------
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 啟用外鍵檢查
        cursor.execute("SET FOREIGN_KEY_CHECKS=1;")

        # =========================================================
        # 表格 1: 股票基本資料 (Stocks)
        # =========================================================
        print("正在建立表格: stocks")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
            id INT PRIMARY KEY AUTO_INCREMENT COMMENT '內部流水號 (主鍵)',
            code VARCHAR(10) UNIQUE NOT NULL COMMENT '股票代號 (如 2330)',
            name VARCHAR(30) NOT NULL COMMENT '股票名稱 (如 台積電)',
            market VARCHAR(5) NOT NULL COMMENT '市場別 (上市/上櫃)',
            industry VARCHAR(30) COMMENT '產業別 (如 半導體業)',
            category VARCHAR(20) COMMENT '證券類別 (股票/ETF)',
            list_date DATE COMMENT '上市日期'
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci COMMENT='股票清單基本資料';
        """)

        # =========================================================
        # 表格 2: 日 K 線資料 (Daily Prices)
        # =========================================================
        print("正在建立表格: daily_prices")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_prices (
            stock_id INT NOT NULL COMMENT '對應 stocks.id',
            date DATE NOT NULL COMMENT '交易日期',
            open DECIMAL(8,2) COMMENT '開盤價',
            high DECIMAL(8,2) COMMENT '最高價',
            low DECIMAL(8,2) COMMENT '最低價',
            close DECIMAL(8,2) COMMENT '收盤價',
            adj_close DECIMAL(9,2) COMMENT '還權收盤價 (除權息修正)',
            volume BIGINT COMMENT '成交股數 (單位:股)',
            turnover DECIMAL(20,2) COMMENT '成交金額 (單位:元)',
            change_price DECIMAL(8,2) COMMENT '漲跌價',
            change_pct DECIMAL(10,2) COMMENT '漲跌幅 (單位:%)',
            
            PRIMARY KEY (stock_id, date),
            FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci COMMENT='日K線歷史股價';
        """)

# =========================================================
        # 表格 3: 三大法人買賣超 (加入 total_net 生成欄位)
        # =========================================================
        print("正在建立表格: institutional_trades")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS institutional_trades (
            stock_id INT NOT NULL COMMENT '對應 stocks.id',
            date DATE NOT NULL COMMENT '交易日期',
            
            foreign_buy INT DEFAULT 0 COMMENT '外資買進 (單位:股)',
            foreign_sell INT DEFAULT 0 COMMENT '外資賣出 (單位:股)',
            foreign_net INT DEFAULT 0 COMMENT '外資買賣超 (單位:股)',
            
            trust_buy INT DEFAULT 0 COMMENT '投信買進 (單位:股)',
            trust_sell INT DEFAULT 0 COMMENT '投信賣出 (單位:股)',
            trust_net INT DEFAULT 0 COMMENT '投信買賣超 (單位:股)',
            
            dealer_buy INT DEFAULT 0 COMMENT '自營商買進 (單位:股)',
            dealer_sell INT DEFAULT 0 COMMENT '自營商賣出 (單位:股)',
            dealer_net INT DEFAULT 0 COMMENT '自營商買賣超 (單位:股)',
            
            -- 合計買賣超 (自動計算欄位)
            total_net INT GENERATED ALWAYS AS (foreign_net + trust_net + dealer_net) STORED COMMENT '三大法人合計買賣超 (單位:股)',

            PRIMARY KEY (stock_id, date),
            FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
                       
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci COMMENT='三大法人買賣超明細';
        """)

        # cursor.execute("CREATE INDEX idx_total_net ON institutional_trades(total_net);")


        # =========================================================
        # 表格 4: 融資融券 (Margin Trading)
        # =========================================================
        print("正在建立表格: margin_trading")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS margin_trading (
            stock_id INT NOT NULL COMMENT '對應 stocks.id',
            date DATE NOT NULL COMMENT '交易日期',
            
            margin_buy INT DEFAULT 0 COMMENT '融資買進 (單位:張)',
            margin_sell INT DEFAULT 0 COMMENT '融資賣出 (單位:張)',
            margin_balance INT DEFAULT 0 COMMENT '融資餘額 (單位:張)',
            
            short_buy INT DEFAULT 0 COMMENT '融券買進/回補 (單位:張)',
            short_sell INT DEFAULT 0 COMMENT '融券賣出/放空 (單位:張)',
            short_balance INT DEFAULT 0 COMMENT '融券餘額 (單位:張)',

            PRIMARY KEY (stock_id, date),
            FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci COMMENT='融資融券餘額與變動';
        """)

        # =========================================================
        # 表格 5: 當沖交易 (Day Trading)
        # =========================================================
        print("正在建立表格: day_trading")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS day_trading (
            stock_id INT NOT NULL COMMENT '對應 stocks.id',
            date DATE NOT NULL COMMENT '交易日期',
            
            day_trade_volume INT COMMENT '當沖成交股數 (單位:股)',
            day_trade_amount DECIMAL(20,2) COMMENT '當沖成交金額 (單位:元)',
            day_ratio DECIMAL(5,2) COMMENT '當沖佔比 (單位:%)',

            PRIMARY KEY (stock_id, date),
            FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci COMMENT='當沖交易統計';
        """)

        # =========================================================
        # 表格 6: 月營收 (Monthly Revenue)
        # =========================================================
        print("正在建立表格: monthly_revenue")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS monthly_revenue (
            stock_id INT NOT NULL COMMENT '對應 stocks.id',
            year INT NOT NULL COMMENT '西元年份',
            month INT NOT NULL COMMENT '月份',
            
            revenue DECIMAL(20,2) COMMENT '當月營收 (單位:千元)',
            mom_growth DECIMAL(8,2) COMMENT '上月比較增減 (單位:%)',
            yoy_growth DECIMAL(8,2) COMMENT '去年同月增減 (單位:%)',
            accumulated_revenue DECIMAL(20,2) COMMENT '累計營收 (單位:千元)',
            accumulated_yoy DECIMAL(8,2) COMMENT '累計年增率 (單位:%)',

            PRIMARY KEY (stock_id, year, month),
            FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci COMMENT='月營收報告';
        """)

        conn.commit()
        print("-" * 30)
        print(f"資料庫 {DB_NAME} 建置完成！")
        print("-" * 30)

    except Error as e:
        print(f"發生錯誤: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            print("資料庫連線已關閉")

if __name__ == "__main__":
    create_safe_database()