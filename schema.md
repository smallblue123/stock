# 股票分析資料庫規格書 (schema.md)

## 資料庫資訊
- **名稱**: `stock_db`
- **編碼**: `utf8mb4_0900_ai_ci`
- **金額欄位**: `DECIMAL(20,2)`
- **價格欄位**: `DECIMAL(8,2)`

## 表格清單

### 1. 股票清單 (stocks)
| 欄位 | 類型 | 說明 |
| :--- | :--- | :--- |
| **id** (PK) | INT | 內部流水號 |
| **code** | VARCHAR(10) | 股票代號 (Unique) |
| name | VARCHAR(30) | 股票名稱 |
| market | VARCHAR(10) | 上市/上櫃 |
| industry | VARCHAR(30) | 產業別 |
| **category** | VARCHAR(20) | 類別 (股票/ETF) |
| list_date | DATE | 上市日期 |

### 2. 日 K 線 (daily_prices)
| 欄位 | 類型 | 說明 |
| :--- | :--- | :--- |
| **stock_id** (PK) | INT | FK -> stocks.id |
| **date** (PK) | DATE | 交易日 |
| open/high/low/close | DECIMAL(8,2) | 開高低收 |
| adj_close | DECIMAL(8,2) | 還權收盤價 |
| volume | BIGINT | 成交股數 (股) |
| turnover | DECIMAL(20,2) | 成交金額 (元) |

### 3. 三大法人 (institutional_trades)
| 欄位 | 類型 | 說明 |
| :--- | :--- | :--- |
| **stock_id** (PK) | INT | FK -> stocks.id |
| **date** (PK) | DATE | 交易日 |
| foreign_buy/sell/net | INT | 外資買/賣/超 (股) |
| trust_buy/sell/net | INT | 投信買/賣/超 (股) |
| dealer_buy/sell/net | INT | 自營商買/賣/超 (股) |
| **total_net** | INT | 三大法人合計買賣超 (自動計算) |

### 4. 融資融券 (margin_trading)
| 欄位 | 類型 | 說明 |
| :--- | :--- | :--- |
| **stock_id** (PK) | INT | FK -> stocks.id |
| **date** (PK) | DATE | 交易日 |
| margin_buy/sell/balance | INT | 融資買/賣/餘額 (張) |
| short_buy/sell/balance | INT | 融券買/賣/餘額 (張) |

### 5. 當沖 (day_trading)
| 欄位 | 類型 | 說明 |
| :--- | :--- | :--- |
| **stock_id** (PK) | INT | FK -> stocks.id |
| **date** (PK) | DATE | 交易日 |
| day_trade_volume | INT | 當沖成交量 (股) |
| day_trade_amount | DECIMAL(20,2) | 當沖成交金額 (元) |
| day_ratio | DECIMAL(5,2) | 當沖佔比 (%) |

### 6. 月營收 (monthly_revenue)
| 欄位 | 類型 | 說明 |
| :--- | :--- | :--- |
| **stock_id** (PK) | INT | FK -> stocks.id |
| **year** (PK) | INT | 西元年 |
| **month** (PK) | INT | 月份 |
| revenue | DECIMAL(20,2) | 當月營收 (千元) |
| mom_growth | DECIMAL(8,2) | 月增率 (%) |
| yoy_growth | DECIMAL(8,2) | 年增率 (%) |