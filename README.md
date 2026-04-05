# 市场数据自动采集系统

自动获取A股ETF、海外指数、黄金、豆粕期货等金融数据，每天定时更新并保存到Git仓库。

## 📊 监控标的

### A股ETF（新浪财经）
- 科创50ETF (588000)
- 创业板50ETF (159949)
- A50ETF (159593)
- 中证2000ETF (563300)

### 海外指数（Yahoo Finance）
- 恒生指数 (^HSI)
- 纳斯达克 (^IXIC)
- 标普500 (^GSPC)
- 印度 (^BSESN)
- 德国DAX (^GDAXI)

### 商品
- 黄金 (GC=F)
- 原油 (CL=F)
- 油气 (^SPSIOP)
- 豆粕期货 (需要天勤量化账号)

## 🚀 使用方法

### 查看数据

1. 在仓库中找到 `data/market_data.db` 文件
2. 下载到本地
3. 使用SQLite客户端打开查看
4. 或 `data/prices.csv` 价格文件和 `data/trading_days.csv` 交易日文件

### SQL查询示例

```sql
-- 查看所有标的最新价格
SELECT s.display_name, p.date, p.close 
FROM prices p 
JOIN symbols s ON p.symbol_code = s.symbol_code
WHERE (p.symbol_code, p.date) IN (
    SELECT symbol_code, MAX(date) 
    FROM prices 
    GROUP BY symbol_code
)
ORDER BY s.name;

-- 查看科创50ETF的历史价格
SELECT date, close 
FROM prices 
WHERE symbol_code = '588000.SS' 
ORDER BY date DESC 
LIMIT 10;

-- 查看交易日历
SELECT * FROM trading_days 
WHERE market = 'US' 
ORDER BY date DESC 
LIMIT 10;
