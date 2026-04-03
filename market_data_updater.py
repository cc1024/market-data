#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
市场数据缓存程序 - GitHub Actions版
自动获取金融数据并保存到Git仓库
"""

import os
import sys
import json
import sqlite3
import logging
import requests
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from contextlib import contextmanager

# ==================== 配置类 ====================
class Config:
    """统一配置管理"""
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(SCRIPT_DIR, "data")
    LOG_DIR = os.path.join(SCRIPT_DIR, "logs")
    DB_PATH = os.path.join(DATA_DIR, "market_data.db")
    
    # 从环境变量读取敏感信息
    TQ_USERNAME = os.environ.get('TQ_USERNAME', '')
    TQ_PASSWORD = os.environ.get('TQ_PASSWORD', '')
    
    # GitHub Actions标志
    IS_GITHUB_ACTIONS = os.environ.get('GITHUB_ACTIONS') == 'true'


os.makedirs(Config.DATA_DIR, exist_ok=True)
os.makedirs(Config.LOG_DIR, exist_ok=True)

# ==================== 日志配置 ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(Config.LOG_DIR, "market_data_updater.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ==================== 数据库工具 ====================
@contextmanager
def db_connection(db_path):
    conn = sqlite3.connect(db_path)
    try:
        yield conn
    finally:
        conn.close()


# ==================== 新浪财经API ====================
class SinaFinanceAPI:
    """新浪财经API - A股ETF数据获取"""
    
    BASE_URL = "http://money.finance.sina.com.cn/quotes_service/api/jsonp_v2.php/var=/CN_MarketData.getKLineData"
    
    @staticmethod
    def get_etf_history(symbol_code, datalen=500):
        """获取ETF历史K线数据（日线）"""
        if symbol_code.endswith('.SS'):
            sina_code = f"sh{symbol_code[:-3]}"
        elif symbol_code.endswith('.SZ'):
            sina_code = f"sz{symbol_code[:-3]}"
        else:
            return None, None
        
        url = f"{SinaFinanceAPI.BASE_URL}?symbol={sina_code}&scale=240&ma=no&datalen={datalen}"
        
        try:
            response = requests.get(url, timeout=15)
            text = response.text
            
            # 解析 JSONP 格式
            if 'var=(' in text:
                start = text.find('(')
                end = text.rfind(')')
                if start != -1 and end != -1:
                    json_str = text[start+1:end]
                else:
                    return None, None
            elif text.startswith('/*') and '*/' in text:
                json_start = text.find('(')
                json_end = text.rfind(')')
                if json_start != -1 and json_end != -1:
                    json_str = text[json_start+1:json_end]
                else:
                    return None, None
            else:
                return None, None
            
            klines = json.loads(json_str)
            
            if not klines:
                return None, None
            
            dates = []
            closes = []
            for k in klines:
                dates.append(k['day'])
                closes.append(float(k['close']))
            
            logger.debug(f"新浪API获取 {sina_code} 成功，{len(dates)} 条数据")
            return dates, closes
            
        except Exception as e:
            logger.error(f"新浪API获取 {symbol_code} 历史数据失败: {e}")
            return None, None


# ==================== 标的配置 ====================
DEFAULT_CONFIG = {'market': 'CN', 'decimals': 2, 'source': 'yahoo'}

SYMBOLS_CONFIG = {
    # A股ETF - 使用新浪财经
    '科创50ETF': {'code': '588000.SS', 'decimals': 3, 'source': 'sina'},
    '创业板50ETF': {'code': '159949.SZ', 'decimals': 3, 'source': 'sina'},
    'A50ETF': {'code': '159593.SZ', 'decimals': 3, 'source': 'sina'},
    '中证2000ETF': {'code': '563300.SS', 'decimals': 3, 'source': 'sina'},
    
    # 恒生指数
    '恒生指数': {'code': '^HSI', 'market': 'HK'},
    # 黄金
    '黄金': {'code': 'GC=F', 'market': 'COMEX'},
    # 海外指数
    '纳斯达克': {'code': '^IXIC', 'market': 'US'},
    '标普500': {'code': '^GSPC', 'market': 'US'},
    '印度': {'code': '^BSESN', 'market': 'IN'},
    '德国DAX': {'code': '^GDAXI', 'market': 'DE'},
    # 豆粕期货（需要天勤量化账号）
    '豆粕期货': {
        'code': 'KQ.i@DCE.m',
        'market': 'DCE',
        'source': 'tqsdk',
        'display_name': '豆粕期货'
    }
}

SYMBOLS = {}
for name, config in SYMBOLS_CONFIG.items():
    SYMBOLS[name] = {**DEFAULT_CONFIG, **config}
    SYMBOLS[name]['display_name'] = config.get('display_name', name)

MARKET_BENCHMARKS = {
    'CN': {'name': '中国A股', 'code': '000001.SS'},
    'US': {'name': '美国股市', 'code': '^GSPC'},
    'HK': {'name': '香港股市', 'code': '^HSI'},
    'DE': {'name': '德国股市', 'code': '^GDAXI'},
    'IN': {'name': '印度股市', 'code': '^BSESN'},
    'COMEX': {'name': '黄金期货', 'code': 'GC=F'},
    'DCE': {'name': '大连商品交易所', 'code': 'KQ.i@DCE.m'}
}


# ==================== 天勤量化客户端 ====================
class TqSdkClient:
    def __init__(self):
        self.api = None
        self.initialized = False
    
    def initialize(self):
        if self.initialized:
            return True
        if not Config.TQ_USERNAME or not Config.TQ_PASSWORD:
            logger.warning("天勤量化凭证未配置，跳过豆粕期货数据获取")
            return False
        try:
            from tqsdk import TqApi, TqAuth
            self.api = TqApi(auth=TqAuth(Config.TQ_USERNAME, Config.TQ_PASSWORD))
            self.initialized = True
            logger.info("天勤量化API初始化成功")
            return True
        except ImportError:
            logger.warning("tqsdk未安装，跳过豆粕期货数据获取")
            return False
        except Exception as e:
            logger.error(f"天勤量化初始化失败: {e}")
            return False
    
    def get_historical_data(self, symbol, days=3650):
        if not self.initialize():
            return None, None
        try:
            klines = self.api.get_kline_serial(symbol, 86400, data_length=days)
            if klines is None or len(klines) == 0:
                return None, None
            
            dates, closes = [], []
            for _, kline in klines.iterrows():
                dt = datetime.fromtimestamp(kline.datetime / 1e9)
                dates.append(dt.strftime('%Y-%m-%d'))
                closes.append(float(kline.close))
            
            dates.reverse()
            closes.reverse()
            return dates, closes
        except Exception as e:
            logger.error(f"天勤量化获取失败: {e}")
            return None, None
    
    def close(self):
        if self.api:
            self.api.close()


# ==================== 市场数据缓存类 ====================
class MarketDataCache:
    def __init__(self):
        self.db_path = Config.DB_PATH
        self.symbols = SYMBOLS
        self.market_benchmarks = MARKET_BENCHMARKS
        self.tq_client = TqSdkClient()
        self.update_stats = {
            'trading_days': {},
            'prices': {},
            'total_added': 0,
            'start_time': None,
            'end_time': None
        }
        self._init_database()
    
    def __del__(self):
        self.tq_client.close()
    
    def _init_database(self):
        with db_connection(self.db_path) as conn:
            c = conn.cursor()
            c.execute('''
                CREATE TABLE IF NOT EXISTS trading_days (
                    date TEXT, market TEXT, created_at TEXT,
                    PRIMARY KEY (date, market)
                )
            ''')
            c.execute('''
                CREATE TABLE IF NOT EXISTS prices (
                    symbol_code TEXT, date TEXT, close REAL,
                    market TEXT, source TEXT, updated_at TEXT,
                    PRIMARY KEY (symbol_code, date)
                )
            ''')
            c.execute('''
                CREATE TABLE IF NOT EXISTS symbols (
                    symbol_code TEXT PRIMARY KEY,
                    name TEXT, market TEXT, decimals INTEGER,
                    source TEXT, display_name TEXT
                )
            ''')
            c.execute('CREATE INDEX IF NOT EXISTS idx_trading_days_date ON trading_days(date)')
            c.execute('CREATE INDEX IF NOT EXISTS idx_prices_symbol ON prices(symbol_code)')
            
            # 插入或更新符号信息
            for name, config in self.symbols.items():
                c.execute('''
                    INSERT OR REPLACE INTO symbols (symbol_code, name, market, decimals, source, display_name)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (config['code'], name, config['market'], config.get('decimals', 2),
                      config.get('source', 'yahoo'), config.get('display_name', name)))
            conn.commit()
        logger.info("数据库初始化完成")
    
    def _get_trading_days(self, market, start_date, end_date):
        benchmark = self.market_benchmarks.get(market, {}).get('code')
        if not benchmark:
            return []
        try:
            start = datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=10)
            end = datetime.strptime(end_date, '%Y-%m-%d')
            data = yf.download(benchmark, start=start, end=end, progress=False)
            return [d.strftime('%Y-%m-%d') for d in data.index] if not data.empty else []
        except Exception as e:
            logger.error(f"{market} 获取交易日失败: {e}")
            return []
    
    def _get_prices_from_sina(self, symbol_code, start_date, end_date, decimals=3):
        dates, closes = SinaFinanceAPI.get_etf_history(symbol_code, datalen=500)
        if not dates or not closes:
            return []
        
        prices = []
        for d, c in zip(dates, closes):
            if start_date <= d <= end_date:
                if decimals == 3:
                    c = round(c, 3)
                prices.append({'date': d, 'close': c})
        
        if prices:
            logger.info(f"{symbol_code} [新浪] 获取到 {len(prices)} 条")
        return prices
    
    def _get_prices_from_yahoo(self, symbol_code, start_date, end_date, decimals=2):
        try:
            data = yf.download(
                symbol_code,
                start=datetime.strptime(start_date, '%Y-%m-%d'),
                end=datetime.strptime(end_date, '%Y-%m-%d'),
                progress=False
            )
            
            if data.empty:
                return []
            
            if 'Close' in data.columns:
                close_series = data['Close']
            else:
                for col in data.columns:
                    if isinstance(col, tuple) and col[0] == 'Close':
                        close_series = data[col]
                        break
                else:
                    return []
            
            if isinstance(close_series, pd.DataFrame):
                close_series = close_series.iloc[:, 0]
            
            prices = []
            for date, price in close_series.items():
                try:
                    if hasattr(price, 'iloc'):
                        price_val = float(price.iloc[0])
                    else:
                        price_val = float(price)
                    
                    if price_val > 0:
                        if decimals == 3:
                            price_val = round(price_val, 3)
                        prices.append({
                            'date': date.strftime('%Y-%m-%d'),
                            'close': price_val
                        })
                except Exception:
                    continue
            
            if prices:
                logger.info(f"{symbol_code} [Yahoo] 获取到 {len(prices)} 条")
            return prices
            
        except Exception as e:
            logger.error(f"Yahoo获取 {symbol_code} 失败: {e}")
            return []
    
    def _get_prices_from_tqsdk(self, symbol_code, start_date, end_date):
        if not Config.TQ_USERNAME or not Config.TQ_PASSWORD:
            return []
        dates, closes = self.tq_client.get_historical_data(symbol_code, days=3650)
        if not dates or not closes:
            return []
        return [
            {'date': d, 'close': c}
            for d, c in zip(dates, closes)
            if start_date <= d <= end_date
        ]
    
    def _get_prices(self, symbol_name, config):
        symbol_code = config['code']
        source = config.get('source', 'yahoo')
        decimals = config.get('decimals', 2)
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=3650)
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        if source == 'tqsdk':
            return self._get_prices_from_tqsdk(symbol_code, start_str, end_str)
        elif source == 'sina':
            return self._get_prices_from_sina(symbol_code, start_str, end_str, decimals)
        else:
            return self._get_prices_from_yahoo(symbol_code, start_str, end_str, decimals)
    
    def update_trading_days(self, market):
        if market not in self.market_benchmarks or market == 'DCE':
            return 0
        
        logger.info(f"更新交易日: {market}")
        end_date = datetime.now()
        start_date = end_date - timedelta(days=3650)
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        with db_connection(self.db_path) as conn:
            c = conn.cursor()
            c.execute("SELECT date FROM trading_days WHERE market = ?", (market,))
            existing = {row[0] for row in c.fetchall()}
            
            new_days = self._get_trading_days(market, start_str, end_str)
            if not new_days:
                return 0
            
            missing = [d for d in new_days if d not in existing]
            
            if missing:
                for date in missing:
                    c.execute('''
                        INSERT OR IGNORE INTO trading_days (date, market, created_at)
                        VALUES (?, ?, ?)
                    ''', (date, market, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                conn.commit()
                logger.info(f"{market} 新增 {len(missing)} 个交易日")
                self.update_stats['trading_days'][market] = len(missing)
            return len(missing)
    
    def update_prices(self, symbol_name, config):
        symbol_code = config['code']
        market = config['market']
        source = config.get('source', 'yahoo')
        
        logger.info(f"更新价格: {symbol_name} ({symbol_code}) [{source}]")
        
        with db_connection(self.db_path) as conn:
            c = conn.cursor()
            c.execute("SELECT date FROM prices WHERE symbol_code = ?", (symbol_code,))
            existing = {row[0] for row in c.fetchall()}
            
            new_prices = self._get_prices(symbol_name, config)
            if not new_prices:
                return 0
            
            missing = [p for p in new_prices if p['date'] not in existing]
            
            if missing:
                for p in missing:
                    c.execute('''
                        INSERT OR IGNORE INTO prices (symbol_code, date, close, market, source, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (symbol_code, p['date'], p['close'], market, source,
                          datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                conn.commit()
                logger.info(f"{symbol_name} 新增 {len(missing)} 条")
                self.update_stats['prices'][symbol_name] = len(missing)
                self.update_stats['total_added'] += len(missing)
            return len(missing)
    
    def update_all(self):
        self.update_stats['start_time'] = datetime.now()
        
        logger.info("=" * 60)
        logger.info("开始更新市场数据")
        logger.info("=" * 60)
        
        logger.info("\n[1] 更新交易日历...")
        for market in self.market_benchmarks.keys():
            self.update_trading_days(market)
        
        logger.info("\n[2] 更新价格数据...")
        for name, config in self.symbols.items():
            try:
                self.update_prices(name, config)
            except Exception as e:
                logger.error(f"{name} 更新失败: {e}")
        
        self.update_stats['end_time'] = datetime.now()
        duration = (self.update_stats['end_time'] - self.update_stats['start_time']).total_seconds()
        
        logger.info(f"\n✅ 更新完成，新增 {self.update_stats['total_added']} 条，耗时 {duration:.1f}s")
        return self.update_stats
    
    def get_statistics(self):
        with db_connection(self.db_path) as conn:
            c = conn.cursor()
            c.execute("SELECT market, COUNT(*) FROM trading_days GROUP BY market")
            trading_stats = dict(c.fetchall())
            c.execute("SELECT symbol_code, COUNT(*) FROM prices GROUP BY symbol_code")
            price_stats = dict(c.fetchall())
            db_size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
        return trading_stats, price_stats, db_size
    
    def print_statistics(self):
        trading_stats, price_stats, db_size = self.get_statistics()
        print("\n" + "=" * 70)
        print("Market Data Cache Statistics")
        print("=" * 70)
        print(f"Database Size: {db_size / 1024 / 1024:.2f} MB")
        print(f"Data Up to: {datetime.now().strftime('%Y-%m-%d')}")
        print("=" * 70)
        
        print("\n📅 Trading Days:")
        for market, count in trading_stats.items():
            market_name = self.market_benchmarks.get(market, {}).get('name', market)
            print(f"  {market_name} ({market}): {count} days")
        
        print("\n💰 Price Data:")
        for name, config in self.symbols.items():
            code = config['code']
            count = price_stats.get(code, 0)
            source = config.get('source', 'yahoo')
            source_icon = "📡" if source == 'sina' else "🌐" if source == 'yahoo' else "⚙️"
            print(f"  {source_icon} {config['display_name']} ({code}): {count} records [{source}]")
        print("=" * 70 + "\n")

def export_data_to_csv(db_path, data_dir):
    """
    将数据库中的数据导出为CSV文件
    
    Args:
        db_path: 数据库文件路径
        data_dir: CSV文件保存目录
    """
    import pandas as pd
    
    with db_connection(db_path) as conn:
        # 1. 导出所有价格数据
        logger.info("  导出价格数据...")
        prices_df = pd.read_sql_query("""
            SELECT 
                s.display_name,
                s.symbol_code,
                p.date,
                p.close,
                p.market,
                p.source,
                p.updated_at
            FROM prices p
            JOIN symbols s ON p.symbol_code = s.symbol_code
            ORDER BY p.symbol_code, p.date DESC
        """, conn)
        prices_csv = os.path.join(data_dir, "prices.csv")
        prices_df.to_csv(prices_csv, index=False, encoding='utf-8-sig')
        logger.info(f"    保存 {len(prices_df)} 条价格记录到 prices.csv")
        
        # 2. 导出最新价格快照
        logger.info("  导出最新价格...")
        latest_df = pd.read_sql_query("""
            SELECT 
                s.display_name,
                s.symbol_code,
                p.date,
                p.close,
                p.source
            FROM prices p
            JOIN symbols s ON p.symbol_code = s.symbol_code
            WHERE (p.symbol_code, p.date) IN (
                SELECT symbol_code, MAX(date)
                FROM prices
                GROUP BY symbol_code
            )
            ORDER BY s.name
        """, conn)
        latest_csv = os.path.join(data_dir, "latest_prices.csv")
        latest_df.to_csv(latest_csv, index=False, encoding='utf-8-sig')
        logger.info(f"    保存 {len(latest_df)} 条最新价格到 latest_prices.csv")
        
        # 3. 导出交易日数据
        logger.info("  导出交易日数据...")
        trading_df = pd.read_sql_query("""
            SELECT 
                market,
                date,
                created_at
            FROM trading_days
            ORDER BY market, date DESC
        """, conn)
        trading_csv = os.path.join(data_dir, "trading_days.csv")
        trading_df.to_csv(trading_csv, index=False, encoding='utf-8-sig')
        logger.info(f"    保存 {len(trading_df)} 条交易日记录到 trading_days.csv")
        
        # 4. 导出各标的统计信息
        logger.info("  导出统计信息...")
        stats_df = pd.read_sql_query("""
            SELECT 
                s.display_name,
                s.symbol_code,
                s.source,
                COUNT(p.date) as record_count,
                MIN(p.date) as first_date,
                MAX(p.date) as last_date,
                MIN(p.close) as min_price,
                MAX(p.close) as max_price,
                ROUND(AVG(p.close), 2) as avg_price
            FROM symbols s
            LEFT JOIN prices p ON s.symbol_code = p.symbol_code
            GROUP BY s.symbol_code
            ORDER BY s.name
        """, conn)
        stats_csv = os.path.join(data_dir, "statistics.csv")
        stats_df.to_csv(stats_csv, index=False, encoding='utf-8-sig')
        logger.info(f"    保存 {len(stats_df)} 条统计记录到 statistics.csv")
        
        # 5. 导出每周汇总数据（可选）
        logger.info("  导出每周汇总...")
        weekly_df = pd.read_sql_query("""
            SELECT 
                s.display_name,
                strftime('%Y-%W', p.date) as week,
                MIN(p.date) as week_start,
                MAX(p.date) as week_end,
                MIN(p.close) as week_low,
                MAX(p.close) as week_high,
                -- 获取周一的开盘价和周五的收盘价需要更复杂的查询
                -- 这里简化处理
                ROUND(AVG(p.close), 2) as week_avg
            FROM prices p
            JOIN symbols s ON p.symbol_code = s.symbol_code
            GROUP BY s.symbol_code, week
            ORDER BY s.name, week DESC
            LIMIT 1000
        """, conn)
        weekly_csv = os.path.join(data_dir, "weekly_summary.csv")
        weekly_df.to_csv(weekly_csv, index=False, encoding='utf-8-sig')
        logger.info(f"    保存 {len(weekly_df)} 条周汇总记录到 weekly_summary.csv")
    
    logger.info(f"CSV文件已保存到: {data_dir}")

def main():
    print("\n" + "=" * 70)
    print("Market Data Cache Updater - GitHub Actions Edition")
    print("=" * 70)
    print(f"Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"DB Path: {Config.DB_PATH}")
    print(f"Environment: {'GitHub Actions' if Config.IS_GITHUB_ACTIONS else 'Local'}")
    print("=" * 70)
    
    print("\nData Source Assignment:")
    for name, config in SYMBOLS.items():
        source = config.get('source', 'yahoo')
        source_icon = "📡" if source == 'sina' else "🌐" if source == 'yahoo' else "⚙️"
        print(f"  {source_icon} {config['display_name']}: {config['code']} [{source}]")
    print("=" * 70)
    
    cache = MarketDataCache()
    cache.update_all()
    cache.print_statistics()
    
    # ========== 新增：导出CSV文件 ==========
    logger.info("\n导出数据为CSV格式...")
    try:
        export_data_to_csv(Config.DB_PATH, Config.DATA_DIR)
        logger.info("CSV文件导出成功")
    except Exception as e:
        logger.error(f"CSV导出失败: {e}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
