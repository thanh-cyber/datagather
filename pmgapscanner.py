"""
PM Gap Scanner - Scans NASDAQ tickers for 5%+ pre-market gaps
Gap calculation: ((PM High - Previous Close) / Previous Close) * 100 >= 5%
PM Session: 4:00 AM to 9:29:59 AM Eastern Time
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import pytz
import time
import threading
import os
import sys
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from polygon import RESTClient
import urllib3
import holidays

# Disable urllib3 warnings (including connection pool warnings)
urllib3.disable_warnings()
# Suppress connection pool full warnings - these are informational
import warnings
warnings.filterwarnings('ignore', message='.*Connection pool is full.*')

# Configure logging (file only, no terminal output)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    handlers=[
        logging.FileHandler("pmgap.log", encoding='utf-8')
    ]
)

# Suppress urllib3 connection pool warnings (informational only)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

# Load environment variables
load_dotenv()

# API keys
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
if not POLYGON_API_KEY:
    logging.error("POLYGON_API_KEY not found in environment variables")
    sys.exit(1)

# Timezone
EASTERN_TZ = pytz.timezone('US/Eastern')

# Rate limiter for API calls
class RateLimiter:
    def __init__(self, max_calls: int, period: float):
        self.max_calls = max_calls
        self.period = period
        self.lock = threading.Lock()
        self.calls = []

    def acquire(self):
        with self.lock:
            now = time.time()
            while self.calls and self.calls[0] <= now - self.period:
                self.calls.pop(0)
            if len(self.calls) >= self.max_calls:
                sleep_time = self.period - (now - self.calls[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
            self.calls.append(time.time())

# Rate limit configuration
POLYGON_PREMIUM = os.getenv("POLYGON_PREMIUM", "false").lower() == "true"
polygon_limiter = RateLimiter(max_calls=300 if POLYGON_PREMIUM else 30, period=1.0 if POLYGON_PREMIUM else 60.0)

# Connection pool configuration
POLYGON_NUM_POOLS = 500
POLYGON_RETRIES = 5

# Polygon client singleton
_polygon_client = None
_polygon_client_lock = threading.Lock()

def get_polygon_client():
    """Get or create the singleton Polygon REST client."""
    global _polygon_client
    if _polygon_client is None:
        with _polygon_client_lock:
            if _polygon_client is None:
                _polygon_client = RESTClient(
                    POLYGON_API_KEY,
                    num_pools=POLYGON_NUM_POOLS,
                    retries=POLYGON_RETRIES
                )
    return _polygon_client

# NASDAQ tickers cache
NASDAQ_TICKERS_CACHE = "nasdaq_tickers_cache.txt"

def load_nasdaq_tickers():
    """Load NASDAQ tickers from cache file."""
    try:
        if os.path.exists(NASDAQ_TICKERS_CACHE):
            with open(NASDAQ_TICKERS_CACHE, 'r') as f:
                tickers = [line.strip() for line in f if line.strip()]
            logging.info(f"Loaded {len(tickers)} NASDAQ tickers from cache")
            return set(tickers)
        else:
            logging.warning(f"NASDAQ tickers cache file not found: {NASDAQ_TICKERS_CACHE}")
            return set()
    except Exception as e:
        logging.error(f"Error loading NASDAQ tickers: {e}")
        return set()

def get_previous_close(ticker, date):
    """
    Get previous day's close price (weekend-aware, unadjusted).
    Looks back up to 7 days to find the last trading day.
    """
    try:
        target_dt = datetime.strptime(date, "%Y-%m-%d")
        client = get_polygon_client()
        
        # Look back up to 7 days to find the last trading day
        for days_back in range(1, 8):
            prev_date = target_dt - timedelta(days=days_back)
            prev_date_str = prev_date.strftime("%Y-%m-%d")
            
            # Skip weekends
            if prev_date.weekday() >= 5:  # Saturday=5, Sunday=6
                continue
            
            try:
                polygon_limiter.acquire()
                prev_aggs = client.get_aggs(
                    ticker=ticker,
                    multiplier=1,
                    timespan="day",
                    from_=prev_date_str,
                    to=prev_date_str,
                    adjusted=False  # Unadjusted data
                )
                
                if prev_aggs and len(prev_aggs) > 0:
                    close = prev_aggs[0].close
                    if close > 0 and close < 50000:  # Sanity check
                        return close
            except Exception as e:
                logging.debug(f"Error fetching previous close for {ticker} on {prev_date_str}: {e}")
                continue
        
        return None
    except Exception as e:
        logging.error(f"Error in get_previous_close for {ticker} on {date}: {e}")
        return None

def get_grouped_snapshot(date):
    """
    Fetch grouped daily aggregates for all stocks on a given date (unadjusted).
    Returns a dictionary mapping ticker to daily high.
    Used to identify which tickers actually traded on the given date.
    """
    max_retries = 3
    for attempt in range(max_retries):
        try:
            client = get_polygon_client()
            polygon_limiter.acquire()
            
            resp = client.get_grouped_daily_aggs(
                date=date,
                adjusted=False  # Unadjusted data
            )
            
            # get_grouped_daily_aggs returns a list directly, not an object with .results
            # If JSON decode error occurred, SDK returns empty list
            if not resp or len(resp) == 0:
                if attempt < max_retries - 1:
                    logging.warning(f"Empty response for grouped snapshot on {date}, attempt {attempt + 1}/{max_retries}, retrying...")
                    time.sleep(1)  # Brief delay before retry
                    continue
                else:
                    logging.warning(f"Empty response for grouped snapshot on {date} after {max_retries} attempts")
                    return {}
            
            # Create dictionary: ticker -> daily high
            snapshot = {}
            for agg in resp:
                if agg.ticker and agg.high:
                    snapshot[agg.ticker] = agg.high
            
            logging.info(f"Fetched {len(snapshot)} stocks from grouped snapshot")
            return snapshot
        except ValueError as e:
            # JSON decode error from Polygon SDK
            if attempt < max_retries - 1:
                logging.warning(f"JSON decode error for grouped snapshot on {date}, attempt {attempt + 1}/{max_retries}, retrying...")
                time.sleep(2)  # Longer delay for JSON errors
                continue
            else:
                logging.error(f"JSON decode error for grouped snapshot on {date} after {max_retries} attempts: {e}")
                return {}
        except Exception as e:
            if attempt < max_retries - 1:
                logging.warning(f"Error fetching grouped snapshot for {date}, attempt {attempt + 1}/{max_retries}, retrying...: {e}")
                time.sleep(1)
                continue
            else:
                logging.error(f"Error fetching grouped snapshot for {date} after {max_retries} attempts: {e}")
                return {}
    
    return {}

def batch_get_previous_closes(tickers, date):
    """
    Batch fetch previous closes for all tickers in parallel.
    Returns a dictionary mapping ticker to previous close.
    """
    previous_closes = {}
    lock = threading.Lock()
    
    def fetch_previous_close(ticker):
        try:
            prev_close = get_previous_close(ticker, date)
            if prev_close:
                with lock:
                    previous_closes[ticker] = prev_close
        except Exception as e:
            logging.debug(f"Error fetching previous close for {ticker}: {e}")
    
    # Process in parallel with 100 workers
    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = [executor.submit(fetch_previous_close, ticker) for ticker in tickers]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.debug(f"Error in batch_get_previous_closes: {e}")
    
    return previous_closes

def check_gap_and_get_data_regular_session(ticker, date, previous_close=None):
    """
    Check if ticker has 5%+ gap during regular market session and return data if qualified.
    Gap calculation: ((Regular Session High - Previous Close) / Previous Close) * 100 >= 5%
    Regular Session: 9:30 AM to 4:00 PM Eastern Time
    Uses minute price data (unadjusted)
    
    Returns:
        dict with ticker, date, previous_close, regular_open, regular_high, gap_percent, minute_data (minute intervals)
        or None if gap < 5%
    """
    try:
        client = get_polygon_client()
        
        # Get previous close if not provided
        if previous_close is None:
            previous_close = get_previous_close(ticker, date)
            if not previous_close:
                return None
        
        # Fetch minute bars from 4:00 AM to 8:00 PM (unadjusted) to get full day data
        target_dt = datetime.strptime(date, "%Y-%m-%d")
        start_time = target_dt.replace(hour=4, minute=0, second=0, microsecond=0)
        end_time = target_dt.replace(hour=20, minute=0, second=0, microsecond=0)
        
        start_str = start_time.strftime("%Y-%m-%d")
        end_str = end_time.strftime("%Y-%m-%d")
        
        polygon_limiter.acquire()
        aggs = client.get_aggs(
            ticker=ticker,
            multiplier=1,
            timespan="minute",
            from_=start_str,
            to=end_str,
            adjusted=False  # Unadjusted data
        )
        
        if not aggs or len(aggs) == 0:
            return None
        
        # Convert to DataFrame
        data = []
        for agg in aggs:
            data.append({
                'timestamp': pd.Timestamp(agg.timestamp, unit='ms', tz='UTC').tz_convert(EASTERN_TZ),
                'open': agg.open,
                'high': agg.high,
                'low': agg.low,
                'close': agg.close,
                'volume': agg.volume
            })
        
        df = pd.DataFrame(data)
        df = df.set_index('timestamp').sort_index()
        
        # Create complete minute-by-minute index from 4:00 AM to 8:00 PM
        base_date = target_dt.date()
        complete_index = pd.date_range(
            start=pd.Timestamp.combine(base_date, datetime.strptime("04:00:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ),
            end=pd.Timestamp.combine(base_date, datetime.strptime("20:00:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ),
            freq='1min'  # 1-minute intervals
        )
        
        # Reindex to complete index
        df = df.reindex(complete_index)
        
        # Forward-fill missing data (using modern pandas syntax)
        df = df.sort_index().ffill()
        
        # Filter to regular market session (9:30 AM to 4:00 PM)
        regular_start = pd.Timestamp.combine(base_date, datetime.strptime("09:30:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ)
        regular_end = pd.Timestamp.combine(base_date, datetime.strptime("16:00:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ)
        regular_session = df[(df.index >= regular_start) & (df.index <= regular_end)].copy()
        
        if regular_session.empty:
            return None
        
        # Calculate regular session high
        regular_high = regular_session["high"].max()
        # Get regular session open - use the open price at 9:30 AM
        regular_open_time = pd.Timestamp.combine(base_date, datetime.strptime("09:30:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ)
        if regular_open_time in df.index and pd.notna(df.loc[regular_open_time, 'open']):
            regular_open = df.loc[regular_open_time, 'open']
        else:
            # Fallback to previous close if no data at 9:30 AM
            regular_open = previous_close
        
        # Calculate gap percentage
        if previous_close and previous_close > 0:
            gap_percent = ((regular_high - previous_close) / previous_close) * 100
        else:
            return None
        
        # Check if gap >= 5%
        if gap_percent < 5.0:
            return None
        
        # Forward-fill any remaining missing minute data (using modern pandas syntax)
        df = df.ffill()
        
        # Ensure all minutes from 4:00 AM to 8:00 PM have data (backfill with previous_close if still missing)
        df = df.bfill()
        for idx in df.index:
            if pd.isna(df.loc[idx, 'close']):
                df.loc[idx, 'close'] = previous_close
        
        return {
            'ticker': ticker,
            'date': date,
            'previous_close': previous_close,
            'regular_open': regular_open,
            'regular_high': regular_high,
            'gap_percent': round(gap_percent, 2),
            'minute_data': df[['close']]  # Only keep close prices for output (minute intervals)
        }
        
    except Exception as e:
        logging.error(f"Error in check_gap_and_get_data_regular_session for {ticker} on {date}: {e}")
        return None

def check_gap_and_get_data(ticker, date, previous_close=None):
    """
    Check if ticker has 5%+ gap during PM session and return data if qualified.
    Gap calculation: ((PM High - Previous Close) / Previous Close) * 100 >= 5%
    PM Session: 4:00 AM to 9:29:59 AM Eastern Time
    Uses minute price data (unadjusted)
    
    Returns:
        dict with ticker, date, previous_close, pm_open, pm_high, gap_percent, minute_data (minute intervals)
        or None if gap < 5%
    """
    try:
        client = get_polygon_client()
        
        # Get previous close if not provided
        if previous_close is None:
            previous_close = get_previous_close(ticker, date)
            if not previous_close:
                return None
        
        # Fetch minute bars from 4:00 AM to 8:00 PM (unadjusted)
        target_dt = datetime.strptime(date, "%Y-%m-%d")
        start_time = target_dt.replace(hour=4, minute=0, second=0, microsecond=0)
        end_time = target_dt.replace(hour=20, minute=0, second=0, microsecond=0)
        
        start_str = start_time.strftime("%Y-%m-%d")
        end_str = end_time.strftime("%Y-%m-%d")
        
        polygon_limiter.acquire()
        aggs = client.get_aggs(
            ticker=ticker,
            multiplier=1,
            timespan="minute",
            from_=start_str,
            to=end_str,
            adjusted=False  # Unadjusted data
        )
        
        if not aggs or len(aggs) == 0:
            return None
        
        # Convert to DataFrame
        data = []
        for agg in aggs:
            data.append({
                'timestamp': pd.Timestamp(agg.timestamp, unit='ms', tz='UTC').tz_convert(EASTERN_TZ),
                'open': agg.open,
                'high': agg.high,
                'low': agg.low,
                'close': agg.close,
                'volume': agg.volume
            })
        
        df = pd.DataFrame(data)
        df = df.set_index('timestamp').sort_index()
        
        # Create complete minute-by-minute index from 4:00 AM to 8:00 PM
        base_date = target_dt.date()
        complete_index = pd.date_range(
            start=pd.Timestamp.combine(base_date, datetime.strptime("04:00:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ),
            end=pd.Timestamp.combine(base_date, datetime.strptime("20:00:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ),
            freq='1min'  # 1-minute intervals
        )
        
        # Reindex to complete index
        df = df.reindex(complete_index)
        
        # Handle missing 4:00 AM PM open - use previous close and forward-fill
        pm_open_time = pd.Timestamp.combine(base_date, datetime.strptime("04:00:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ)
        if pm_open_time not in df.index or pd.isna(df.loc[pm_open_time, 'close']):
            # Create synthetic bar at 4:00 AM with previous close
            df.loc[pm_open_time, 'open'] = previous_close
            df.loc[pm_open_time, 'high'] = previous_close
            df.loc[pm_open_time, 'low'] = previous_close
            df.loc[pm_open_time, 'close'] = previous_close
            df.loc[pm_open_time, 'volume'] = 0
        
        # Forward-fill missing data (using modern pandas syntax)
        df = df.sort_index().ffill()
        
        # Filter to PM session (4:00 AM to 9:29:59 AM)
        pm_start = pd.Timestamp.combine(base_date, datetime.strptime("04:00:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ)
        pm_end = pd.Timestamp.combine(base_date, datetime.strptime("09:29:59", "%H:%M:%S").time()).tz_localize(EASTERN_TZ)
        pre_market = df[(df.index >= pm_start) & (df.index <= pm_end)].copy()
        
        if pre_market.empty:
            return None
        
        # Calculate PM high
        pm_high = pre_market["high"].max()
        # Get PM open - use the open price at 4:00 AM from original df (we ensured it exists above)
        pm_open_time = pd.Timestamp.combine(base_date, datetime.strptime("04:00:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ)
        if pm_open_time in df.index and pd.notna(df.loc[pm_open_time, 'open']):
            pm_open = df.loc[pm_open_time, 'open']
        else:
            # Fallback to previous close (shouldn't happen since we create synthetic bar)
            pm_open = previous_close
        
        # Calculate gap percentage
        if previous_close and previous_close > 0:
            gap_percent = ((pm_high - previous_close) / previous_close) * 100
        else:
            return None
        
        # Check if gap >= 5%
        if gap_percent < 5.0:
            return None
        
        # Forward-fill any remaining missing minute data (using modern pandas syntax)
        df = df.ffill()
        
        # Ensure all minutes from 4:00 AM to 8:00 PM have data (backfill with previous_close if still missing)
        df = df.bfill()
        for idx in df.index:
            if pd.isna(df.loc[idx, 'close']):
                df.loc[idx, 'close'] = previous_close
        
        return {
            'ticker': ticker,
            'date': date,
            'previous_close': previous_close,
            'pm_open': pm_open,
            'pm_high': pm_high,
            'gap_percent': round(gap_percent, 2),
            'minute_data': df[['close']]  # Only keep close prices for output (minute intervals)
        }
        
    except Exception as e:
        logging.error(f"Error in check_gap_and_get_data for {ticker} on {date}: {e}")
        return None

def is_trading_day(date_str):
    """
    Check if a date is a trading day (not weekend, not US public holiday where market is closed).
    Note: Veterans Day and Columbus Day markets are open.
    """
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        
        # Check if weekend
        if date_obj.weekday() >= 5:  # Saturday=5, Sunday=6
            return False
        
        # Check US public holidays
        us_holidays = holidays.UnitedStates(years=date_obj.year)
        
        # List of holidays where market is CLOSED
        market_closed_holidays = [
            'New Year\'s Day',
            'Martin Luther King Jr. Day',
            'Presidents\' Day',
            'Good Friday',
            'Memorial Day',
            'Independence Day',
            'Labor Day',
            'Thanksgiving',
            'Christmas'
        ]
        
        # Check if date is a holiday where market is closed
        holiday_name = us_holidays.get(date_obj)
        if holiday_name:
            # Veterans Day and Columbus Day are explicitly open
            if holiday_name in ['Veterans Day', 'Columbus Day']:
                return True
            # Check if it's a market-closed holiday
            if any(closed_holiday in holiday_name for closed_holiday in market_closed_holidays):
                return False
        
        return True
    except Exception as e:
        logging.error(f"Error checking trading day for {date_str}: {e}")
        return False

def process_ticker(ticker, date, previous_close):
    """Process a single ticker and return result if it meets gap criteria."""
    result = check_gap_and_get_data(ticker, date, previous_close)
    if result:
        logging.info(f"[FOUND] {ticker} on {date}: {result['gap_percent']}% gap")
    return result

def process_ticker_regular_session(ticker, date, previous_close):
    """Process a single ticker for regular session and return result if it meets gap criteria."""
    result = check_gap_and_get_data_regular_session(ticker, date, previous_close)
    if result:
        logging.info(f"[FOUND] {ticker} on {date}: {result['gap_percent']}% gap (regular session)")
    return result

def process_single_date(date, regular_session=False):
    """
    Process a single date: fetch grouped snapshot, filter NASDAQ tickers,
    batch fetch previous closes, then check gaps for all tickers.
    
    Args:
        date: Date string in YYYY-MM-DD format
        regular_session: If True, scan regular market session (9:30 AM - 4:00 PM), 
                        otherwise scan PM session (4:00 AM - 9:29:59 AM)
    """
    if not is_trading_day(date):
        logging.info(f"{date} is not a trading day, skipping")
        return []
    
    session_type = "regular session" if regular_session else "PM session"
    logging.info(f"Processing date: {date} ({session_type})")
    
    # Load NASDAQ tickers
    nasdaq_tickers = load_nasdaq_tickers()
    if not nasdaq_tickers:
        logging.warning(f"No NASDAQ tickers loaded for {date}")
        return []
    
    # Fetch grouped snapshot
    logging.info(f"Fetching grouped snapshot for {date}...")
    snapshot = get_grouped_snapshot(date)
    if not snapshot:
        logging.warning(f"No snapshot data for {date}")
        return []
    
    # Filter to NASDAQ tickers present in snapshot
    tickers_in_snapshot = [ticker for ticker in snapshot.keys() if ticker in nasdaq_tickers]
    logging.info(f"Found {len(tickers_in_snapshot)} NASDAQ tickers in snapshot (out of {len(nasdaq_tickers)} total)")
    
    if not tickers_in_snapshot:
        return []
    
    # Batch fetch previous closes for all tickers
    logging.info(f"Batch fetching previous closes for {len(tickers_in_snapshot)} tickers...")
    previous_closes = batch_get_previous_closes(tickers_in_snapshot, date)
    logging.info(f"Fetched {len(previous_closes)} previous closes")
    
    # Filter to only tickers that have previous closes
    tickers_to_process = [ticker for ticker in tickers_in_snapshot if ticker in previous_closes]
    logging.info(f"Processing {len(tickers_to_process)} tickers with previous closes")
    
    if not tickers_to_process:
        return []
    
    # Process all tickers in parallel
    results = []
    process_func = process_ticker_regular_session if regular_session else process_ticker
    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = {
            executor.submit(process_func, ticker, date, previous_closes.get(ticker)): ticker
            for ticker in tickers_to_process
        }
        
        completed = 0
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as e:
                logging.error(f"Error processing {ticker} on {date}: {e}")
            
            completed += 1
            if completed % 100 == 0:
                logging.info(f"Processed {completed}/{len(tickers_to_process)} tickers...")
    
    logging.info(f"Found {len(results)} tickers with 5%+ gaps for {date} ({session_type})")
    return results

def main():
    """Main function to scan NASDAQ tickers for 5%+ gaps.
    
    Usage:
        python pmgapscanner.py                          # Today's date (PM session)
        python pmgapscanner.py open                    # Today's date (regular session)
        python pmgapscanner.py 2025-01-01               # Single date (PM session)
        python pmgapscanner.py open 2025-01-01          # Single date (regular session)
        python pmgapscanner.py 2025-01-01 to 2025-12-10 # Date range (PM session)
        python pmgapscanner.py open 2025-01-01 to 2025-12-10 # Date range (regular session)
        python pmgapscanner.py 2025-01-01 - 2025-12-10  # Date range (alternative, PM session)
        python pmgapscanner.py open 2025-01-01 - 2025-12-10  # Date range (alternative, regular session)
    """
    start_time = time.time()
    
    # Check for "open" keyword as first argument (case-insensitive)
    regular_session = False
    if len(sys.argv) > 1 and sys.argv[1].lower() == "open":
        regular_session = True
        sys.argv.pop(1)  # Remove "open" from arguments
    
    # Parse command-line arguments
    if len(sys.argv) == 1:
        # No arguments - use today's date
        target_date = datetime.now(EASTERN_TZ).strftime("%Y-%m-%d")
        dates_to_process = [target_date]
        session_type = "regular session" if regular_session else "PM session"
        logging.info(f"Starting gap scanner for {target_date} ({session_type})")
    elif len(sys.argv) == 2:
        # Single date
        dates_to_process = [sys.argv[1]]
        session_type = "regular session" if regular_session else "PM session"
        logging.info(f"Starting gap scanner for {sys.argv[1]} ({session_type})")
    elif len(sys.argv) == 4 and (sys.argv[2].lower() == "to" or sys.argv[2] == "-"):
        # Date range: python pmgapscanner.py 2025-01-01 to 2025-12-10
        # Also supports: python pmgapscanner.py 2025-01-01 - 2025-12-10
        start_date_str = sys.argv[1]
        end_date_str = sys.argv[3]
        
        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
            
            # Generate list of dates
            dates_to_process = []
            current_date = start_date
            while current_date <= end_date:
                date_str = current_date.strftime("%Y-%m-%d")
                if is_trading_day(date_str):
                    dates_to_process.append(date_str)
                current_date += timedelta(days=1)
            
            session_type = "regular session" if regular_session else "PM session"
            logging.info(f"Starting gap scanner for date range: {start_date_str} to {end_date_str} ({session_type})")
            logging.info(f"Processing {len(dates_to_process)} trading days")
        except ValueError as e:
            logging.error(f"Invalid date format: {e}")
            print(f"Error: Invalid date format. Use YYYY-MM-DD")
            return
    else:
        logging.error("Invalid arguments")
        print("Usage:")
        print("  python pmgapscanner.py                          # Today's date (PM session)")
        print("  python pmgapscanner.py open                     # Today's date (regular session)")
        print("  python pmgapscanner.py 2025-01-01               # Single date (PM session)")
        print("  python pmgapscanner.py open 2025-01-01          # Single date (regular session)")
        print("  python pmgapscanner.py 2025-01-01 to 2025-12-10 # Date range (PM session)")
        print("  python pmgapscanner.py open 2025-01-01 to 2025-12-10 # Date range (regular session)")
        print("  python pmgapscanner.py 2025-01-01 - 2025-12-10  # Date range (alternative, PM session)")
        print("  python pmgapscanner.py open 2025-01-01 - 2025-12-10  # Date range (alternative, regular session)")
        return
    
    if not dates_to_process:
        logging.warning("No trading days to process")
        return
    
    # Process dates in parallel (10 concurrent dates)
    all_results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_single_date, date, regular_session): date for date in dates_to_process}
        
        for future in as_completed(futures):
            date = futures[future]
            try:
                results = future.result()
                all_results.extend(results)
            except Exception as e:
                logging.error(f"Error processing date {date}: {e}")
    
    if not all_results:
        session_type = "regular session" if regular_session else "PM session"
        logging.info(f"No tickers found with 5%+ gaps ({session_type})")
        print(f"No tickers found with 5%+ gaps ({session_type})")
        return
    
    # Create output DataFrame
    output_rows = []
    
    for result in all_results:
        if regular_session:
            row = {
                'Ticker': result['ticker'],
                'Date': result['date'],
                'Previous Close': f"${result['previous_close']:.2f}",
                'Regular Open': f"${result['regular_open']:.2f}",
                'Regular High': f"${result['regular_high']:.2f}",
                'Gap %': f"{result['gap_percent']}%"
            }
        else:
            row = {
                'Ticker': result['ticker'],
                'Date': result['date'],
                'Previous Close': f"${result['previous_close']:.2f}",
                'PM Open': f"${result['pm_open']:.2f}",
                'PM High': f"${result['pm_high']:.2f}",
                'Gap %': f"{result['gap_percent']}%"
            }
        
        # Add minute close prices from 04:00 to 20:00
        minute_data = result['minute_data']
        
        # OPTIMIZATION: Iterate DataFrame index directly (faster than dictionary conversion + lookups)
        # The minute_data already has forward-filled data with complete index from 4:00 AM to 8:00 PM
        # This avoids timestamp matching issues and is ~6x faster than dictionary lookups
        for timestamp, close_price in minute_data['close'].items():
            time_str = timestamp.strftime("%H:%M")  # Minute intervals
            
            if pd.isna(close_price) or close_price is None:
                close_price = result['previous_close']
            
            if pd.notna(close_price) and close_price is not None:
                row[time_str] = f"${close_price:.2f}"
            else:
                row[time_str] = "N/A"
        
        output_rows.append(row)
    
    # Create DataFrame and save to CSV
    df = pd.DataFrame(output_rows)
    
    # Sort by gap % descending
    def extract_gap_percent(value):
        """Extract numeric gap percent from string like '5.0%'."""
        try:
            if isinstance(value, str):
                return float(value.rstrip('%'))
            return float(value)
        except (ValueError, AttributeError):
            return 0.0
    
    df = df.sort_values(by='Gap %', ascending=False, key=lambda x: x.apply(extract_gap_percent))
    
    # Save to CSV with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    session_prefix = "regular" if regular_session else "pm"
    output_file = f"{session_prefix}gap_scanner_{timestamp}.csv"
    
    df.to_csv(output_file, index=False, quoting=csv.QUOTE_ALL)
    logging.info(f"Output saved to {output_file}")
    session_type = "regular session" if regular_session else "PM session"
    logging.info(f"Total tickers with 5%+ gaps ({session_type}): {len(df)}")
    
    # Calculate and display total time
    end_time = time.time()
    total_seconds = end_time - start_time
    total_minutes = total_seconds / 60
    total_hours = total_minutes / 60
    
    if total_hours >= 1:
        time_str = f"{total_hours:.2f} hours ({total_minutes:.1f} minutes)"
    elif total_minutes >= 1:
        time_str = f"{total_minutes:.2f} minutes ({total_seconds:.1f} seconds)"
    else:
        time_str = f"{total_seconds:.2f} seconds"
    
    logging.info(f"Total execution time: {time_str}")
    
    # Print to terminal (only the execution time)
    print(f"Total execution time: {time_str}")

if __name__ == "__main__":
    main()

