"""
NASDAQ 1-Minute Price Data - Fetches 1-minute adjusted price data for all NASDAQ tickers that traded on a date
Reads tickers from grouped daily snapshot, filters to NASDAQ-listed, outputs previous 5 days' closes and 1-minute close prices
Time range: 4:00 AM to 8:00 PM Eastern Time
Output columns: Ticker, Date, Prev Close 1-5 (1=yesterday, 5=5 days ago), then 1-minute intervals (04:00, 04:01, ... 20:00)
Output: Excel files (.xlsx) saved per month (Jan, Feb, Mar, etc.)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict
import logging
import pytz
import time
import threading
import os
import sys
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
log_file = "universe.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8')
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

# Rate limit configuration (300 calls per second)
polygon_limiter = RateLimiter(max_calls=300, period=1.0)

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

def get_grouped_snapshot(date):
    """
    Fetch grouped daily aggregates for all stocks on a given date.
    Returns a dictionary mapping ticker to daily close.
    Used to identify which tickers actually traded on the given date.
    """
    max_retries = 3
    for attempt in range(max_retries):
        try:
            client = get_polygon_client()
            polygon_limiter.acquire()
            
            resp = client.get_grouped_daily_aggs(
                date=date,
                adjusted=True  # Adjusted data
            )
            
            if not resp or len(resp) == 0:
                if attempt < max_retries - 1:
                    logging.warning(f"Empty response for grouped snapshot on {date}, attempt {attempt + 1}/{max_retries}, retrying...")
                    time.sleep(1)
                    continue
                else:
                    logging.warning(f"Empty response for grouped snapshot on {date} after {max_retries} attempts")
                    return {}
            
            # Create dictionary: ticker -> daily close
            snapshot = {}
            for agg in resp:
                if agg.ticker and agg.close:
                    snapshot[agg.ticker] = agg.close
            
            logging.info(f"Fetched {len(snapshot)} stocks from grouped snapshot")
            return snapshot
        except ValueError as e:
            if attempt < max_retries - 1:
                logging.warning(f"JSON decode error for grouped snapshot on {date}, attempt {attempt + 1}/{max_retries}, retrying...")
                time.sleep(2)
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

def is_trading_day(date_str):
    """
    Check if a date is a trading day (not weekend, not US public holiday where market is closed).
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
            if holiday_name in ['Veterans Day', 'Columbus Day']:
                return True
            if any(closed_holiday in holiday_name for closed_holiday in market_closed_holidays):
                return False
        
        return True
    except Exception as e:
        logging.error(f"Error checking trading day for {date_str}: {e}")
        return False

def get_previous_closes(ticker, date, num_days=5):
    """
    Get previous N trading days' close prices (weekend-aware, adjusted).
    Looks back to find the specified number of trading days.
    
    Returns:
        list of tuples [(prev_close_price, prev_close_date), ...] ordered from most recent to oldest
        Returns up to num_days items. Empty list if error.
    """
    try:
        target_dt = datetime.strptime(date, "%Y-%m-%d")
        client = get_polygon_client()
        
        closes = []  # List of (close, date_str) tuples
        trading_days_found = 0
        
        # Look back up to 14 days to find 5 trading days
        for days_back in range(1, 15):
            if trading_days_found >= num_days:
                break
                
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
                    adjusted=True  # Adjusted data
                )
                
                if prev_aggs and len(prev_aggs) > 0:
                    close = prev_aggs[0].close
                    if close > 0 and close < 50000:  # Sanity check
                        closes.append((close, prev_date_str))
                        trading_days_found += 1
            except Exception as e:
                logging.debug(f"Error fetching previous close for {ticker} on {prev_date_str}: {e}")
                continue
        
        return closes
    except Exception as e:
        logging.error(f"Error in get_previous_closes for {ticker} on {date}: {e}")
        return []

def get_1min_data(ticker, date):
    """
    Fetch previous 5 days' closes and 1-minute adjusted price data from 4:00 AM to 8:00 PM Eastern.
    
    Returns:
        dict with ticker, date, prev_close_1 through prev_close_5, and 1-minute close prices keyed by time string
        or None if error
    """
    try:
        client = get_polygon_client()
        
        # Get previous 5 trading days' closes
        prev_closes = get_previous_closes(ticker, date, num_days=5)
        
        # Extract the most recent prev close for forward-fill fallback
        prev_close = prev_closes[0][0] if prev_closes else None
        
        # Fetch 1-minute bars from 4:00 AM to 8:00 PM (adjusted)
        target_dt = datetime.strptime(date, "%Y-%m-%d")
        
        polygon_limiter.acquire()
        aggs = client.get_aggs(
            ticker=ticker,
            multiplier=1,
            timespan="minute",
            from_=date,
            to=date,
            adjusted=True  # Adjusted data
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
        
        # Create complete 1-minute index from 4:00 AM to 8:00 PM
        base_date = target_dt.date()
        complete_index = pd.date_range(
            start=pd.Timestamp.combine(base_date, datetime.strptime("04:00:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ),
            end=pd.Timestamp.combine(base_date, datetime.strptime("20:00:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ),
            freq='1min'  # 1-minute intervals
        )
        
        # Reindex to complete index
        df = df.reindex(complete_index)
        
        # If 4:00 AM has no data, use previous close
        start_time = complete_index[0]
        if prev_close is not None and (start_time not in df.index or pd.isna(df.loc[start_time, 'close'])):
            df.loc[start_time, 'open'] = prev_close
            df.loc[start_time, 'high'] = prev_close
            df.loc[start_time, 'low'] = prev_close
            df.loc[start_time, 'close'] = prev_close
            df.loc[start_time, 'volume'] = 0
        
        # Forward-fill missing data (any gap uses the previous price)
        df = df.sort_index().ffill()
        
        # Build result dict
        result = {
            'Ticker': ticker,
            'Date': date
        }
        
        # Add prev close columns (Prev Close 1 = yesterday, Prev Close 2 = 2 days ago, etc.)
        for i in range(1, 6):
            if i <= len(prev_closes):
                result[f'Prev Close {i}'] = prev_closes[i - 1][0]
            else:
                result[f'Prev Close {i}'] = None
        
        # Add 1-minute close prices as columns keyed by time string
        # Use prev_close as fallback if still missing after forward fill
        for timestamp in complete_index:
            time_str = timestamp.strftime("%H:%M")
            if timestamp in df.index and pd.notna(df.loc[timestamp, 'close']):
                result[time_str] = df.loc[timestamp, 'close']
            elif prev_close is not None:
                result[time_str] = prev_close
            else:
                result[time_str] = None
        
        return result
        
    except Exception as e:
        logging.error(f"Error fetching 1-min data for {ticker} on {date}: {e}")
        return None

def process_ticker(ticker_data):
    """Process a single ticker to get 1-minute data."""
    row_index, ticker, date = ticker_data
    
    result = get_1min_data(ticker, date)
    
    if result:
        result['_row_index'] = row_index
        logging.debug(f"Fetched 1-min data for {ticker} on {date}")
        return result
    else:
        # Return error row with just ticker/date and empty prev_close columns
        logging.warning(f"No 1-min data for {ticker} on {date}")
        error_result = {
            'Ticker': ticker,
            'Date': date,
            '_row_index': row_index
        }
        for i in range(1, 6):
            error_result[f'Prev Close {i}'] = None
        return error_result

def process_single_date(date):
    """
    Process a single date: fetch grouped snapshot, filter NASDAQ tickers,
    then fetch 1-minute data for all tickers.
    """
    if not is_trading_day(date):
        logging.info(f"{date} is not a trading day, skipping")
        return []
    
    logging.info(f"Processing date: {date}")
    
    # Load NASDAQ tickers
    nasdaq_tickers = load_nasdaq_tickers()
    if not nasdaq_tickers:
        logging.warning(f"No NASDAQ tickers loaded for {date}")
        return []
    
    # Fetch grouped snapshot to get tickers that actually traded
    logging.info(f"Fetching grouped snapshot for {date}...")
    snapshot = get_grouped_snapshot(date)
    if not snapshot:
        logging.warning(f"No snapshot data for {date}")
        return []
    
    # Filter to NASDAQ tickers present in snapshot (actually traded)
    tickers_to_process = [ticker for ticker in snapshot.keys() if ticker in nasdaq_tickers]
    logging.info(f"Found {len(tickers_to_process)} NASDAQ tickers that traded on {date}")
    
    if not tickers_to_process:
        return []
    
    # Sort tickers alphabetically for consistent ordering
    tickers_to_process.sort()
    
    # Prepare data for parallel processing
    ticker_data = [(i, ticker, date) for i, ticker in enumerate(tickers_to_process)]
    
    # Process all tickers in parallel with 50 workers
    results_dict = {}
    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = {executor.submit(process_ticker, data): data[0] for data in ticker_data}
        
        completed = 0
        for future in as_completed(futures):
            row_index = futures[future]
            try:
                result = future.result()
                if result:
                    results_dict[result.get('_row_index', row_index)] = result
            except Exception as e:
                ticker = ticker_data[row_index][1]
                logging.error(f"Error processing {ticker} on {date}: {e}")
                error_result = {
                    'Ticker': ticker,
                    'Date': date,
                    '_row_index': row_index
                }
                for i in range(1, 6):
                    error_result[f'Prev Close {i}'] = None
                results_dict[row_index] = error_result
            
            completed += 1
            if completed % 100 == 0:
                logging.info(f"Processed {completed}/{len(tickers_to_process)} tickers...")
    
    # Ensure all rows are included in order
    all_results = []
    for row_index in range(len(ticker_data)):
        if row_index in results_dict:
            all_results.append(results_dict[row_index])
        else:
            _, ticker, _ = ticker_data[row_index]
            error_result = {
                'Ticker': ticker,
                'Date': date,
                '_row_index': row_index
            }
            for i in range(1, 6):
                error_result[f'Prev Close {i}'] = None
            all_results.append(error_result)
    
    logging.info(f"Fetched data for {len(all_results)} NASDAQ tickers on {date}")
    return all_results

def main():
    """Main function to fetch 1-minute price data for all NASDAQ tickers.
    
    Usage:
        python universe.py 2025-01-15              # Single date
        python universe.py 2025-01-01 to 2025-01-10  # Date range
        python universe.py 2025-01-01 - 2025-01-10   # Date range (alternative)
    """
    start_time = time.time()
    
    # Parse command-line arguments
    if len(sys.argv) == 1:
        print("Usage:")
        print("  python universe.py 2025-01-15              # Single date")
        print("  python universe.py 2025-01-01 to 2025-01-10  # Date range")
        print("  python universe.py 2025-01-01 - 2025-01-10   # Date range (alternative)")
        return
    elif len(sys.argv) == 2:
        # Single date
        dates_to_process = [sys.argv[1]]
        logging.info(f"Starting NASDAQ 1-min data fetch for {sys.argv[1]}")
    elif len(sys.argv) == 4 and (sys.argv[2].lower() == "to" or sys.argv[2] == "-"):
        # Date range
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
            
            logging.info(f"Starting NASDAQ 1-min data fetch for date range: {start_date_str} to {end_date_str}")
            logging.info(f"Processing {len(dates_to_process)} trading days")
        except ValueError as e:
            logging.error(f"Invalid date format: {e}")
            print(f"Error: Invalid date format. Use YYYY-MM-DD")
            return
    else:
        print("Usage:")
        print("  python universe.py 2025-01-15              # Single date")
        print("  python universe.py 2025-01-01 to 2025-01-10  # Date range")
        print("  python universe.py 2025-01-01 - 2025-01-10   # Date range (alternative)")
        return
    
    if not dates_to_process:
        logging.warning("No trading days to process")
        print("No trading days to process")
        return
    
    # Helper function to get month period key (e.g., "2025-01" for Jan 2025)
    def get_month_key(date_str):
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return f"{dt.year}-{dt.month:02d}"
    
    # Helper function to get period label (e.g., "2025_Jan")
    def get_period_label(month_key):
        year, month = month_key.split("-")
        month = int(month)
        month_names = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun", 
                       7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}
        return f"{year}_{month_names[month]}"
    
    # Group dates by month periods
    dates_by_period = defaultdict(list)
    for date in dates_to_process:
        period_key = get_month_key(date)
        dates_by_period[period_key].append(date)
    
    # Sort periods chronologically
    sorted_periods = sorted(dates_by_period.keys())
    
    logging.info(f"Processing {len(dates_to_process)} dates across {len(sorted_periods)} months")
    print(f"Processing {len(dates_to_process)} dates across {len(sorted_periods)} months")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    total_rows_saved = 0
    
    # Process each month separately to manage memory
    for period_key in sorted_periods:
        period_dates = dates_by_period[period_key]
        period_label = get_period_label(period_key)
        
        logging.info(f"Processing period {period_label} ({len(period_dates)} dates)...")
        print(f"\nProcessing period {period_label} ({len(period_dates)} dates)...")
        
        # Collect results for this period
        period_results = []
        
        for date in period_dates:
            logging.info(f"Processing {date}...")
            print(f"Processing {date}...")
            
            results = process_single_date(date)
            
            if not results:
                logging.info(f"No data for {date}")
                print(f"No data for {date}")
                continue
            
            period_results.extend(results)
            logging.info(f"Added {len(results)} rows for {date}")
            print(f"Added {len(results)} rows for {date}")
        
        if not period_results:
            logging.warning(f"No data collected for period {period_label}")
            print(f"No data collected for period {period_label}")
            continue
        
        # Create DataFrame from period results
        df = pd.DataFrame(period_results)
        
        # Sort by Date then Ticker
        df = df.sort_values(by=['Date', 'Ticker'])
        
        # Remove the temporary _row_index column before saving
        if '_row_index' in df.columns:
            df = df.drop(columns=['_row_index'])
        
        # Reorder columns: Ticker, Date, Prev Close 1-5, then time columns
        prev_close_cols = [f'Prev Close {i}' for i in range(1, 6)]
        time_columns = [col for col in df.columns if col not in ['Ticker', 'Date', '_row_index'] + prev_close_cols]
        time_columns.sort()  # Sort time columns chronologically
        ordered_columns = ['Ticker', 'Date'] + prev_close_cols + time_columns
        df = df[ordered_columns]
        
        # Format price columns (prev close and time columns)
        price_columns = [col for col in df.columns if col not in ['Ticker', 'Date']]
        for col in price_columns:
            df[col] = df[col].apply(lambda x: f"${x:.2f}" if pd.notna(x) and x is not None else "")
        
        # Generate output filename for this period
        output_file = f"universe_{period_label}_{timestamp}.xlsx"
        
        # Save to Excel
        df.to_excel(output_file, index=False, engine='openpyxl')
        logging.info(f"Output saved to {output_file}")
        logging.info(f"Period rows: {len(df)}, Dates: {len(period_dates)}")
        print(f"Output saved to {output_file}")
        print(f"Period rows: {len(df)}, Dates: {len(period_dates)}")
        
        total_rows_saved += len(df)
        
        # Clear memory
        del df
        del period_results
    
    logging.info(f"Total rows saved across all periods: {total_rows_saved}")
    print(f"\nTotal rows saved across all periods: {total_rows_saved}")
    
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
    print(f"Total execution time: {time_str}")

if __name__ == "__main__":
    main()
