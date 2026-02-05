"""
Previous High Fetcher - Fetches the highest high from 4:00 AM to 8:00 PM ET
for each of the previous 50 trading days from Polygon.io.
Reads tickers and dates from tickers.xlsx.
Output columns: Ticker, Date, then 50 columns:
    "High YYYY-MM-DD" (one per previous trading day, most recent first)
"""

import pandas as pd
from datetime import datetime, timedelta
import logging
import time
import threading
import os
import sys
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from polygon import RESTClient
import urllib3
import pytz

# Disable urllib3 warnings (including connection pool warnings)
urllib3.disable_warnings()
# Suppress connection pool full warnings - these are informational
import warnings
warnings.filterwarnings('ignore', message='.*Connection pool is full.*')

# Configure logging
log_file = "prevhigh.log"

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

# Rate limit configuration (fixed to 300 calls/second)
polygon_limiter = RateLimiter(max_calls=300, period=1.0)

# Connection pool configuration
POLYGON_NUM_POOLS = 500
POLYGON_RETRIES = 5

# Timezone
EASTERN_TZ = pytz.timezone("US/Eastern")

# Polygon client singleton
_polygon_client = None
_polygon_client_lock = threading.Lock()

# Header dates (fixed based on first row in tickers.xlsx)
HEADER_DATES = None

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

def get_previous_trading_days_with_data(ticker, date_str, num_days=50, max_lookback_days=365):
    """
    Get previous trading days (most recent first) confirmed by actual 4:00 AM–8:00 PM ET data.
    This automatically filters non-trading days (market closed).
    """
    target_dt = datetime.strptime(date_str, "%Y-%m-%d")
    days = []  # list of date_str

    for days_back in range(1, max_lookback_days + 1):
        if len(days) >= num_days:
            break

        prev_date = target_dt - timedelta(days=days_back)
        if prev_date.weekday() >= 5:  # Saturday=5, Sunday=6
            continue

        prev_date_str = prev_date.strftime("%Y-%m-%d")
        try:
            high = get_day_high_4am_8pm(ticker, prev_date_str)
            if high is None:
                continue
            days.append(prev_date_str)
        except Exception as e:
            logging.debug(f"Error confirming trading day {ticker} on {prev_date_str}: {e}")
            continue

    return days


def get_day_high_4am_8pm(ticker, date_str):
    """
    Get the highest high from 4:00 AM to 8:00 PM ET for a given day.
    Uses 1-minute bars (unadjusted).
    """
    client = get_polygon_client()

    polygon_limiter.acquire()
    aggs = client.get_aggs(
        ticker=ticker,
        multiplier=1,
        timespan="minute",
        from_=date_str,
        to=date_str,
        adjusted=False  # Unadjusted data - point-in-time prices
    )

    if not aggs:
        return None

    highest_high = None
    for agg in aggs:
        ts = datetime.fromtimestamp(agg.timestamp / 1000, tz=pytz.UTC).astimezone(EASTERN_TZ)
        if ts.hour < 4 or ts.hour > 20:
            continue
        if ts.hour == 20 and ts.minute > 0:
            continue

        high = agg.high
        if high is None:
            continue
        if highest_high is None or high > highest_high:
            highest_high = high

    return highest_high

def process_ticker(row_data):
    """Process a single ticker to get previous 50 days of 4am-8pm highs."""
    row_index, ticker, date_str = row_data

    result = {
        'Ticker': ticker,
        'Date': date_str,
        '_row_index': row_index
    }

    try:
        if not HEADER_DATES:
            logging.error("HEADER_DATES not set; cannot compute columns")
        else:
            for prev_date_str in HEADER_DATES:
                try:
                    high = get_day_high_4am_8pm(ticker, prev_date_str)
                    result[f"High {prev_date_str}"] = high
                except Exception as e:
                    logging.debug(f"Error fetching high for {ticker} on {prev_date_str}: {e}")
                    result[f"High {prev_date_str}"] = None

            logging.info(f"Fetched highs for {len(HEADER_DATES)} days for {ticker} on {date_str}")
    except Exception as e:
        logging.error(f"Error processing {ticker} on {date_str}: {e}")

    return result

def main():
    """Main function to process tickers from tickers.xlsx and fetch highs for previous 50 trading days"""
    script_start_time = time.time()
    
    try:
        # Read tickers from Excel file
        excel_file = 'tickers.xlsx'
        if not os.path.exists(excel_file):
            logging.error(f"Excel file not found: {excel_file}")
            print(f"Error: Excel file not found: {excel_file}")
            return
        
        logging.info(f"Reading tickers from {excel_file}")
        df = pd.read_excel(excel_file)
        
        # Validate required columns
        if 'Ticker' not in df.columns:
            logging.error("'Ticker' column not found in Excel file")
            print("Error: 'Ticker' column not found in Excel file")
            return
        
        if 'Date' not in df.columns:
            logging.error("'Date' column not found in Excel file")
            print("Error: 'Date' column not found in Excel file")
            return
        
        # Reset index to ensure 0-based sequential indexing for order preservation
        df = df.reset_index(drop=True)
        
        logging.info(f"Processing {len(df)} tickers from Excel")
        
        # Prepare data for parallel processing - include ALL rows to maintain order
        ticker_data = []
        for row_index, row in df.iterrows():
            ticker = str(row['Ticker']).strip().upper()
            
            # Handle date - could be datetime or string
            date_val = row['Date']
            if isinstance(date_val, datetime):
                date_str = date_val.strftime("%Y-%m-%d")
            elif isinstance(date_val, pd.Timestamp):
                date_str = date_val.strftime("%Y-%m-%d")
            else:
                date_str = str(date_val).strip()
            
            # Always add to ticker_data to maintain order
            ticker_data.append((row_index, ticker, date_str))
        
        if not ticker_data:
            logging.error("No tickers found")
            print("Error: No tickers found")
            return
        
        # Build fixed header dates from the first row (ensures exactly 50 columns)
        global HEADER_DATES
        if ticker_data:
            _, first_ticker, first_date_str = ticker_data[0]
            HEADER_DATES = get_previous_trading_days_with_data(
                first_ticker, first_date_str, num_days=50, max_lookback_days=365
            )
            if len(HEADER_DATES) < 50:
                logging.warning(
                    f"Header built with only {len(HEADER_DATES)} trading days for {first_ticker} on {first_date_str}"
                )
        else:
            HEADER_DATES = []

        # Process with parallel workers and write rows in input order as they become available
        max_workers = 150
        logging.info(f"Processing {len(ticker_data)} tickers with {max_workers} workers...")

        # Determine filename based on timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"prevhigh_{timestamp}.csv"

        total_rows = 0
        next_index = 0
        pending = {}

        # Fixed header from the first row's trading-day list (50 columns)
        header = ['Ticker', 'Date'] + [f"High {d}" for d in (HEADER_DATES or [])]

        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=header, quoting=csv.QUOTE_ALL, extrasaction="ignore")
            writer.writeheader()
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Create futures with row_index mapping for maintaining order
                futures = {}
                for data in ticker_data:
                    row_index, ticker, date_str = data
                    future = executor.submit(process_ticker, data)
                    futures[future] = row_index

                completed = 0
                for future in as_completed(futures):
                    completed += 1
                    row_index = futures[future]
                    try:
                        result = future.result()
                        if result:
                            pending[row_index] = result
                    except Exception as e:
                        # Create error row for exception cases
                        _, ticker, date_str = ticker_data[row_index]
                        logging.error(f"Error processing row {row_index} ({ticker} on {date_str}): {e}")
                        pending[row_index] = {
                            'Ticker': ticker,
                            'Date': date_str,
                            '_row_index': row_index
                        }

                    # Write any contiguous rows now available
                    while next_index in pending:
                        row = pending.pop(next_index)
                        row.pop('_row_index', None)
                        writer.writerow(row)
                        total_rows += 1
                        next_index += 1

                    # Log progress every 50 tickers
                    if completed % 50 == 0:
                        logging.info(f"Processed {completed}/{len(ticker_data)} tickers...")

        if total_rows == 0:
            logging.warning("No results generated")
            print("No results generated")
            try:
                os.remove(filename)
            except OSError:
                pass
            return

        logging.info(f"Output saved to {filename}")
        logging.info(f"Total records: {total_rows}")
        print(f"Output saved to {filename}")
        print(f"Total records: {total_rows}")
        
        # Calculate and display total time
        end_time = time.time()
        total_seconds = end_time - script_start_time
        total_minutes = total_seconds / 60
        
        if total_minutes >= 1:
            time_str = f"{total_minutes:.2f} minutes ({total_seconds:.1f} seconds)"
        else:
            time_str = f"{total_seconds:.2f} seconds"
        
        logging.info(f"Total execution time: {time_str}")
        print(f"Total execution time: {time_str}")
        
    except Exception as e:
        logging.error(f"Error in main: {e}", exc_info=True)
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
