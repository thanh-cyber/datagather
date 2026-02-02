"""
Volume Minute Fetcher - Fetches minute-by-minute volume data from Polygon.io
Reads tickers from tickers.xlsx and fetches volume data from 4:00 AM to 9:30 AM Eastern Time
Outputs each minute as a column
"""

import pandas as pd
from datetime import datetime
import logging
import time
import threading
import os
import sys
import csv
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from polygon import RESTClient
import urllib3

# Disable urllib3 warnings (including connection pool warnings)
urllib3.disable_warnings()
# Suppress connection pool full warnings - these are informational
import warnings
warnings.filterwarnings('ignore', message='.*Connection pool is full.*')

# Configure logging
log_file = "volume.log"

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

def fetch_volume_data(ticker, date):
    """
    Fetch minute-by-minute volume data from 4:00 AM to 9:30 AM Eastern Time.
    
    Args:
        ticker: Stock ticker symbol
        date: Date string in YYYY-MM-DD format (or None if invalid)
        
    Returns:
        dict with ticker, date, and volume columns for each minute (04:00 to 09:30)
    """
    try:
        # Handle None or invalid date
        if not date:
            logging.error(f"Invalid or missing date for {ticker}: {date}")
            # Return result with None values for all minutes
            base_date = datetime.now().date()
            complete_index = pd.date_range(
                start=pd.Timestamp.combine(base_date, datetime.strptime("04:00:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ),
                end=pd.Timestamp.combine(base_date, datetime.strptime("09:30:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ),
                freq='1min'
            )
            result = {
                'Ticker': ticker,
                'Date': date if date else "ERROR",
                '_row_index': None
            }
            for idx in complete_index:
                minute_str = idx.strftime("%H:%M")
                result[minute_str] = None
            return result
        
        client = get_polygon_client()
        
        # Parse date
        try:
            target_dt = datetime.strptime(date, "%Y-%m-%d")
            base_date = target_dt.date()
        except ValueError:
            logging.error(f"Invalid date format for {ticker}: {date}")
            base_date = datetime.now().date()
            complete_index = pd.date_range(
                start=pd.Timestamp.combine(base_date, datetime.strptime("04:00:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ),
                end=pd.Timestamp.combine(base_date, datetime.strptime("09:30:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ),
                freq='1min'
            )
            result = {
                'Ticker': ticker,
                'Date': date,
                '_row_index': None
            }
            for idx in complete_index:
                minute_str = idx.strftime("%H:%M")
                result[minute_str] = None
            return result
        
        # Fetch minute bars from 4:00 AM to 9:30 AM (unadjusted)
        start_time = target_dt.replace(hour=4, minute=0, second=0, microsecond=0)
        end_time = target_dt.replace(hour=9, minute=30, second=0, microsecond=0)
        
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
        
        # Create complete minute-by-minute index from 4:00 AM to 9:30 AM (inclusive)
        complete_index = pd.date_range(
            start=pd.Timestamp.combine(base_date, datetime.strptime("04:00:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ),
            end=pd.Timestamp.combine(base_date, datetime.strptime("09:30:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ),
            freq='1min'  # 1-minute intervals
        )
        
        # Initialize result dict with ticker and date
        result = {
            'Ticker': ticker,
            'Date': date,
            '_row_index': None  # Will be set later
        }
        
        # Initialize all minute columns with None
        for idx in complete_index:
            minute_str = idx.strftime("%H:%M")
            result[minute_str] = None
        
        if not aggs or len(aggs) == 0:
            logging.warning(f"No volume data found for {ticker} on {date}")
            return result
        
        # Convert to DataFrame
        data = []
        for agg in aggs:
            timestamp = pd.Timestamp(agg.timestamp, unit='ms', tz='UTC').tz_convert(EASTERN_TZ)
            # Only include data from 4:00 AM to 9:30 AM (inclusive)
            if timestamp.hour < 4 or (timestamp.hour == 9 and timestamp.minute > 30):
                continue
            data.append({
                'timestamp': timestamp,
                'volume': agg.volume
            })
        
        if not data:
            return result
        
        df = pd.DataFrame(data)
        df = df.set_index('timestamp').sort_index()
        
        # Floor timestamps to minute boundaries for matching
        df.index = df.index.floor('1min')
        
        # Fill in volume values for each minute
        for idx in complete_index:
            minute_str = idx.strftime("%H:%M")
            if idx in df.index:
                result[minute_str] = int(df.loc[idx, 'volume']) if pd.notna(df.loc[idx, 'volume']) else None
            else:
                result[minute_str] = None
        
        logging.info(f"Fetched volume data for {ticker} on {date}")
        return result
        
    except Exception as e:
        logging.error(f"Error fetching volume data for {ticker} on {date}: {e}")
        # Return result with None values for all minutes
        if date:
            try:
                base_date = datetime.strptime(date, "%Y-%m-%d").date()
            except:
                base_date = datetime.now().date()
        else:
            base_date = datetime.now().date()
        
        complete_index = pd.date_range(
            start=pd.Timestamp.combine(base_date, datetime.strptime("04:00:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ),
            end=pd.Timestamp.combine(base_date, datetime.strptime("09:30:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ),
            freq='1min'
        )
        result = {
            'Ticker': ticker,
            'Date': date if date else "ERROR",
            '_row_index': None
        }
        for idx in complete_index:
            minute_str = idx.strftime("%H:%M")
            result[minute_str] = None
        return result

def process_ticker(row_data):
    """Process a single ticker to get volume data."""
    row_index, ticker, date_str = row_data
    
    # Fetch volume data
    result = fetch_volume_data(ticker, date_str)
    
    # Add row_index for sorting
    result['_row_index'] = row_index
    
    return result

def main():
    """Main function to process tickers from tickers.xlsx and fetch volume data"""
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
        
        # Reset index to ensure 0-based sequential indexing for order preservation
        df = df.reset_index(drop=True)
        
        logging.info(f"Processing {len(df)} ticker-date pairs from Excel")
        
        # Prepare data for parallel processing - include ALL rows to maintain order
        ticker_data = []
        for row_index, row in df.iterrows():
            ticker = str(row['Ticker']).strip().upper()
            date_input = row['Date']
            
            # Handle different date formats - include even invalid ones
            date_str = None
            if pd.isna(date_input):
                logging.warning(f"Row {row_index}: Empty date for {ticker}")
            else:
                # Convert date to string format
                if isinstance(date_input, datetime):
                    date_str = date_input.strftime('%Y-%m-%d')
                elif isinstance(date_input, str):
                    try:
                        # Try parsing various date formats
                        date_obj = pd.to_datetime(date_input)
                        date_str = date_obj.strftime('%Y-%m-%d')
                    except:
                        logging.warning(f"Row {row_index}: Invalid date format for {ticker}: {date_input}")
                else:
                    try:
                        date_obj = pd.to_datetime(date_input)
                        date_str = date_obj.strftime('%Y-%m-%d')
                    except:
                        logging.warning(f"Row {row_index}: Invalid date format for {ticker}: {date_input}")
            
            # Always add to ticker_data to maintain order, even if date is invalid
            ticker_data.append((row_index, ticker, date_str))
        
        if not ticker_data:
            logging.error("No ticker-date pairs found")
            print("Error: No ticker-date pairs found")
            return
        
        # Process with parallel workers
        max_workers = 30
        logging.info(f"Processing {len(ticker_data)} ticker-date pairs with {max_workers} workers...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Create futures with row_index mapping for maintaining order
            futures = {}
            for data in ticker_data:
                row_index, ticker, date_str = data
                future = executor.submit(process_ticker, data)
                futures[future] = row_index
            
            # Store results with row_index for sorting
            results_dict = {}
            
            completed = 0
            for future in as_completed(futures):
                completed += 1
                row_index = futures[future]
                try:
                    result = future.result()
                    if result:
                        results_dict[row_index] = result
                except Exception as e:
                    # Create error row for exception cases - get ticker from ticker_data
                    _, ticker, date_str = ticker_data[row_index]
                    logging.error(f"Error processing row {row_index} ({ticker} on {date_str}): {e}")
                    # Create result with None values for all minutes
                    if date_str:
                        try:
                            base_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                        except:
                            base_date = datetime.now().date()
                    else:
                        base_date = datetime.now().date()
                    
                    complete_index = pd.date_range(
                        start=pd.Timestamp.combine(base_date, datetime.strptime("04:00:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ),
                        end=pd.Timestamp.combine(base_date, datetime.strptime("09:30:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ),
                        freq='1min'
                    )
                    error_result = {
                        'Ticker': ticker,
                        'Date': date_str if date_str else "ERROR",
                        '_row_index': row_index
                    }
                    for idx in complete_index:
                        minute_str = idx.strftime("%H:%M")
                        error_result[minute_str] = None
                    results_dict[row_index] = error_result
                
                # Log progress every 50 tickers
                if completed % 50 == 0:
                    logging.info(f"Processed {completed}/{len(ticker_data)} ticker-date pairs...")
        
        # Ensure all rows are included, even if processing failed
        all_results = []
        
        for row_index in range(len(ticker_data)):
            if row_index in results_dict:
                all_results.append(results_dict[row_index])
            else:
                # Create error row for any missing results
                _, ticker, date_str = ticker_data[row_index]
                
                # Create complete index for error row (use date_str if valid, otherwise use today)
                if date_str:
                    try:
                        base_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    except:
                        base_date = datetime.now().date()
                else:
                    base_date = datetime.now().date()
                
                complete_index = pd.date_range(
                    start=pd.Timestamp.combine(base_date, datetime.strptime("04:00:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ),
                    end=pd.Timestamp.combine(base_date, datetime.strptime("09:30:00", "%H:%M:%S").time()).tz_localize(EASTERN_TZ),
                    freq='1min'
                )
                
                error_result = {
                    'Ticker': ticker,
                    'Date': date_str if date_str else "ERROR",
                    '_row_index': row_index
                }
                for idx in complete_index:
                    minute_str = idx.strftime("%H:%M")
                    error_result[minute_str] = None
                all_results.append(error_result)
        
        if not all_results:
            logging.warning("No results generated")
            print("No results generated")
            return
        
        # Create DataFrame and save to CSV
        output_df = pd.DataFrame(all_results)
        
        # Sort by original row index to maintain exact order from Excel
        output_df = output_df.sort_values(by='_row_index')
        
        # Remove the temporary _row_index column before saving
        output_df = output_df.drop(columns=['_row_index'])
        
        # Determine filename based on date(s) in data with timestamp to avoid overwriting
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Filter out NaN and "ERROR" strings to get valid dates
        valid_dates = output_df['Date'].dropna()
        valid_dates = valid_dates[valid_dates != "ERROR"]
        unique_dates = sorted(valid_dates.unique())
        if len(unique_dates) == 1:
            # Single date - use that date with timestamp
            date_str = unique_dates[0]
            filename = f"volume_{date_str}_{timestamp}.csv"
        elif len(unique_dates) > 1:
            # Multiple dates - use date range with timestamp
            start_date = unique_dates[0]
            end_date = unique_dates[-1]
            filename = f"volume_{start_date}_to_{end_date}_{timestamp}.csv"
        else:
            # Fallback to timestamp only if no dates
            filename = f"volume_{timestamp}.csv"
        
        output_df.to_csv(filename, index=False, quoting=csv.QUOTE_ALL)
        logging.info(f"Output saved to {filename}")
        logging.info(f"Total records: {len(output_df)}")
        print(f"Output saved to {filename}")
        print(f"Total records: {len(output_df)}")
        
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
