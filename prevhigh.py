"""
Previous High Fetcher - Fetches high prices from previous 5 trading days from Polygon.io
Reads tickers and dates from tickers.xlsx and fetches the high for each of the 5 previous trading days
Returns unadjusted highs (point-in-time prices)
Output columns: Ticker, Date, then one column per trading day (column name = date, value = high)
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

def get_previous_highs(ticker, date):
    """
    Get highs from previous 5 trading days (weekend-aware, unadjusted).
    Looks back to find 5 trading days and returns the high and date for each.
    
    Args:
        ticker: Stock ticker symbol
        date: Date string in YYYY-MM-DD format
        
    Returns:
        List of tuples [(date_str, high), ...] for up to 5 trading days, ordered most recent first.
        Returns empty list if error.
    """
    try:
        target_dt = datetime.strptime(date, "%Y-%m-%d")
        client = get_polygon_client()
        
        # Find 5 trading days before target date
        # Look back up to 14 days to account for weekends and holidays
        trading_days_found = 0
        highs = []  # List of (date_str, high) tuples
        
        for days_back in range(1, 15):
            if trading_days_found >= 5:
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
                    adjusted=False  # Unadjusted data - returns point-in-time prices
                )
                
                if prev_aggs and len(prev_aggs) > 0:
                    high = prev_aggs[0].high
                    if high > 0 and high < 50000:  # Sanity check
                        highs.append((prev_date_str, high))
                        trading_days_found += 1
            except Exception as e:
                logging.debug(f"Error fetching high for {ticker} on {prev_date_str}: {e}")
                continue
        
        return highs
    except Exception as e:
        logging.error(f"Error in get_previous_highs for {ticker} on {date}: {e}")
        return []

def process_ticker(row_data):
    """Process a single ticker to get previous 5 days of highs."""
    row_index, ticker, date_str = row_data
    
    # Fetch previous highs (list of (date, high) tuples)
    previous_highs = get_previous_highs(ticker, date_str)
    
    result = {
        'Ticker': ticker,
        'Date': date_str,
        '_row_index': row_index
    }
    
    # Add columns for each day - column name is the date, value is the high
    for day_date, day_high in previous_highs:
        result[day_date] = day_high
    
    if previous_highs:
        logging.info(f"Fetched {len(previous_highs)} days of highs for {ticker} on {date_str}")
    else:
        logging.warning(f"No previous highs found for {ticker} on {date_str}")
    
    return result

def main():
    """Main function to process tickers from tickers.xlsx and fetch highs for each of the previous 5 trading days"""
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
        
        # Process with parallel workers
        max_workers = 30
        logging.info(f"Processing {len(ticker_data)} tickers with {max_workers} workers...")
        
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
                    results_dict[row_index] = {
                        'Ticker': ticker,
                        'Date': date_str,
                        '_row_index': row_index
                    }
                
                # Log progress every 50 tickers
                if completed % 50 == 0:
                    logging.info(f"Processed {completed}/{len(ticker_data)} tickers...")
        
        # Ensure all rows are included, even if processing failed
        all_results = []
        for row_index in range(len(ticker_data)):
            if row_index in results_dict:
                all_results.append(results_dict[row_index])
            else:
                # Create error row for any missing results
                _, ticker, date_str = ticker_data[row_index]
                all_results.append({
                    'Ticker': ticker,
                    'Date': date_str,
                    '_row_index': row_index
                })
        
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
        
        # Determine filename based on timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"prevhigh_{timestamp}.csv"
        
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
