"""
OHLCV Data Fetcher - Fetches unadjusted OHLCV data from Polygon.io
Reads tickers from tickers.xlsx and fetches daily OHLCV data for each ticker-date pair
Returns point-in-time prices (not adjusted for splits/dividends)
"""

import pandas as pd
from datetime import datetime
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
# Get script directory for log file
script_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(script_dir, "ohlcv.log")

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

def fetch_ohlcv_data(ticker, date):
    """
    Fetch unadjusted OHLCV data for a ticker on a specific date.
    Returns point-in-time prices (not adjusted for splits/dividends).
    
    Args:
        ticker: Stock ticker symbol
        date: Date string in YYYY-MM-DD format
        
    Returns:
        dict with ticker, date, open, high, low, close, volume, vwap
        or None if error
    """
    try:
        # Handle None or invalid date
        if not date:
            logging.error(f"Invalid or missing date for {ticker}: {date}")
            return {
                'Ticker': ticker,
                'Date': date if date else "ERROR",
                'Open': None,
                'High': None,
                'Low': None,
                'Close': None,
                'Volume': None,
                'VWAP': None
            }
        
        client = get_polygon_client()
        
        # Parse date
        try:
            date_obj = datetime.strptime(date, "%Y-%m-%d")
        except (ValueError, TypeError):
            logging.error(f"Invalid date format for {ticker}: {date}")
            return {
                'Ticker': ticker,
                'Date': date if date else "ERROR",
                'Open': None,
                'High': None,
                'Low': None,
                'Close': None,
                'Volume': None,
                'VWAP': None
            }
        
        # Fetch daily aggregate for the specific date (adjusted=False for point-in-time prices)
        aggs = client.get_aggs(
            ticker=ticker,
            multiplier=1,
            timespan="day",
            from_=date,
            to=date,
            adjusted=False  # Unadjusted data - returns point-in-time prices
        )
        
        if not aggs or len(aggs) == 0:
            logging.warning(f"No OHLCV data found for {ticker} on {date}")
            return {
                'Ticker': ticker,
                'Date': date,
                'Open': None,
                'High': None,
                'Low': None,
                'Close': None,
                'Volume': None,
                'VWAP': None
            }
        
        # Get the first (and only) aggregate for the date
        agg = aggs[0]
        
        return {
            'Ticker': ticker,
            'Date': date,
            'Open': agg.open,
            'High': agg.high,
            'Low': agg.low,
            'Close': agg.close,
            'Volume': agg.volume,
            'VWAP': agg.vwap if hasattr(agg, 'vwap') and agg.vwap else None
        }
        
    except Exception as e:
        logging.error(f"Error fetching OHLCV data for {ticker} on {date}: {e}")
        return {
            'Ticker': ticker,
            'Date': date,
            'Open': None,
            'High': None,
            'Low': None,
            'Close': None,
            'Volume': None,
            'VWAP': None
        }

def process_ticker_date_pair(row_data):
    """Process a single ticker-date pair."""
    row_index, ticker, date_str = row_data
    
    # Fetch OHLCV data (always returns a dict, never None)
    result = fetch_ohlcv_data(ticker, date_str)
    
    # Always add row_index to result for sorting (fetch_ohlcv_data always returns a dict)
    result['_row_index'] = row_index
    
    if result.get('Open') is not None or result.get('Close') is not None:
        logging.info(f"Fetched OHLCV for {ticker} on {date_str}")
    
    return result

def main():
    """Main function to process tickers from tickers.xlsx and fetch OHLCV data"""
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
        
        all_results = []
        
        # Process with parallel workers
        max_workers = 20
        logging.info(f"Processing {len(ticker_data)} ticker-date pairs with {max_workers} workers...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Create futures with row_index mapping for maintaining order
            futures = {}
            for data in ticker_data:
                row_index, ticker, date_str = data
                future = executor.submit(process_ticker_date_pair, data)
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
                    # Create error row for exception cases - get ticker/date from ticker_data
                    _, ticker, date_str = ticker_data[row_index]
                    logging.error(f"Error processing row {row_index} ({ticker} on {date_str}): {e}")
                    results_dict[row_index] = {
                        'Ticker': ticker,
                        'Date': date_str if date_str else "ERROR",
                        'Open': None,
                        'High': None,
                        'Low': None,
                        'Close': None,
                        'Volume': None,
                        'VWAP': None,
                        '_row_index': row_index
                    }
                
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
                all_results.append({
                    'Ticker': ticker,
                    'Date': date_str if date_str else "ERROR",
                    'Open': None,
                    'High': None,
                    'Low': None,
                    'Close': None,
                    'Volume': None,
                    'VWAP': None,
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
        
        # Determine filename based on date(s) in data with timestamp to avoid overwriting
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Filter out NaN and "ERROR" strings to get valid dates
        valid_dates = output_df['Date'].dropna()
        valid_dates = valid_dates[valid_dates != "ERROR"]
        unique_dates = sorted(valid_dates.unique())
        if len(unique_dates) == 1:
            # Single date - use that date with timestamp
            date_str = unique_dates[0]
            filename = f"OHLCV_{date_str}_{timestamp}.csv"
        elif len(unique_dates) > 1:
            # Multiple dates - use date range with timestamp
            start_date = unique_dates[0]
            end_date = unique_dates[-1]
            filename = f"OHLCV_{start_date}_to_{end_date}_{timestamp}.csv"
        else:
            # Fallback to timestamp only if no dates
            filename = f"OHLCV_{timestamp}.csv"
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_file = os.path.join(script_dir, filename)
        
        output_df.to_csv(output_file, index=False, quoting=csv.QUOTE_ALL)
        logging.info(f"Output saved to {output_file}")
        logging.info(f"Total records: {len(output_df)}")
        print(f"Output saved to {output_file}")
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
