import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import pytz
import time
import threading
import os
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from polygon import RESTClient
import sys

# Configure logging (file only)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    handlers=[logging.FileHandler("pmhighopen.log")]
)

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
                time.sleep(self.period - (now - self.calls[0]))
            self.calls.append(now)

# Load environment variables
load_dotenv()

# API keys
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
POLYGON_PREMIUM = os.getenv("POLYGON_PREMIUM", "true").lower() == "true"  # Default to premium

# Rate limiter for Polygon Premium (200 calls per second)
polygon_limiter = RateLimiter(max_calls=200, period=1.0)
logging.info(f"Polygon rate limit: 200 calls per second (Premium)")

# Eastern Timezone
EASTERN_TZ = pytz.timezone('US/Eastern')

# Polygon client singleton
_polygon_client = None

def get_polygon_client():
    global _polygon_client
    if _polygon_client is None:
        if not POLYGON_API_KEY:
            raise ValueError("POLYGON_API_KEY not found in environment variables")
        _polygon_client = RESTClient(POLYGON_API_KEY, num_pools=100, retries=3)
    return _polygon_client

def get_pm_high_open(ticker, date):
    """
    Get PM High Time and the OPEN price of the 1-minute candle at that time.
    Returns: (pm_high_open, pm_high_time) or (None, None) on error
    """
    client = get_polygon_client()
    
    try:
        start_date = datetime.strptime(date, "%Y-%m-%d")
        end_date = start_date + timedelta(days=1)
        
        pre_market_start = pd.Timestamp(f"{date} 04:00:00").tz_localize(EASTERN_TZ)
        pre_market_end = pd.Timestamp(f"{date} 09:29:59").tz_localize(EASTERN_TZ)
        
        # Fetch intraday data for the target date (rate limit the API call)
        polygon_limiter.acquire()
        aggs = client.get_aggs(
            ticker,
            multiplier=1,
            timespan="minute",
            from_=start_date.strftime("%Y-%m-%d"),
            to=end_date.strftime("%Y-%m-%d"),
            adjusted=True
        )
        
        if not aggs:
            logging.warning(f"No intraday data for {ticker} on {date}")
            return None, None
        
        # Convert to DataFrame
        data = pd.DataFrame([{
            'timestamp': agg.timestamp,
            'open': agg.open,
            'high': agg.high,
            'low': agg.low,
            'close': agg.close,
            'volume': agg.volume
        } for agg in aggs])
        
        if data.empty:
            logging.warning(f"Empty DataFrame for {ticker} on {date}")
            return None, None
        
        # Convert timestamp to datetime with timezone
        data['datetime'] = pd.to_datetime(data['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(EASTERN_TZ)
        data.set_index('datetime', inplace=True)
        data = data[~data.index.duplicated(keep='first')]
        
        # Filter to pre-market hours (04:00 to 09:29:59)
        pre_market = data[(data.index >= pre_market_start) & (data.index <= pre_market_end)].copy()
        
        if pre_market.empty:
            logging.warning(f"No pre-market data for {ticker} on {date}")
            return None, None
        
        # Find when the maximum HIGH occurred
        pm_high_idx = pre_market["high"].idxmax()
        pm_high_time = pm_high_idx if not pd.isna(pm_high_idx) else None
        
        if pm_high_time is None:
            logging.warning(f"Could not determine PM High Time for {ticker} on {date}")
            return None, None
        
        # Round down PM High Time to the nearest minute (e.g., 08:00:15 -> 08:00:00)
        # This gives us the minute candle that contains the PM High Time
        pm_high_time_minute = pm_high_time.replace(second=0, microsecond=0)
        pm_high_time_str = pm_high_time.strftime("%H:%M:%S")
        
        # First, try to find the exact bar at the rounded minute timestamp
        # In Polygon minute aggregates, bars are timestamped at the start of the minute
        if pm_high_time_minute in pre_market.index:
            pm_high_open = pre_market.loc[pm_high_time_minute, "open"]
            if not pd.isna(pm_high_open):
                return round(float(pm_high_open), 4), pm_high_time_str
        
        # If exact match not found, find all bars that match the minute (hour:minute) and sort by timestamp
        matching_bars = pre_market[pre_market.index.time == pm_high_time_minute.time()].sort_index()
        
        if not matching_bars.empty:
            # Use the first (earliest) bar of that minute (the actual minute candle open)
            # In Polygon minute aggregates, bars are timestamped at the start of the minute
            # So the first bar should be the minute candle
            pm_high_open = matching_bars.iloc[0]["open"]
            
            if pd.isna(pm_high_open):
                logging.warning(f"PM High Open is NaN for {ticker} on {date} at {pm_high_time_str}")
                return None, None
            
            # Log if we found multiple bars in the same minute (shouldn't happen normally)
            if len(matching_bars) > 1:
                logging.warning(f"Multiple bars found for minute {pm_high_time_minute.strftime('%H:%M:%S')} for {ticker} on {date}: using first bar at {matching_bars.index[0].strftime('%H:%M:%S')}")
            
            return round(float(pm_high_open), 4), pm_high_time_str
        else:
            # Fallback: try to find the closest bar within the same minute
            # Look for bars within 1 minute of the rounded time
            time_diff = abs(pre_market.index - pm_high_time_minute)
            closest_idx = time_diff.argmin()
            closest_time = pre_market.index[closest_idx]
            
            # Only use if within 1 minute and same hour:minute
            time_diff_seconds = abs((closest_time - pm_high_time_minute).total_seconds())
            if time_diff_seconds <= 60 and closest_time.time().replace(second=0) == pm_high_time_minute.time():
                pm_high_open = pre_market.iloc[closest_idx]["open"]
                if not pd.isna(pm_high_open):
                    logging.warning(f"Using closest bar for {ticker} at {date}: PM High Time={pm_high_time_str}, using bar at {closest_time.strftime('%H:%M:%S')}")
                    return round(float(pm_high_open), 4), pm_high_time_str
            
            logging.warning(f"PM High Time minute {pm_high_time_minute.strftime('%H:%M:%S')} not found in pre_market data for {ticker} on {date}")
            return None, None
            
    except Exception as e:
        logging.error(f"Error getting PM High Open for {ticker} on {date}: {e}")
        return None, None

def process_ticker(ticker, date, excel_row_index):
    """Process a single ticker-date pair and return result with Excel row index."""
    try:
        pm_high_open, pm_high_time = get_pm_high_open(ticker, date)
        
        return {
            "Ticker": ticker,
            "Date": date,
            "PM High Open": round(float(pm_high_open), 4) if pm_high_open is not None else "Not Available",
            "PM High Time": pm_high_time if pm_high_time else "Not Available",
            "_excel_row_index": excel_row_index
        }
    except Exception as e:
        logging.error(f"Error processing {ticker} on {date}: {e}")
        return {
            "Ticker": ticker,
            "Date": date,
            "PM High Open": "Not Available",
            "PM High Time": "Not Available",
            "_excel_row_index": excel_row_index,
            "Error": str(e)
        }

def read_excel_file(excel_filename):
    """Read Excel file and return list of (ticker, date, excel_row_index) tuples."""
    try:
        df = pd.read_excel(excel_filename)
        ticker_date_pairs = []
        
        for row_index, row in df.iterrows():
            ticker = str(row['Ticker']).strip().upper()
            date_str = str(row['Date']).strip()
            
            # Parse date - handle multiple formats
            try:
                # Try YYYY-MM-DD first
                date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
            except ValueError:
                try:
                    # Try DD/MM/YYYY
                    date = datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
                except ValueError:
                    try:
                        # Try MM/DD/YYYY
                        date = datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d")
                    except ValueError:
                        # Fallback to pandas parsing
                        try:
                            date = pd.to_datetime(date_str).strftime("%Y-%m-%d")
                        except:
                            logging.warning(f"Invalid date format for {ticker}: {date_str}")
                            continue
            
            ticker_date_pairs.append((ticker, date, row_index))
        
        return ticker_date_pairs
    except Exception as e:
        logging.error(f"Error reading Excel file {excel_filename}: {e}")
        return []

def main():
    """Main function to process tickers from Excel files."""
    script_start_time = time.time()
    
    # Get Excel filename from command line arguments
    excel_filename = None
    if len(sys.argv) > 1:
        first_arg = sys.argv[1]
        # Support tickers, tickers2, tickers3 with or without .xlsx extension
        if first_arg.lower() in ['tickers', 'tickers2', 'tickers3']:
            excel_filename = f"{first_arg}.xlsx"
        elif first_arg.endswith('.xlsx'):
            excel_filename = first_arg
        else:
            excel_filename = f"{first_arg}.xlsx"
    else:
        # Default to tickers.xlsx
        excel_filename = 'tickers.xlsx'
    
    if not os.path.exists(excel_filename):
        logging.error(f"Excel file not found: {excel_filename}")
        print(f"Error: Excel file not found: {excel_filename}")
        return
    
    logging.info(f"Reading from Excel file: {excel_filename}")
    print(f"Reading from Excel file: {excel_filename}")
    
    # Read Excel file
    ticker_date_pairs = read_excel_file(excel_filename)
    
    if not ticker_date_pairs:
        logging.error("No ticker-date pairs found in Excel file")
        print("Error: No ticker-date pairs found in Excel file")
        return
    
    total_pairs = len(ticker_date_pairs)
    logging.info(f"Processing {total_pairs} ticker-date pairs")
    print(f"Processing {total_pairs} ticker-date pairs with 20 workers...")
    
    # Process with 20 workers
    workers = 20
    all_results = []
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Submit all tasks and store futures in order with ticker/date for error handling
        futures = []
        for ticker, date, excel_row_index in ticker_date_pairs:
            future = executor.submit(process_ticker, ticker, date, excel_row_index)
            futures.append((future, ticker, date, excel_row_index))
        
        completed_count = 0
        progress_interval = max(1, min(50, total_pairs // 20))
        
        # Process results in the same order as input (not completion order)
        for i, (future, ticker, date, excel_row_index) in enumerate(futures, 1):
            try:
                result = future.result()
                if result:
                    all_results.append(result)
            except Exception as e:
                logging.error(f"Error processing {ticker} on {date} at index {excel_row_index}: {e}")
                all_results.append({
                    "Ticker": ticker,
                    "Date": date,
                    "PM High Open": "Not Available",
                    "PM High Time": "Not Available",
                    "_excel_row_index": excel_row_index,
                    "Error": str(e)
                })
            
            completed_count += 1
            if completed_count % progress_interval == 0:
                logging.info(f"Processed {completed_count}/{total_pairs} pairs")
                print(f"Progress: {completed_count}/{total_pairs} pairs processed")
    
    if not all_results:
        logging.error("No results generated")
        print("Error: No results generated")
        return
    
    # Convert to DataFrame
    output_df = pd.DataFrame(all_results)
    
    # Sort by Excel row index to maintain original Excel order
    if "_excel_row_index" in output_df.columns:
        output_df = output_df.sort_values("_excel_row_index").reset_index(drop=True)
        # Remove the internal Excel row index column
        output_df = output_df.drop(columns=["_excel_row_index"])
    
    # Format Date to d/m/y (handle invalid dates gracefully)
    def format_date(date_val):
        if pd.isna(date_val) or date_val == "Unknown" or date_val == "Invalid Date":
            return date_val
        try:
            return pd.to_datetime(date_val).strftime('%d/%m/%Y')
        except:
            return date_val
    
    output_df['Date'] = output_df['Date'].apply(format_date)
    
    # Remove Error column if it exists (internal use only)
    if "Error" in output_df.columns:
        output_df = output_df.drop(columns=["Error"])
    
    # Reorder columns: Ticker, Date, PM High Open, PM High Time
    output_df = output_df.reindex(columns=["Ticker", "Date", "PM High Open", "PM High Time"])
    
    # Create output filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    base_name = excel_filename.replace('.xlsx', '')
    output_filename = f"{base_name}_pmhighopen_{timestamp}.xlsx"
    
    # Write to Excel
    output_df.to_excel(output_filename, index=False)
    
    total_time = time.time() - script_start_time
    pairs_per_second = total_pairs / total_time if total_time > 0 else 0
    
    logging.info(f"Processed {len(all_results)} of {total_pairs} pairs in {total_time/60:.1f} minutes ({pairs_per_second:.1f} pairs/sec)")
    print(f"\n[SUCCESS] Processed {len(all_results)} of {total_pairs} pairs in {total_time/60:.1f} minutes ({pairs_per_second:.1f} pairs/sec)")
    print(f"Output saved to: {output_filename}")

if __name__ == "__main__":
    main()

