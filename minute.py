import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import pytz
import time
import os
from dotenv import load_dotenv
from polygon import RESTClient
import requests
import sys
from typing import Optional
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    handlers=[logging.FileHandler("minute.log")]
)

# Load environment variables
load_dotenv()

# API key
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

# Timezone setup
EASTERN_TZ = pytz.timezone('US/Eastern')

# Rate limiter for API calls
class RateLimiter:
    def __init__(self, max_calls: int, period: float):
        self.max_calls = max_calls
        self.period = period
        self.calls = []

    def acquire(self):
        now = time.time()
        while self.calls and self.calls[0] <= now - self.period:
            self.calls.pop(0)
        if len(self.calls) >= self.max_calls:
            time.sleep(self.period - (now - self.calls[0]))
        self.calls.append(now)

# Global rate limiter - 200 calls per second for parallel processing
polygon_limiter = RateLimiter(max_calls=200, period=1.0)

# Thread-safe Polygon client creation
def get_polygon_client():
    """Create a new Polygon client instance for each thread."""
    return RESTClient(POLYGON_API_KEY)

def parse_date(date_input):
    """
    Parse date from various formats (Excel can export dates in different formats)
    Returns datetime object or None if parsing fails
    Handles: datetime objects, pandas Timestamp, date strings in various formats
    """
    if date_input is None:
        return None
    
    # Handle pandas Timestamp objects
    if hasattr(date_input, 'to_pydatetime'):
        return date_input.to_pydatetime()
    
    # Handle datetime objects directly
    if isinstance(date_input, datetime):
        return date_input
    
    # Handle date objects (convert to datetime)
    if hasattr(date_input, 'year') and hasattr(date_input, 'month') and hasattr(date_input, 'day'):
        if not isinstance(date_input, datetime):
            return datetime.combine(date_input, datetime.min.time())
        return date_input
    
    # Handle string dates
    date_str = str(date_input).strip()
    
    # Remove time part if present
    date_str_clean = date_str.split(' ')[0]
    
    # Try multiple date formats - prioritize D/M/Y format first
    date_formats = [
        '%d/%m/%Y',     # "24/10/2025" (D/M/Y format - prioritized)
        '%d/%m/%y',     # "24/10/25" (D/M/Y with 2-digit year)
        '%d-%m-%Y',     # "24-10-2025" (D-M-Y format)
        '%d-%m-%y',     # "24-10-25" (D-M-Y with 2-digit year)
        '%Y-%m-%d',     # "2025-10-24" (ISO format)
        '%m/%d/%Y',     # "10/24/2025" (US M/D/Y format)
        '%Y/%m/%d',     # "2025/10/24"
        '%b %d, %Y',    # "Oct 24, 2025"
        '%d %b %Y',     # "24 Oct 2025"
    ]
    
    for date_format in date_formats:
        try:
            return datetime.strptime(date_str_clean, date_format)
        except ValueError:
            continue
    
    # If all formats fail, log and return None
    logging.warning(f"Could not parse date: {date_input} (type: {type(date_input)})")
    return None

def get_raw_intraday_data(ticker, date, api_key):
    """
    Get raw intraday data from Polygon API for minute-by-minute data using Polygon client
    Matches stock_data_pm.py approach EXACTLY with separate API call for target date
    """
    try:
        # Parse date from various formats
        date_obj = parse_date(date)
        if date_obj is None:
            logging.warning(f"Invalid date format for {ticker}: {date}")
            return None
        
        date_str = date_obj.strftime('%Y-%m-%d')
        
        client = get_polygon_client()
        
        # Make separate API call for the target date like stock_data_pm.py does
        try:
            polygon_limiter.acquire()
            aggs = client.get_aggs(
                ticker,
                multiplier=1,
                timespan="minute",
                from_=date_str,
                to=date_str,
                adjusted=True
            )
            
            if not aggs:
                logging.debug(f"No Polygon data for {ticker} on {date_str}")
                return None
            
            # Create DataFrame exactly like stock_data_pm.py
            df = pd.DataFrame([
                {
                    "timestamp": getattr(agg, 'timestamp', None),
                    "open": getattr(agg, 'open', None),
                    "high": getattr(agg, 'high', None),
                    "low": getattr(agg, 'low', None),
                    "close": getattr(agg, 'close', None),
                    "volume": getattr(agg, 'volume', None)
                }
                for agg in aggs if hasattr(agg, 'timestamp')
            ])
            
            if df.empty:
                logging.debug(f"Empty DataFrame for {ticker} on {date_str}")
                return None
            
            # Process timestamps exactly like stock_data_pm.py
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", errors="coerce")
            
            # Check for null timestamps
            if df["timestamp"].isnull().any():
                logging.warning(f"Invalid timestamps for {ticker}")
                df = df.dropna(subset=["timestamp"])
            
            # Convert to datetime index with timezone
            df["datetime"] = df["timestamp"].dt.tz_localize("UTC").dt.tz_convert(EASTERN_TZ)
            df.set_index("datetime", inplace=True)
            
            # Add time column for filtering
            df['time'] = df.index.time
            
            logging.info(f"Fetched {len(df)} bars for {ticker} on {date_str}")
            return df
            
        except Exception as e:
            logging.warning(f"Polygon data fetch failed for {ticker}: {e}")
            return None
        
        # YFinance fallback if Polygon fails (matches stock_data_pm.py)
        if df is None or df.empty:
            logging.warning(f"No Polygon.io data for {ticker} on {date_str}, falling back to yfinance which may not be split-adjusted")
            try:
                yf_ticker = yf.Ticker(ticker)
                polygon_limiter.acquire()
                # Use same parameters as stock_data_pm.py
                hist = yf_ticker.history(interval="1m", start=date_str, auto_adjust=True)
                if not hist.empty:
                    df = hist[["Open", "High", "Low", "Close", "Volume"]].reset_index()
                    df.columns = ["timestamp", "open", "high", "low", "close", "volume"]
                    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
                    if df["timestamp"].dt.tz:
                        df["datetime"] = df["timestamp"].dt.tz_convert(EASTERN_TZ)
                    else:
                        df["datetime"] = df["timestamp"].dt.tz_localize(EASTERN_TZ)
                    df.set_index("datetime", inplace=True)
                    df['time'] = df.index.time
                    logging.info(f"Fetched {len(df)} yfinance records for {ticker} on {date_str}")
                    return df
                else:
                    logging.warning(f"No yfinance intraday data for {ticker} on {date_str}")
                    return None
            except Exception as e:
                logging.error(f"Error fetching yfinance intraday data for {ticker}: {e}")
                return None
        
    except Exception as e:
        logging.debug(f"Error getting raw intraday data for {ticker}: {e}")
        return None

def filter_minute_data(df, start_time="04:00:00", end_time="20:00:00"):
    """
    Filter minute data to only include data between start_time and end_time (Eastern Time)
    """
    if df is None or df.empty:
        return None
    
    try:
        start_time_obj = datetime.strptime(start_time, "%H:%M:%S").time()
        end_time_obj = datetime.strptime(end_time, "%H:%M:%S").time()
        
        # Filter data between start and end times
        filtered_df = df[(df['time'] >= start_time_obj) & (df['time'] <= end_time_obj)].copy()
        
        if filtered_df.empty:
            logging.debug(f"No data found between {start_time} and {end_time}")
            return None
            
        return filtered_df
        
    except Exception as e:
        logging.error(f"Error filtering minute data: {e}")
        return None

def get_previous_close_price(ticker, date):
    """
    Get the previous trading day's close price for forward fill at 4:00 AM
    """
    try:
        # Parse date from various formats
        date_obj = parse_date(date)
        if date_obj is None:
            logging.warning(f"Invalid date format for {ticker}: {date}")
            return None
        
        date = date_obj
        
        # Try to get previous trading day's close from Polygon
        client = get_polygon_client()
        
        # Check up to 7 days back to find the last trading day
        for days_back in range(1, 8):
            check_date = (date - timedelta(days=days_back)).strftime('%Y-%m-%d')
            
            try:
                polygon_limiter.acquire()
                aggs = client.get_aggs(
                    ticker,
                    multiplier=1,
                    timespan="day",
                    from_=check_date,
                    to=check_date,
                    adjusted=True
                )
                
                if aggs and len(aggs) > 0:
                    prev_close = aggs[0].close
                    logging.info(f"Found previous close for {ticker} on {check_date}: {prev_close}")
                    return prev_close
                    
            except Exception as e:
                logging.debug(f"No Polygon daily data for {ticker} on {check_date}: {e}")
                continue
        
        # Fallback to yfinance if Polygon fails
        logging.warning(f"No previous close found via Polygon for {ticker}, trying yfinance")
        try:
            yf_ticker = yf.Ticker(ticker)
            # Get last 10 days to find most recent close
            hist = yf_ticker.history(period="10d", interval="1d")
            if not hist.empty:
                prev_close = hist['Close'].iloc[-1]
                logging.info(f"Found previous close via yfinance for {ticker}: {prev_close}")
                return prev_close
        except Exception as e:
            logging.warning(f"yfinance fallback failed for {ticker}: {e}")
        
        logging.warning(f"No previous close price found for {ticker}")
        return None
        
    except Exception as e:
        logging.error(f"Error getting previous close for {ticker}: {e}")
        return None

def determine_failure_reason(ticker, date):
    """
    Determine the specific reason why minute data processing failed for a ticker
    """
    try:
        # Parse date first to ensure it's in correct format
        date_obj = parse_date(date)
        if date_obj is None:
            return f"Invalid date format: {date}"
        
        date_str = date_obj.strftime('%Y-%m-%d')
        
        # Try to get raw data to determine failure reason
        raw_data = get_raw_intraday_data(ticker, date_str, POLYGON_API_KEY)
        
        if raw_data is None:
            return "No raw data available (API failure or invalid ticker)"
        
        # Check if we have any data
        if raw_data.empty:
            return "Empty raw data (no trades on this date)"
        
        # Filter to 4AM-8PM to see if we have data in the target range
        minute_data = filter_minute_data(raw_data, "04:00:00", "20:00:00")
        
        if minute_data is None:
            return "No data in 4AM-8PM range (outside trading hours)"
        
        if minute_data.empty:
            return "Empty data in 4AM-8PM range (no trades during market hours)"
        
        # If we get here, there might be an issue with the processing logic itself
        return "Processing logic error (data available but processing failed)"
        
    except Exception as e:
        return f"Exception during processing: {str(e)[:100]}"  # Truncate long error messages

def process_ticker_minute_data(ticker, date):
    """
    Process minute data for a single ticker and date - output in wide format with each minute as a column
    Uses forward fill like stock_data_pm.py for minutes with no trades
    Enhanced: Uses yesterday's close price if no price at 4:00 AM
    """
    try:
        logging.info(f"Processing minute data for {ticker} on {date}")
        
        # Get raw intraday data
        raw_data = get_raw_intraday_data(ticker, date, POLYGON_API_KEY)
        
        if raw_data is None:
            logging.warning(f"No raw data available for {ticker} on {date}")
            return None
        
        # Filter to 4AM-8PM Eastern Time
        minute_data = filter_minute_data(raw_data, "04:00:00", "20:00:00")
        
        if minute_data is None:
            logging.warning(f"No minute data available for {ticker} on {date} between 4AM-8PM")
            return None
        
        # Create wide format with forward fill (matches stock_data_pm.py)
        result = {'Ticker': ticker, 'Date': date}
        
        # Create time range from 04:00 to 20:00 (like stock_data_pm.py)
        start_time = datetime.strptime('04:00', '%H:%M').time()
        end_time = datetime.strptime('20:00', '%H:%M').time()
        
        # Get the date from the data index
        base_date = minute_data.index[0].date()
        
        # Get previous close price for 4:00 AM fallback
        previous_close = get_previous_close_price(ticker, date)
        
        # Forward fill logic (enhanced with previous close fallback)
        last_price = None
        current_time = datetime.combine(base_date, start_time)
        end_datetime = datetime.combine(base_date, end_time)
        
        while current_time <= end_datetime:
            time_key = current_time.strftime('%H:%M')
            
            # Find bar for this minute in the DataFrame
            matching_bars = minute_data[minute_data.index.time == current_time.time()]
            
            if not matching_bars.empty:
                result[time_key] = matching_bars['close'].iloc[-1]  # Use last bar if multiple
                last_price = matching_bars['close'].iloc[-1]
            else:
                # No trade this minute, use last price (forward fill)
                if last_price is not None:
                    result[time_key] = last_price
                else:
                    # No data yet - use previous close if available (especially for 4:00 AM)
                    if previous_close is not None:
                        result[time_key] = previous_close
                        last_price = previous_close
                        logging.info(f"Using previous close {previous_close} for {ticker} at {time_key}")
                    else:
                        result[time_key] = 'Not Available'  # No data available
            
            current_time += timedelta(minutes=1)
        
        logging.info(f"Successfully processed minute data for {ticker} on {date} with forward fill and previous close fallback")
        logging.info(f"Result dictionary has {len(result)} keys (Ticker, Date, plus {len(result)-2} minute columns)")
        return result
        
    except Exception as e:
        logging.error(f"Error processing minute data for {ticker} on {date}: {e}")
        return None

def process_single_ticker(row_data):
    """Process a single ticker-date pair - designed for parallel execution"""
    row_index, ticker, date_str = row_data
    
    # Always create a result row to maintain exact order from Excel
    result_row = {'Ticker': ticker, 'Date': date_str, '__input_order': row_index}
    failure_reason = None
    
    try:
        # Process minute data for this ticker-date pair
        minute_data = process_ticker_minute_data(ticker, date_str)
        
        if minute_data is not None:
            # Add successful minute data to result row
            for key, value in minute_data.items():
                if key not in ['Ticker', 'Date']:  # Don't overwrite Ticker/Date
                    result_row[key] = value
            logging.info(f"Successfully processed {ticker} on {date_str}")
        else:
            # Determine failure reason
            failure_reason = determine_failure_reason(ticker, date_str)
            
            # Fill with "Not Available" for failed processing to maintain order
            logging.warning(f"Failed to process {ticker} on {date_str} - {failure_reason}")
            # Create empty minute columns from 04:00 to 20:00
            start_time = datetime.strptime('04:00', '%H:%M').time()
            end_time = datetime.strptime('20:00', '%H:%M').time()
            current_time = datetime.combine(datetime.today().date(), start_time)
            end_datetime = datetime.combine(datetime.today().date(), end_time)
            
            while current_time <= end_datetime:
                time_key = current_time.strftime('%H:%M')
                result_row[time_key] = 'Not Available'  # Not Available for failed processing
                current_time += timedelta(minutes=1)
        
        # Add failure reason column (appears after 20:00)
        result_row['Failure Reason'] = failure_reason if failure_reason else 'Success'
        
        return result_row
        
    except Exception as e:
        logging.error(f"Error processing {ticker} on {date_str}: {e}")
        # Return failed result row
        result_row['Failure Reason'] = f"Processing exception: {str(e)[:100]}"
        # Fill with Not Available
        start_time = datetime.strptime('04:00', '%H:%M').time()
        end_time = datetime.strptime('20:00', '%H:%M').time()
        current_time = datetime.combine(datetime.today().date(), start_time)
        end_datetime = datetime.combine(datetime.today().date(), end_time)
        
        while current_time <= end_datetime:
            time_key = current_time.strftime('%H:%M')
            result_row[time_key] = 'Not Available'
            current_time += timedelta(minutes=1)
        
        return result_row

def main():
    """Main function to process tickers from tickerminute.xlsx and output minute data with parallel processing"""
    script_start_time = time.time()
    
    try:
        # Read tickers from Excel file
        logging.info("Reading tickers from tickerminute.xlsx")
        df = pd.read_excel('tickerminute.xlsx')
        
        logging.info(f"Processing {len(df)} ticker-date pairs from Excel with 25 parallel workers")
        
        # Prepare data for parallel processing
        ticker_data = []
        for row_index, row in df.iterrows():
            ticker = str(row['Ticker']).strip().upper()
            date_input = row['Date']
            
            # Parse date and convert to standard format
            date_obj = parse_date(date_input)
            if date_obj is None:
                logging.warning(f"Skipping row {row_index}: Invalid date format for {ticker}: {date_input}")
                continue
            
            date_str = date_obj.strftime('%Y-%m-%d')
            ticker_data.append((row_index, ticker, date_str))
        
        all_minute_data = []
        
        # Process with 25 parallel workers
        with ThreadPoolExecutor(max_workers=25) as executor:
            # Submit all tasks
            future_to_ticker = {executor.submit(process_single_ticker, data): data for data in ticker_data}
            
            # Collect results as they complete
            completed_count = 0
            for future in as_completed(future_to_ticker):
                try:
                    result = future.result()
                    all_minute_data.append(result)
                    completed_count += 1
                    
                    # Log progress every 10 completions
                    if completed_count % 10 == 0:
                        logging.info(f"Completed {completed_count}/{len(ticker_data)} ticker-date pairs")
                        
                except Exception as e:
                    logging.error(f"Error in parallel processing: {e}")
                    # Add failed result
                    row_index, ticker, date_str = future_to_ticker[future]
                    failed_result = {
                        'Ticker': ticker, 
                        'Date': date_str, 
                        '__input_order': row_index,
                        'Failure Reason': f"Parallel processing error: {str(e)[:100]}"
                    }
                    # Fill with Not Available
                    start_time = datetime.strptime('04:00', '%H:%M').time()
                    end_time = datetime.strptime('20:00', '%H:%M').time()
                    current_time = datetime.combine(datetime.today().date(), start_time)
                    end_datetime = datetime.combine(datetime.today().date(), end_time)
                    
                    while current_time <= end_datetime:
                        time_key = current_time.strftime('%H:%M')
                        failed_result[time_key] = 'Not Available'
                        current_time += timedelta(minutes=1)
                    
                    all_minute_data.append(failed_result)
        
        # Combine all minute data
        if all_minute_data:
            logging.info(f"Combining {len(all_minute_data)} ticker results (maintaining exact Excel order)")
            # Convert list of dictionaries to DataFrame
            combined_data = pd.DataFrame(all_minute_data)
            logging.info(f"DataFrame created with shape: {combined_data.shape}")
            
            # Ensure rows are ordered exactly by the Excel input order
            if '__input_order' in combined_data.columns:
                combined_data = combined_data.sort_values(by=['__input_order'], kind='mergesort')
                combined_data = combined_data.drop(columns=['__input_order'])
            
            # Generate output filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"minute_{timestamp}.csv"
            
            # Save to CSV
            combined_data.to_csv(output_file, index=False)
            
            # Count successful vs failed processing
            successful_count = len([row for row in all_minute_data if row.get('Failure Reason') == 'Success'])
            failed_count = len(all_minute_data) - successful_count
            
            logging.info(f"Successfully saved {len(combined_data)} minute records to {output_file}")
            logging.info(f"Processing results: {successful_count} successful, {failed_count} failed (maintained Excel order)")
            print(f"Minute data saved to: {output_file}")
            print(f"Total records: {len(combined_data)} (exact Excel order maintained)")
            print(f"Successful processing: {successful_count}")
            print(f"Failed processing: {failed_count} (filled with 'Not Available')")
            print(f"Unique tickers: {combined_data['Ticker'].nunique()}")
            print(f"Date range: {combined_data['Date'].min()} to {combined_data['Date'].max()}")
            
        else:
            logging.warning("No minute data was successfully processed")
            print("No minute data was successfully processed")
    
    except FileNotFoundError:
        logging.error("tickerminute.xlsx file not found")
        print("Error: tickerminute.xlsx file not found")
        print("Please ensure tickerminute.xlsx exists in the current directory")
    except Exception as e:
        logging.error(f"Error in main function: {e}")
        print(f"Error: {e}")
    
    # Log script completion time
    script_end_time = time.time()
    total_time = script_end_time - script_start_time
    logging.info(f"Script completed in {total_time:.2f} seconds")
    print(f"Script completed in {total_time:.2f} seconds")

if __name__ == "__main__":
    main()

