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

# Global rate limiter
polygon_limiter = RateLimiter(max_calls=30, period=60.0)

# Global Polygon client
_polygon_client = None

def get_polygon_client():
    """Get or create the global Polygon client instance."""
    global _polygon_client
    if _polygon_client is None:
        _polygon_client = RESTClient(POLYGON_API_KEY)
    return _polygon_client

def get_raw_intraday_data(ticker, date, api_key):
    """
    Get raw intraday data from Polygon API for minute-by-minute data
    """
    try:
        if isinstance(date, str):
            date = datetime.strptime(date, '%Y-%m-%d')
        date_str = date.strftime('%Y-%m-%d')
        
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{date_str}/{date_str}?adjusted=true&sort=asc&limit=50000&apiKey={api_key}"
        
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Check if we have valid data
        if 'status' not in data or data['status'] != 'OK':
            logging.debug(f"Invalid status for {ticker} on {date_str}: {data.get('status', 'UNKNOWN')}")
            return None
            
        if 'results' not in data or not data['results']:
            logging.debug(f"No results data for {ticker} on {date_str}")
            return None
        
        # Convert to DataFrame with proper timestamp handling
        df = pd.DataFrame(data['results'])
        if df.empty:
            logging.debug(f"Empty DataFrame for {ticker} on {date_str}")
            return None
            
        # Convert UTC timestamps to Eastern Time (automatically handles EST/EDT)
        df['timestamp'] = pd.to_datetime(df['t'], unit='ms', utc=True)
        df['timestamp'] = df['timestamp'].dt.tz_convert(EASTERN_TZ)
        df['time'] = df['timestamp'].dt.time
        
        return df
        
    except requests.exceptions.Timeout:
        logging.debug(f"Timeout getting raw intraday data for {ticker} on {date_str}")
        return None
    except requests.exceptions.RequestException as e:
        logging.debug(f"Request error getting raw intraday data for {ticker}: {e}")
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

def process_ticker_minute_data(ticker, date):
    """
    Process minute data for a single ticker and date
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
        
        # Add ticker and date columns
        minute_data['ticker'] = ticker
        minute_data['date'] = date
        
        # Reorder columns
        columns = ['ticker', 'date', 'timestamp', 'time', 'open', 'high', 'low', 'close', 'volume']
        minute_data = minute_data[columns]
        
        logging.info(f"Successfully processed {len(minute_data)} minute records for {ticker} on {date}")
        return minute_data
        
    except Exception as e:
        logging.error(f"Error processing minute data for {ticker} on {date}: {e}")
        return None

def main():
    """Main function to process tickers from tickers.xlsx and output minute data"""
    script_start_time = time.time()
    
    try:
        # Read tickers from Excel file
        logging.info("Reading tickers from tickers.xlsx")
        df = pd.read_excel('tickers.xlsx')
        
        logging.info(f"Processing {len(df)} ticker-date pairs from Excel")
        
        all_minute_data = []
        
        for row_index, row in df.iterrows():
            ticker = str(row['Ticker']).strip().upper()
            date_str = str(row['Date']).strip()
            
            # Process minute data for this ticker-date pair
            minute_data = process_ticker_minute_data(ticker, date_str)
            
            if minute_data is not None:
                all_minute_data.append(minute_data)
            
            # Add small delay to respect rate limits
            time.sleep(0.1)
            
            # Log progress every 10 tickers
            if (row_index + 1) % 10 == 0:
                logging.info(f"Processed {row_index + 1}/{len(df)} ticker-date pairs")
        
        # Combine all minute data
        if all_minute_data:
            combined_data = pd.concat(all_minute_data, ignore_index=True)
            
            # Generate output filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"minute_{timestamp}.csv"
            
            # Save to CSV
            combined_data.to_csv(output_file, index=False)
            
            logging.info(f"Successfully saved {len(combined_data)} minute records to {output_file}")
            print(f"Minute data saved to: {output_file}")
            print(f"Total records: {len(combined_data)}")
            print(f"Unique tickers: {combined_data['ticker'].nunique()}")
            print(f"Date range: {combined_data['date'].min()} to {combined_data['date'].max()}")
            
        else:
            logging.warning("No minute data was successfully processed")
            print("No minute data was successfully processed")
    
    except FileNotFoundError:
        logging.error("tickers.xlsx file not found")
        print("Error: tickers.xlsx file not found")
        print("Please ensure tickers.xlsx exists in the current directory")
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

