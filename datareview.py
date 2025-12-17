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
    handlers=[logging.FileHandler("datareview.log")]
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

def create_minute_data(data, previous_close):
    """Create minute-by-minute data with forward fill from 04:00 to 20:00"""
    
    # Create time range from 04:00 to 20:00 (PM to AH close)
    start_time = datetime.strptime('04:00', '%H:%M').time()
    end_time = datetime.strptime('20:00', '%H:%M').time()
    
    # Get the date from the data index
    base_date = data.index[0].date()
    
    # Create minute data dictionary
    minute_data = {}
    last_price = previous_close
    
    # Fill minute data
    current_time = datetime.combine(base_date, start_time)
    end_datetime = datetime.combine(base_date, end_time)
    
    while current_time <= end_datetime:
        time_key = current_time.strftime('%H:%M')
        
        # Find bar for this minute in the DataFrame
        matching_bars = data[data.index.time == current_time.time()]
        
        if not matching_bars.empty:
            minute_data[time_key] = matching_bars['close'].iloc[-1]  # Use last bar if multiple
            last_price = matching_bars['close'].iloc[-1]
        else:
            # No trade this minute, use last price (forward fill)
            minute_data[time_key] = last_price
        
        current_time += timedelta(minutes=1)
    
    return minute_data

def get_pm_high_open_and_minute_data(ticker, date, previous_close=None):
    """
    Get PM High Open (the OPEN price of the 1-minute candle at PM High Time) and minute-by-minute data.
    Returns: (pm_high_open, minute_data_dict, actual_pm_high_time) or (None, None, None) on error
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
            return None, None, None
        
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
            return None, None, None
        
        # Convert timestamp to datetime with timezone
        data['datetime'] = pd.to_datetime(data['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(EASTERN_TZ)
        data.set_index('datetime', inplace=True)
        data = data[~data.index.duplicated(keep='first')]
        
        # Filter to pre-market hours (04:00 to 09:29:59)
        pre_market = data[(data.index >= pre_market_start) & (data.index <= pre_market_end)].copy()
        
        if pre_market.empty:
            logging.warning(f"No pre-market data for {ticker} on {date}")
            return None, None, None
        
        # Get previous close if not provided (use first available price or last close)
        if previous_close is None:
            # Try to get previous close from the data or use first available price
            if not data.empty:
                previous_close = data.iloc[0]['close']
            else:
                previous_close = None
        
        # Find when the maximum HIGH occurred
        pm_high_idx = pre_market["high"].idxmax()
        pm_high_time = pm_high_idx if not pd.isna(pm_high_idx) else None
        
        if pm_high_time is None:
            logging.warning(f"Could not determine PM High Time for {ticker} on {date}")
            return None, None, None
        
        # Round down PM High Time to the nearest minute (e.g., 08:00:15 -> 08:00:00)
        pm_high_time_minute = pm_high_time.replace(second=0, microsecond=0)
        
        # Create minute-by-minute data for full day (04:00 to 20:00)
        # Use previous_close if available, otherwise use first available price from data
        fallback_price = previous_close if previous_close is not None else (data.iloc[0]['close'] if not data.empty else None)
        if fallback_price is None:
            logging.warning(f"No price available for minute data creation for {ticker} on {date}")
            return None, None, None
        
        minute_data = create_minute_data(data, fallback_price)
        
        # Store the actual PM High Time (before rounding) for verification
        actual_pm_high_time = pm_high_time
        
        # First, try to find the exact bar at the rounded minute timestamp
        if pm_high_time_minute in pre_market.index:
            pm_high_open = pre_market.loc[pm_high_time_minute, "open"]
            if not pd.isna(pm_high_open):
                return round(float(pm_high_open), 4), minute_data, actual_pm_high_time
        
        # If exact match not found, find all bars that match the minute (hour:minute) and sort by timestamp
        matching_bars = pre_market[pre_market.index.time == pm_high_time_minute.time()].sort_index()
        
        if not matching_bars.empty:
            # Use the first (earliest) bar of that minute (the actual minute candle open)
            pm_high_open = matching_bars.iloc[0]["open"]
            
            if pd.isna(pm_high_open):
                logging.warning(f"PM High Open is NaN for {ticker} on {date}")
                return None, None, None
            
            return round(float(pm_high_open), 4), minute_data, actual_pm_high_time
        else:
            # Fallback: try to find the closest bar within the same minute
            time_diff = abs(pre_market.index - pm_high_time_minute)
            closest_idx = time_diff.argmin()
            closest_time = pre_market.index[closest_idx]
            
            # Only use if within 1 minute and same hour:minute
            time_diff_seconds = abs((closest_time - pm_high_time_minute).total_seconds())
            if time_diff_seconds <= 60 and closest_time.time().replace(second=0) == pm_high_time_minute.time():
                pm_high_open = pre_market.iloc[closest_idx]["open"]
                if not pd.isna(pm_high_open):
                    return round(float(pm_high_open), 4), minute_data, actual_pm_high_time
            
            logging.warning(f"PM High Time minute {pm_high_time_minute.strftime('%H:%M:%S')} not found in pre_market data for {ticker} on {date}")
            return None, None, None
            
    except Exception as e:
        logging.error(f"Error getting PM High Open for {ticker} on {date}: {e}")
        return None, None, None

def process_row(row):
    """Process a single row and return result if it matches criteria."""
    try:
        ticker = str(row['Ticker']).strip().upper()
        date_str = str(row['Date']).strip()
        
        # Handle PM High Time - could be string "08:00:00" or datetime
        pm_high_time_raw = row.get('PM High Time', '')
        if pd.isna(pm_high_time_raw) or pm_high_time_raw == "Not Available":
            return None
        
        # Convert to string and normalize
        pm_high_time = str(pm_high_time_raw).strip()
        # Handle different time formats: "08:00:00", "8:00:00", etc.
        if ':' in pm_high_time:
            # Extract hour:minute:second
            time_parts = pm_high_time.split(':')
            if len(time_parts) >= 2:
                hour = int(time_parts[0])
                minute = int(time_parts[1])
                # Normalize to "08:00:00" format
                pm_high_time_normalized = f"{hour:02d}:{minute:02d}:00"
            else:
                return None
        else:
            return None
        
        # Filter: PM High Time must be 08:00:00
        if pm_high_time_normalized != "08:00:00":
            return None
        
        # Parse date - handle multiple formats (stock_data_pm outputs DD/MM/YYYY)
        try:
            # Try DD/MM/YYYY first (stock_data_pm.py format)
            date = datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
        except ValueError:
            try:
                # Try YYYY-MM-DD
                date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
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
                        return None
        
        # Get PM High value
        pm_high = row.get('PM High', None)
        if pm_high is None or pd.isna(pm_high) or pm_high == "Not Available":
            logging.warning(f"No PM High value for {ticker} on {date}")
            return None
        
        # Convert PM High to float
        try:
            pm_high = float(pm_high)
        except (ValueError, TypeError):
            logging.warning(f"Invalid PM High value for {ticker} on {date}: {pm_high}")
            return None
        
        # Get the 08:00 price (close price at 08:00) - column name is "08:00"
        price_8am = row.get('08:00', None)
        if price_8am is None or pd.isna(price_8am) or price_8am == "Not Available":
            logging.warning(f"No 08:00 price for {ticker} on {date}")
            return None
        
        # Convert to float
        try:
            price_8am = float(price_8am)
        except (ValueError, TypeError):
            logging.warning(f"Invalid 08:00 price for {ticker} on {date}: {price_8am}")
            return None
        
        # Get previous close for minute data creation
        # Try to fetch from API first (like stock_data_pm.py does) for accuracy
        previous_close = None
        try:
            target_dt = datetime.strptime(date, "%Y-%m-%d")
            client = get_polygon_client()
            
            # Look back up to 7 days to find the last trading day (like stock_data_pm.py)
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
                        adjusted=True
                    )
                    
                    if prev_aggs and len(prev_aggs) > 0:
                        close = prev_aggs[0].close
                        if close > 0 and close < 50000:  # Sanity check
                            previous_close = close
                            logging.info(f"Fetched previous close for {ticker} on {prev_date_str}: {previous_close}")
                            break
                except Exception as e:
                    logging.debug(f"Error fetching previous close for {prev_date_str}: {e}")
                    continue
        except Exception as e:
            logging.warning(f"Error fetching previous close from API for {ticker}: {e}")
        
        # Fallback to Excel value if API fetch failed
        if previous_close is None:
            previous_close_excel = row.get('Previous Close', None)
            if previous_close_excel is not None and not pd.isna(previous_close_excel) and previous_close_excel != "Not Available":
                try:
                    previous_close = float(previous_close_excel)
                    logging.info(f"Using Previous Close from Excel for {ticker}: {previous_close}")
                except (ValueError, TypeError):
                    previous_close = None
        
        # Final fallback: use first available price from data (will be set in get_pm_high_open_and_minute_data)
        if previous_close is None:
            logging.warning(f"Could not get previous close for {ticker} on {date}, will use first available price from data")
        
        # Get PM High Open and minute data (requires API call)
        # Also get the actual PM High Time from fresh API data to verify it matches Excel
        pm_high_open, minute_data, actual_pm_high_time = get_pm_high_open_and_minute_data(ticker, date, previous_close)
        
        if pm_high_open is None:
            logging.warning(f"Could not get PM High Open for {ticker} on {date}")
            return None
        
        # Verify that the actual PM High Time from API matches the Excel file's PM High Time
        if actual_pm_high_time:
            actual_pm_high_time_str = actual_pm_high_time.strftime("%H:%M:%S")
            # Normalize for comparison
            actual_hour = actual_pm_high_time.hour
            actual_minute = actual_pm_high_time.minute
            actual_normalized = f"{actual_hour:02d}:{actual_minute:02d}:00"
            
            if actual_normalized != "08:00:00":
                # PM High Time from API doesn't match Excel - log warning but continue
                logging.warning(f"PM High Time mismatch for {ticker} on {date}: Excel says 08:00:00, but API says {actual_pm_high_time_str}")
                # Still process it since user wants all tickers with Excel PM High Time = 08:00:00
        
        # Use fresh API data for 08:00 price (from minute_data) instead of Excel file value
        # This ensures consistency with the minute columns
        price_8am_fresh = None
        if minute_data and '08:00' in minute_data:
            try:
                price_8am_fresh = float(minute_data['08:00'])
            except (ValueError, TypeError):
                price_8am_fresh = price_8am  # Fallback to Excel value
        
        # Use fresh API value if available, otherwise use Excel value
        price_8am_to_use = price_8am_fresh if price_8am_fresh is not None else price_8am
        
        # Calculate % difference: ((PM High Open - 08:00) / 08:00) * 100
        if price_8am_to_use > 0:
            percent_diff = ((pm_high_open - price_8am_to_use) / price_8am_to_use) * 100
        else:
            logging.warning(f"Invalid 08:00 price (zero or negative) for {ticker} on {date}")
            return None
        
        # Validate: If PM High Time is 08:00:00, check if any later minutes have CLOSE prices higher than 08:00 HIGH
        # This would indicate a data inconsistency
        validation_warning = None
        if minute_data and pm_high is not None:
            # Get all minute prices after 08:00
            later_minutes = [k for k in minute_data.keys() if k > '08:00' and k < '09:30']
            for min_key in later_minutes:
                try:
                    min_price = float(minute_data[min_key])
                    if min_price > pm_high:
                        validation_warning = f"WARNING: {min_key} price ({min_price:.4f}) > PM High ({pm_high:.4f})"
                        logging.warning(f"{ticker} on {date}: {validation_warning}")
                        break
                except (ValueError, TypeError):
                    continue
        
        # Build result dictionary
        result = {
            "Ticker": ticker,
            "Date": date_str,  # Keep original format (DD/MM/YYYY)
            "PM High": round(float(pm_high), 4),
            "PM High Open": pm_high_open,
            "08:00 Price": round(price_8am_to_use, 4),
            "Percent Difference": round(percent_diff, 2)
        }
        
        # Add validation warning if found
        if validation_warning:
            result["Validation Warning"] = validation_warning
        
        # Add minute-by-minute data columns (04:00 to 20:00)
        if minute_data:
            for time_key, price in minute_data.items():
                result[time_key] = round(float(price), 4) if price is not None and not pd.isna(price) else "Not Available"
        
        return result
        
    except Exception as e:
        logging.error(f"Error processing row for {ticker if 'ticker' in locals() else 'Unknown'}: {e}")
        return None

def main():
    """Main function to filter tickers from stock data pm 2025.xlsx."""
    script_start_time = time.time()
    
    excel_filename = 'stock data pm 2025.xlsx'
    
    if not os.path.exists(excel_filename):
        logging.error(f"Excel file not found: {excel_filename}")
        print(f"Error: Excel file not found: {excel_filename}")
        return
    
    logging.info(f"Reading from Excel file: {excel_filename}")
    print(f"Reading from Excel file: {excel_filename}")
    
    # Read Excel file
    try:
        df = pd.read_excel(excel_filename)
    except Exception as e:
        logging.error(f"Error reading Excel file: {e}")
        print(f"Error reading Excel file: {e}")
        return
    
    if df.empty:
        logging.error("Excel file is empty")
        print("Error: Excel file is empty")
        return
    
    logging.info(f"Loaded {len(df)} rows from Excel file")
    print(f"Loaded {len(df)} rows from Excel file")
    
    # Filter rows where PM High Time is 08:00:00
    # Handle different formats: string "08:00:00", datetime, etc.
    def normalize_pm_high_time(val):
        if pd.isna(val) or val == "Not Available":
            return None
        val_str = str(val).strip()
        if ':' in val_str:
            parts = val_str.split(':')
            if len(parts) >= 2:
                try:
                    hour = int(parts[0])
                    minute = int(parts[1])
                    return f"{hour:02d}:{minute:02d}:00"
                except:
                    return None
        return None
    
    # Apply normalization and filter
    df['PM High Time Normalized'] = df['PM High Time'].apply(normalize_pm_high_time)
    df_filtered = df[df['PM High Time Normalized'] == '08:00:00'].copy()
    df_filtered = df_filtered.drop(columns=['PM High Time Normalized'])
    
    if df_filtered.empty:
        logging.info("No rows with PM High Time = 08:00:00")
        print("No rows with PM High Time = 08:00:00")
        return
    
    logging.info(f"Found {len(df_filtered)} rows with PM High Time = 08:00:00")
    print(f"Found {len(df_filtered)} rows with PM High Time = 08:00:00")
    print("Processing rows to calculate PM High Open and minute-by-minute data...")
    
    # Process rows (with parallelization for API calls)
    workers = 20
    all_results = []
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = []
        for idx, row in df_filtered.iterrows():
            future = executor.submit(process_row, row)
            futures.append((future, idx))
        
        completed_count = 0
        total_rows = len(df_filtered)
        progress_interval = max(1, min(50, total_rows // 20))
        
        for i, (future, idx) in enumerate(futures, 1):
            try:
                result = future.result()
                if result:
                    all_results.append(result)
            except Exception as e:
                logging.error(f"Error processing row at index {idx}: {e}")
            
            completed_count += 1
            if completed_count % progress_interval == 0:
                logging.info(f"Processed {completed_count}/{total_rows} rows, found {len(all_results)} matches")
                print(f"Progress: {completed_count}/{total_rows} rows processed, {len(all_results)} matches found")
    
    if not all_results:
        logging.info("No tickers found with PM High Time = 08:00:00")
        print("\nNo tickers found with PM High Time = 08:00:00")
        return
    
    # Convert to DataFrame
    output_df = pd.DataFrame(all_results)
    
    # Sort by Percent Difference (descending)
    output_df = output_df.sort_values('Percent Difference', ascending=False).reset_index(drop=True)
    
    # Reorder columns - put minute columns at the end like stock_data_pm.py
    primary_columns = ['Ticker', 'Date']
    
    # Get minute columns (04:00, 04:01, etc.) - sorted
    minute_columns = sorted([col for col in output_df.columns if col.count(':') == 1 and col.count(' ') == 0])
    
    # Get all other columns (excluding minute columns)
    other_columns = [col for col in output_df.columns if col not in minute_columns and col not in primary_columns]
    
    # Reorder DataFrame: primary columns, other columns, minute columns
    final_columns = primary_columns + other_columns + minute_columns
    output_df = output_df.reindex(columns=final_columns)
    
    # Create output filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_filename = f"datareview_output_{timestamp}.xlsx"
    
    # Write to Excel
    output_df.to_excel(output_filename, index=False)
    
    total_time = time.time() - script_start_time
    
    logging.info(f"Found {len(all_results)} tickers matching criteria in {total_time/60:.1f} minutes")
    print(f"\n[SUCCESS] Found {len(all_results)} tickers matching criteria in {total_time/60:.1f} minutes")
    print(f"Output saved to: {output_filename}")
    print(f"\nTop 10 by % Difference:")
    print(output_df.head(10).to_string(index=False))

if __name__ == "__main__":
    main()

