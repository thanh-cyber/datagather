"""
stock_data_rsi.py - Standalone script that outputs ONLY RSI columns
Reads from tickers.xlsx (or specified file) and calculates RSI at PM High and Open High
Uses the fixed RSI calculation method from stock_data_pm.py
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import pytz
import time
import threading
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from dotenv import load_dotenv
from polygon import RESTClient
import yfinance as yf
import sys
import ta  # For RSI calculation

# Configure logging to file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    handlers=[
        logging.FileHandler("stock_data_rsi.log", mode='a'),  # Append mode, creates file if doesn't exist
        logging.StreamHandler()  # Also log to console
    ]
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
POLYGON_PREMIUM = os.getenv("POLYGON_PREMIUM", "false").lower() == "true"

# Instantiate global rate limiter
polygon_limiter = RateLimiter(max_calls=200 if POLYGON_PREMIUM else 30, period=1.0 if POLYGON_PREMIUM else 60.0)

# Timezone
EASTERN_TZ = pytz.timezone('US/Eastern')

# Global Polygon client (thread-safe)
_polygon_client_lock = threading.Lock()
_polygon_client = None

def get_polygon_client():
    """Get or create Polygon client (thread-safe singleton)."""
    global _polygon_client
    with _polygon_client_lock:
        if _polygon_client is None:
            _polygon_client = RESTClient(POLYGON_API_KEY)
        return _polygon_client

def calculate_rsi_at_high(data, high_time, timeframe_minutes=1, period=14, hist_data=None):
    """Calculate RSI at a specific high time for a given timeframe using data imputation like TradeZero Pro."""
    try:
        # Use hist_data for rolling calculation if available, but ensure final value is from target date
        if hist_data is not None and not hist_data.empty:
            # Calculate RSI using historical data for rolling window (30 days)
            if timeframe_minutes == 1:
                working_data = hist_data.copy()
            else:
                try:
                    working_data = hist_data.resample(f'{timeframe_minutes}min').agg({
                        'open': 'first',
                        'high': 'max',
                        'low': 'min',
                        'close': 'last',
                        'volume': 'sum'
                    }).dropna()
                    
                    if working_data.empty:
                        logging.debug(f"RSI calculation failed: resampled DataFrame is empty. Original={len(hist_data)}, Timeframe={timeframe_minutes}min")
                        return "Not Available"
                        
                except Exception as resample_error:
                    logging.debug(f"RSI resampling failed for {timeframe_minutes}min: {resample_error}")
                    return "Not Available"
            
            # Data imputation like TradeZero Pro - forward-fill missing closing prices
            working_data['close'] = working_data['close'].ffill()
            working_data['close'] = working_data['close'].bfill()
            
            # Ensure we have enough data for RSI calculation (standard 14-period RSI)
            if len(working_data) < period + 1:
                logging.debug(f"RSI calculation failed: insufficient data even after imputation. Available={len(working_data)}, Required={period + 1}, Timeframe={timeframe_minutes}min")
                return "Not Available"
            
            # Calculate RSI using standard 14-period window (like TradeZero Pro)
            rsi = ta.momentum.RSIIndicator(close=working_data['close'], window=period)
            rsi_values = rsi.rsi()
            
            # Find the RSI value at the specific high_time from resampled historical data
            # For longer timeframes (like 60min), use the RSI from historical data since target date data may not have enough points
            if high_time in data.index:
                # For all timeframes, use RSI from historical data for proper rolling calculation (like brokers)
                # This ensures consistent calculation across all timeframes
                if high_time in working_data.index:
                    # Exact match found in resampled historical data
                    rsi_at_high = rsi_values.loc[high_time]
                else:
                    # Find the bar that contains high_time
                    # For 1min: exact minute match
                    # For 2min/5min/60min: find the resampled bar that contains high_time
                    if timeframe_minutes == 1:
                        # For 1-minute, find exact minute or closest minute in historical data
                        time_diff = abs(working_data.index - high_time)
                        closest_idx = time_diff.argmin()
                        rsi_at_high = rsi_values.iloc[closest_idx]
                    else:
                        # For longer timeframes, find the bar that contains high_time
                        # Get all bars up to and including the one containing high_time
                        bars_before_high = working_data[working_data.index <= high_time]
                        if not bars_before_high.empty:
                            # Use the last bar that ends at or before high_time
                            rsi_at_high = rsi_values.loc[bars_before_high.index[-1]]
                        else:
                            # If no bars before high_time, find closest bar (shouldn't happen in practice)
                            time_diff = abs(working_data.index - high_time)
                            closest_idx = time_diff.argmin()
                            rsi_at_high = rsi_values.iloc[closest_idx]
            else:
                # High time not in target date data - return Not Available
                logging.debug(f"High time {high_time} not found in target date data")
                return "Not Available"
        else:
            # No historical data available - use target date data only
            use_data = data
            if use_data.empty or pd.isna(high_time):
                logging.debug(f"RSI calculation failed: use_data empty={use_data.empty}, high_time={high_time}")
                return "Not Available"
            
            # For 1-minute data, calculate RSI directly without resampling (like brokers do)
            if timeframe_minutes == 1:
                working_data = use_data.copy()
            else:
                try:
                    working_data = use_data.resample(f'{timeframe_minutes}min').agg({
                        'open': 'first',
                        'high': 'max',
                        'low': 'min',
                        'close': 'last',
                        'volume': 'sum'
                    }).dropna()
                    
                    if working_data.empty:
                        logging.debug(f"RSI calculation failed: resampled DataFrame is empty. Original={len(use_data)}, Timeframe={timeframe_minutes}min")
                        return "Not Available"
                        
                except Exception as resample_error:
                    logging.debug(f"RSI resampling failed for {timeframe_minutes}min: {resample_error}")
                    return "Not Available"
            
            # Data imputation like TradeZero Pro - forward-fill missing closing prices
            working_data['close'] = working_data['close'].ffill()
            working_data['close'] = working_data['close'].bfill()
            
            # Ensure we have enough data for RSI calculation (standard 14-period RSI)
            if len(working_data) < period + 1:
                logging.debug(f"RSI calculation failed: insufficient data even after imputation. Available={len(working_data)}, Required={period + 1}, Timeframe={timeframe_minutes}min")
                return "Not Available"
            
            # Calculate RSI using standard 14-period window (like TradeZero Pro)
            rsi = ta.momentum.RSIIndicator(close=working_data['close'], window=period)
            rsi_values = rsi.rsi()
            
            # Find the RSI value at or closest to the high time
            if high_time in working_data.index:
                rsi_at_high = rsi_values.loc[high_time]
            else:
                # Find the closest time within a reasonable range
                time_diff = abs(working_data.index - high_time)
                closest_idx = time_diff.argmin()
                rsi_at_high = rsi_values.iloc[closest_idx]
        
        if pd.isna(rsi_at_high):
            return "Not Available"
        
        return round(rsi_at_high, 2)
        
    except Exception as e:
        logging.warning(f"Error calculating RSI at high for {timeframe_minutes}min: {e}")
        return "Not Available"

def get_intraday_data_for_rsi(ticker, date):
    """Get intraday data needed for RSI calculation only."""
    try:
        start_date = datetime.strptime(date, "%Y-%m-%d")
        start_date = EASTERN_TZ.localize(start_date.replace(hour=4, minute=0, second=0, microsecond=0))
        end_date = datetime.strptime(date, "%Y-%m-%d")
        end_date = EASTERN_TZ.localize(end_date.replace(hour=20, minute=0, second=0, microsecond=0))
        
        # Get historical data for RSI calculation (30 days)
        hist_start = start_date - timedelta(days=30)
        hist_data = None
        
        client = get_polygon_client()
        
        try:
            polygon_limiter.acquire()
            hist_aggs = client.get_aggs(ticker, 1, "minute", hist_start, end_date.strftime("%Y-%m-%d"), adjusted=True)
            if hist_aggs:
                hist_data = pd.DataFrame([{
                    'timestamp': agg.timestamp,
                    'open': agg.open,
                    'high': agg.high,
                    'low': agg.low,
                    'close': agg.close,
                    'volume': agg.volume
                } for agg in hist_aggs])
                hist_data['datetime'] = pd.to_datetime(hist_data['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(EASTERN_TZ)
                hist_data.set_index('datetime', inplace=True)
                hist_data = hist_data[~hist_data.index.duplicated(keep='first')]
                logging.info(f"Fetched {len(hist_data)} historical bars for {ticker} from {hist_start} to {end_date.strftime('%Y-%m-%d')}")
        except Exception as e:
            logging.warning(f"Historical data fetch failed for {ticker}: {e}")

        current_date = datetime.now(EASTERN_TZ).date()
        if start_date.date() > current_date:
            logging.warning(f"Target date {date} is in the future for {ticker}")
            return None, None, None, None

        pre_market_start = pd.Timestamp(f"{date} 04:00:00").tz_localize(EASTERN_TZ)
        pre_market_end = pd.Timestamp(f"{date} 09:29:59").tz_localize(EASTERN_TZ)
        regular_start = pd.Timestamp(f"{date} 09:30:00").tz_localize(EASTERN_TZ)
        regular_end = pd.Timestamp(f"{date} 16:00:00").tz_localize(EASTERN_TZ)

        pre_market_data = None
        for attempt in range(3):
            try:
                polygon_limiter.acquire()
                aggs = client.get_aggs(
                    ticker,
                    multiplier=1,
                    timespan="minute",
                    from_=start_date.strftime("%Y-%m-%d"),
                    to=start_date.strftime("%Y-%m-%d"),
                    adjusted=True
                )
                if not isinstance(aggs, list):
                    logging.warning(f"Invalid aggs type for {ticker}: {type(aggs)}")
                    continue
                    
                pre_market_aggs = pd.DataFrame([
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
                if not pre_market_aggs.empty:
                    pre_market_aggs["timestamp"] = pd.to_datetime(pre_market_aggs["timestamp"], unit="ms", errors="coerce")
                    if pre_market_aggs["timestamp"].isnull().any():
                        pre_market_aggs = pre_market_aggs.dropna(subset=["timestamp"])
                    pre_market_aggs["datetime"] = pre_market_aggs["timestamp"].dt.tz_localize("UTC").dt.tz_convert(EASTERN_TZ)
                    pre_market_aggs.set_index("datetime", inplace=True)
                    pre_market_data = pre_market_aggs[pre_market_start:pre_market_end].copy()
                break
            except Exception as e:
                if "429" in str(e):
                    time.sleep(0.1)
                    continue
                logging.error(f"Error fetching pre-market data for {ticker}: {e}")
                pre_market_data = None
                break

        full_data = None
        for attempt in range(3):
            try:
                polygon_limiter.acquire()
                aggs = client.get_aggs(
                    ticker,
                    multiplier=1,
                    timespan="minute",
                    from_=start_date.strftime("%Y-%m-%d"),
                    to=end_date.strftime("%Y-%m-%d"),
                    adjusted=True
                )
                if not isinstance(aggs, list):
                    logging.warning(f"Invalid aggs type for {ticker}: {type(aggs)}")
                    continue
                    
                full_data = pd.DataFrame([
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
                break
            except Exception as e:
                if "429" in str(e):
                    time.sleep(0.1)
                    continue
                logging.error(f"Error fetching aggregate data for {ticker}: {e}")
                full_data = None
                break

        if pre_market_data is None and (full_data is None or full_data.empty):
            logging.warning(f"No Polygon.io data for {ticker} on {date}, falling back to yfinance")
            try:
                yf_ticker = yf.Ticker(ticker)
                polygon_limiter.acquire()
                hist = yf_ticker.history(interval="1m", start=date, auto_adjust=True)
                if not hist.empty:
                    data = hist[["Open", "High", "Low", "Close", "Volume"]].reset_index()
                    data.columns = ["timestamp", "open", "high", "low", "close", "volume"]
                    data["timestamp"] = pd.to_datetime(data["timestamp"], errors="coerce")
                    if data["timestamp"].dt.tz:
                        data["datetime"] = data["timestamp"].dt.tz_convert(EASTERN_TZ)
                    else:
                        data["datetime"] = data["timestamp"].dt.tz_localize(EASTERN_TZ)
                    data.set_index("datetime", inplace=True)
                    pre_market = data[pre_market_start:pre_market_end].copy()
                    regular_market = data[regular_start:regular_end].copy()
                    return pre_market, regular_market, hist_data, True
                else:
                    return None, None, None, True
            except Exception as e:
                logging.error(f"Error fetching yfinance intraday data for {ticker}: {e}")
                return None, None, None, True

        regular_market_filtered = None
        
        if pre_market_data is not None and full_data is not None:
            full_data["timestamp"] = pd.to_datetime(full_data["timestamp"], unit="ms", errors="coerce")
            if full_data["timestamp"].isna().any():
                logging.warning(f"Invalid timestamps in Polygon.io data for {ticker}")
                return None, None, None, False
            full_data["datetime"] = full_data["timestamp"].dt.tz_localize("UTC").dt.tz_convert(EASTERN_TZ)
            full_data.set_index("datetime", inplace=True)
            regular_market_filtered = full_data[regular_start:regular_end].copy()
        elif full_data is not None:
            full_data["timestamp"] = pd.to_datetime(full_data["timestamp"], unit="ms", errors="coerce")
            if full_data["timestamp"].isna().any():
                logging.warning(f"Invalid timestamps in Polygon.io data for {ticker}")
                return None, None, None, False
            full_data["datetime"] = full_data["timestamp"].dt.tz_localize("UTC").dt.tz_convert(EASTERN_TZ)
            full_data.set_index("datetime", inplace=True)
            regular_market_filtered = full_data[regular_start:regular_end].copy()
            pre_market_data = full_data[pre_market_start:pre_market_end].copy()
        elif pre_market_data is not None:
            # Only pre_market_data exists, but we need regular_market too - return None
            logging.warning(f"Only pre-market data available for {ticker} on {date}, regular market data needed")
            return None, None, None, False
        else:
            logging.warning(f"No intraday data for {ticker} on {date}")
            return None, None, None, False

        # Use pre_market_data directly if available (already filtered correctly)
        if pre_market_data is not None and not pre_market_data.empty:
            pre_market = pre_market_data.copy()
        else:
            logging.warning(f"No pre-market data for {ticker} on {date}")
            return None, None, None, False
        
        # Use filtered regular_market if available (already filtered correctly)
        if regular_market_filtered is not None and not regular_market_filtered.empty:
            regular_market = regular_market_filtered.copy()
        else:
            logging.warning(f"No regular market data for {ticker} on {date}")
            return None, None, None, False

        return pre_market, regular_market, hist_data, False
        
    except Exception as e:
        logging.error(f"Error in get_intraday_data_for_rsi for {ticker}: {e}")
        return None, None, None, False

def process_pair(ticker, date, excel_row_index=None):
    """Process a single ticker-date pair and return RSI data."""
    try:
        pre_market, regular_market, hist_data, used_yfinance = get_intraday_data_for_rsi(ticker, date)
        
        if pre_market is None or pre_market.empty:
            logging.warning(f"No pre-market data for {ticker} on {date}")
            return create_failed_row(ticker, date, excel_row_index)
        
        if regular_market is None or regular_market.empty:
            logging.warning(f"No regular market data for {ticker} on {date}")
            return create_failed_row(ticker, date, excel_row_index)
        
        result = {}
        
        # Calculate PM High and time
        pm_high = pre_market["high"].max()
        if pd.isna(pm_high):
            logging.warning(f"No valid high price for {ticker} pre-market data")
            return create_failed_row(ticker, date, excel_row_index)
        
        pm_high_idx = pre_market["high"].idxmax()
        pm_high_time = pm_high_idx if not pd.isna(pm_high_idx) else None
        
        # Calculate Open High and time
        open_high = regular_market["high"].max()
        if pd.isna(open_high):
            logging.warning(f"No valid high price in regular market data for {ticker}")
            return create_failed_row(ticker, date, excel_row_index)
        
        open_high_idx = regular_market["high"].idxmax()
        open_high_time = open_high_idx if not pd.isna(open_high_idx) else None
        
        # Calculate RSI at PM High
        if pm_high_time is not None:
            result["PM RSI at High 1min"] = calculate_rsi_at_high(pre_market, pm_high_time, 1, hist_data=hist_data)
            result["PM RSI at High 2min"] = calculate_rsi_at_high(pre_market, pm_high_time, 2, hist_data=hist_data)
            result["PM RSI at High 5min"] = calculate_rsi_at_high(pre_market, pm_high_time, 5, hist_data=hist_data)
            result["PM RSI at High 1hour"] = calculate_rsi_at_high(pre_market, pm_high_time, 60, hist_data=hist_data)
        else:
            result["PM RSI at High 1min"] = "Not Available"
            result["PM RSI at High 2min"] = "Not Available"
            result["PM RSI at High 5min"] = "Not Available"
            result["PM RSI at High 1hour"] = "Not Available"
        
        # Calculate RSI at Open High
        if open_high_time is not None:
            result["Open RSI at High 1min"] = calculate_rsi_at_high(regular_market, open_high_time, 1, hist_data=hist_data)
            result["Open RSI at High 2min"] = calculate_rsi_at_high(regular_market, open_high_time, 2, hist_data=hist_data)
            result["Open RSI at High 5min"] = calculate_rsi_at_high(regular_market, open_high_time, 5, hist_data=hist_data)
            result["Open RSI at High 1hour"] = calculate_rsi_at_high(regular_market, open_high_time, 60, hist_data=hist_data)
        else:
            result["Open RSI at High 1min"] = "Not Available"
            result["Open RSI at High 2min"] = "Not Available"
            result["Open RSI at High 5min"] = "Not Available"
            result["Open RSI at High 1hour"] = "Not Available"
        
        return create_final_row(ticker, date, result, excel_row_index)
        
    except Exception as e:
        logging.error(f"Error processing {ticker} on {date}: {e}")
        return create_failed_row(ticker, date, excel_row_index, str(e))

def create_final_row(ticker, date, rsi_data, excel_row_index=None):
    """Create final row with only RSI columns."""
    row = {
        "Ticker": ticker,
        "Date": date,
        "PM RSI at High 1min": rsi_data.get("PM RSI at High 1min", "Not Available"),
        "PM RSI at High 2min": rsi_data.get("PM RSI at High 2min", "Not Available"),
        "PM RSI at High 5min": rsi_data.get("PM RSI at High 5min", "Not Available"),
        "PM RSI at High 1hour": rsi_data.get("PM RSI at High 1hour", "Not Available"),
        "Open RSI at High 1min": rsi_data.get("Open RSI at High 1min", "Not Available"),
        "Open RSI at High 2min": rsi_data.get("Open RSI at High 2min", "Not Available"),
        "Open RSI at High 5min": rsi_data.get("Open RSI at High 5min", "Not Available"),
        "Open RSI at High 1hour": rsi_data.get("Open RSI at High 1hour", "Not Available"),
    }
    # Store excel_row_index for sorting (but don't include in output)
    row['_excel_row_index'] = excel_row_index if excel_row_index is not None else 0
    return row

def create_failed_row(ticker, date, excel_row_index=None, error_message="Failed"):
    """Create a failed row with all RSI columns set to 'Not Available'."""
    row = {
        "Ticker": ticker,
        "Date": date if date else "Invalid Date",
        "PM RSI at High 1min": "Not Available",
        "PM RSI at High 2min": "Not Available",
        "PM RSI at High 5min": "Not Available",
        "PM RSI at High 1hour": "Not Available",
        "Open RSI at High 1min": "Not Available",
        "Open RSI at High 2min": "Not Available",
        "Open RSI at High 5min": "Not Available",
        "Open RSI at High 1hour": "Not Available",
        "Error": error_message
    }
    # Store excel_row_index for sorting (but don't include in output)
    row['_excel_row_index'] = excel_row_index if excel_row_index is not None else 0
    return row

def main():
    """Main function to process tickers from Excel file."""
    excel_filename = "Tickers.xlsx"
    
    # Check for command-line argument (Excel filename)
    if len(sys.argv) > 1:
        first_arg = sys.argv[1]
        try:
            datetime.strptime(first_arg, "%Y-%m-%d")
            logging.error("This script only supports Excel file mode. Please provide an Excel filename.")
            return
        except ValueError:
            excel_filename = first_arg if first_arg.endswith('.xlsx') else f"{first_arg}.xlsx"
            logging.info(f"Using Excel file: {excel_filename}")
    else:
        logging.info("No command line arguments provided, using default Tickers.xlsx")

    try:
        df = pd.read_excel(excel_filename)
        logging.info(f"Processing Excel with {len(df)} rows")
        
        ticker_date_pairs = []
        for row_index, row in df.iterrows():
            ticker = str(row['Ticker']).strip().upper()
            date_str = str(row['Date']).strip()
            
            excel_row_index = row_index
            
            # Skip empty or NaN values
            if pd.isna(ticker) or pd.isna(date_str) or ticker == '' or date_str == '':
                continue
            
            # Parse date - handle multiple formats
            try:
                if isinstance(date_str, str):
                    # Try different date formats
                    date_obj = None
                    date_formats = [
                        "%Y-%m-%d",      # 2025-11-20
                        "%d/%m/%Y",      # 20/11/2025
                        "%m/%d/%Y",      # 11/20/2025
                        "%Y/%m/%d",      # 2025/11/20
                        "%d-%m-%Y",      # 20-11-2025
                        "%m-%d-%Y",      # 11-20-2025
                    ]
                    for fmt in date_formats:
                        try:
                            date_obj = datetime.strptime(date_str.strip(), fmt)
                            break
                        except ValueError:
                            continue
                    
                    if date_obj is None:
                        # Fallback to pandas parsing
                        date_obj = pd.to_datetime(date_str)
                else:
                    date_obj = pd.to_datetime(date_str)
                
                date = date_obj.strftime("%Y-%m-%d")
            except Exception as e:
                logging.warning(f"Invalid date format for {ticker}: {date_str} - {e}")
                continue
            
            ticker_date_pairs.append((ticker, date, excel_row_index))
        
        logging.info(f"Processing {len(ticker_date_pairs)} ticker-date pairs")
        
        # Process in parallel
        results = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            future_to_pair = {
                executor.submit(process_pair, ticker, date, excel_row_index): (ticker, date, excel_row_index)
                for ticker, date, excel_row_index in ticker_date_pairs
            }
            
            for future in as_completed(future_to_pair):
                ticker, date, excel_row_index = future_to_pair[future]
                try:
                    result = future.result()
                    results.append(result)
                    logging.info(f"Completed {ticker} on {date}")
                except Exception as e:
                    logging.error(f"Error processing {ticker} on {date}: {e}")
                    results.append(create_failed_row(ticker, date, excel_row_index, str(e)))
        
        # Create DataFrame and sort by internal _excel_row_index to maintain Excel order
        output_df = pd.DataFrame(results)
        if not output_df.empty and "_excel_row_index" in output_df.columns:
            output_df = output_df.sort_values("_excel_row_index").reset_index(drop=True)
            # Drop the internal sorting column before output
            output_df = output_df.drop(columns=["_excel_row_index"])
        
        # Generate output filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"stock_data_rsi_{timestamp}.xlsx"
        
        # Write to Excel
        output_df.to_excel(output_file, index=False)
        logging.info(f"Output written to {output_file}")
        print(f"Output written to {output_file}")
        
    except FileNotFoundError:
        logging.error(f"Excel file '{excel_filename}' not found")
        print(f"Error: Excel file '{excel_filename}' not found")
    except Exception as e:
        logging.error(f"Error processing Excel file: {e}")
        print(f"Error: {e}")

if __name__ == "__main__":
    main()

