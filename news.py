#!/usr/bin/env python3
"""
News Scanner
Scans NASDAQ tickers based on PM criteria but only fetches and outputs news data
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pytz
import re
import logging
import time
import os
import pandas as pd
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv
import finnhub
from polygon import RESTClient

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('news.log')
        # Removed StreamHandler to prevent terminal output
    ]
)

# Timezone setup
EASTERN_TZ = pytz.timezone('US/Eastern')

# Rate limiter for API calls
class RateLimiter:
    def __init__(self, max_calls=5, period=1.0):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self.lock = threading.Lock()
    
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

# Global rate limiter - midpoint setting
news_limiter = RateLimiter(max_calls=6, period=1.0)

# API Keys and clients
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY', 'your_polygon_key_here')
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY', 'your_finnhub_key_here')

# Initialize clients
try:
    polygon_client = RESTClient(POLYGON_API_KEY)
    finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)
except Exception as e:
    logging.warning(f"Failed to initialize API clients: {e}")
    polygon_client = None
    finnhub_client = None

# Rate limiters - Optimized for paid Polygon plan (200 calls/second)
polygon_limiter = RateLimiter(max_calls=150, period=1.0)  # Conservative: 150 calls/second (75% of limit)
finnhub_limiter = RateLimiter(max_calls=10, period=1.0)  # Keep Finnhub at 10 calls/second

def try_direct_timestamp_parsing(raw_timestamp):
    """Quick parsing for known formats (fast path)."""
    if not raw_timestamp:
        return None
    
    try:
        from datetime import datetime
        
        # Handle common formats directly - these are the most frequent patterns
        formats_to_try = [
            '%m/%d/%Y',           # 12/25/2023
            '%Y-%m-%d',           # 2023-12-25
            '%B %d, %Y',          # December 25, 2023
            '%b %d, %Y',          # Dec 25, 2023
            '%d %B %Y',           # 25 December 2023
            '%d %b %Y',           # 25 Dec 2023
            '%I:%M %p',           # 3:45 PM
            '%H:%M',              # 15:45
            '%m/%d/%y',           # 12/25/23
            '%Y-%m-%dT%H:%M:%S',  # 2023-12-25T15:45:00
            '%Y-%m-%dT%H:%M:%SZ', # 2023-12-25T15:45:00Z
        ]
        
        clean_timestamp = raw_timestamp.strip()
        
        for fmt in formats_to_try:
            try:
                parsed = datetime.strptime(clean_timestamp, fmt)
                # Handle timezone - if no timezone info, assume Eastern
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=EASTERN_TZ)
                else:
                    parsed = parsed.astimezone(EASTERN_TZ)
                return parsed
            except ValueError:
                continue
                
    except Exception as e:
        logging.debug(f"Direct parsing failed for '{raw_timestamp}': {e}")
    
    return None

def try_relative_timestamp_parsing(raw_timestamp, fetch_time):
    """Parse relative timestamps like '1mo ago', '2mo ago', etc."""
    try:
        clean_timestamp = raw_timestamp.strip().lower()
        
        # Handle relative time formats
        if 'ago' in clean_timestamp:
            # Extract number and unit
            import re
            match = re.search(r'(\d+)\s*(mo|month|day|d|hour|h|minute|min|m|second|sec|s)\s*ago', clean_timestamp)
            if match:
                number = int(match.group(1))
                unit = match.group(2)
                
                # Calculate the actual date
                if unit in ['mo', 'month']:
                    # Approximate months as 30 days
                    actual_date = fetch_time - timedelta(days=number * 30)
                elif unit in ['day', 'd']:
                    actual_date = fetch_time - timedelta(days=number)
                elif unit in ['hour', 'h']:
                    actual_date = fetch_time - timedelta(hours=number)
                elif unit in ['minute', 'min', 'm']:
                    actual_date = fetch_time - timedelta(minutes=number)
                elif unit in ['second', 'sec', 's']:
                    actual_date = fetch_time - timedelta(seconds=number)
                else:
                    return None
                
                return actual_date
        
        return None
        
    except Exception as e:
        logging.debug(f"Relative timestamp parsing failed for '{raw_timestamp}': {e}")
        return None

def try_dateutil_timestamp_parsing(raw_timestamp, fetch_time):
    """Robust parsing with dateutil.parser (fallback)."""
    try:
        from dateutil import parser
        
        # Clean the timestamp
        clean_timestamp = raw_timestamp.strip()
        
        # Parse with fetch time as context for relative timestamps
        parsed = parser.parse(clean_timestamp, default=fetch_time, fuzzy=True)
        
        # Convert to Eastern Time
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=EASTERN_TZ)
        else:
            parsed = parsed.astimezone(EASTERN_TZ)
        
        return parsed
        
    except Exception as e:
        logging.debug(f"Dateutil parsing failed for '{raw_timestamp}': {e}")
        return None

def parse_yahoo_news(soup, ticker, target_date):
    """Parse Yahoo Finance news with optimized timestamp parsing."""
    news_items = []
    
    try:
        # Find all article containers - look for actual news articles
        articles = soup.find_all('section', {'data-testid': 'storyitem'})
        
        if not articles:
            # Fallback: look for other news containers
            articles = soup.find_all(['article', 'div'], class_=re.compile(r'story|item'))
        
        if not articles:
            # Another fallback
            articles = soup.find_all('li', class_=re.compile(r'story'))
        
        logging.info(f"[YAHOO NEWS] Found {len(articles)} potential articles for {ticker}")
        
        # Date range for filtering - target date and 2 days prior
        target_dt = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=EASTERN_TZ)
        start_dt = target_dt - timedelta(days=2)  # 2 days before target date
        end_dt = target_dt  # Up to target date (not after)
        logging.info(f"Looking for news from {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')} (target date and 2 days prior)")
        
        for article in articles:
            try:
                # Extract title
                title_elem = article.find(['h1', 'h2', 'h3', 'h4', 'a'], class_=re.compile(r'title|headline|subject'))
                if not title_elem:
                    title_elem = article.find(['h1', 'h2', 'h3', 'h4'])
                
                if not title_elem:
                    continue
                    
                title = title_elem.get_text(strip=True)
                if not title or len(title) < 10:
                    continue
                
                # Find timestamp element with optimized selectors
                time_elem = None
                
                # OPTIMIZED SELECTORS - Ordered by efficiency and Yahoo Finance prevalence
                time_selectors = [
                    # YAHOO FINANCE SPECIFIC (current structure) - Most common patterns first
                    ('div', 'publishing'),  # Most common Yahoo pattern - publishing info
                    ('div', lambda x: x and hasattr(x, 'get') and x.get('class') and 'publishing' in str(x.get('class', []))),
                    ('div', 'footer'),  # Footer often contains timestamp
                    ('div', lambda x: x and hasattr(x, 'get') and x.get('class') and 'footer' in str(x.get('class', []))),
                    
                    # YAHOO FINANCE LEGACY PATTERNS
                    ('span', 'caas-time'),  # Legacy Yahoo pattern
                    ('time', 'caas-time'),  # HTML5 time with caas-time class
                    ('*', lambda x: x and hasattr(x, 'get') and x.get('class') and 'caas-time' in str(x.get('class', []))),
                    
                    # DATA ATTRIBUTES (modern approach)
                    ('*', lambda x: hasattr(x, 'get') and x.get('data-time')),
                    ('*', lambda x: hasattr(x, 'get') and x.get('data-timestamp')),
                    ('*', lambda x: hasattr(x, 'get') and x.get('data-published')),
                    ('*', lambda x: hasattr(x, 'get') and x.get('datetime')),
                    
                    # HTML5 TIME ELEMENTS (high reliability)
                    ('time', None),  # Any time element
                    ('time', lambda x: True),  # Any time element with any class
                    
                    # COMMON NEWS PATTERNS (keyword-based) - Medium efficiency
                    ('span', lambda x: x and hasattr(x, 'get') and x.get('class') and any(term in str(x.get('class', [])).lower() for term in ['timestamp', 'time', 'date', 'published', 'ago', 'byline', 'meta'])),
                    ('div', lambda x: x and hasattr(x, 'get') and x.get('class') and any(term in str(x.get('class', [])).lower() for term in ['timestamp', 'time', 'date', 'published', 'ago', 'byline', 'meta'])),
                    ('p', lambda x: x and hasattr(x, 'get') and x.get('class') and any(term in str(x.get('class', [])).lower() for term in ['timestamp', 'time', 'date', 'published', 'ago', 'byline', 'meta'])),
                    ('small', lambda x: x and hasattr(x, 'get') and x.get('class') and any(term in str(x.get('class', [])).lower() for term in ['timestamp', 'time', 'date', 'published', 'ago'])),
                    
                    # FALLBACK CLASS-BASED SELECTORS
                    ('span', 'time'),
                    ('div', 'time'),
                    ('span', 'date'),
                    ('div', 'date'),
                    
                    # REGEX WILDCARDS (last resort) - Only when needed
                    ('*', lambda x: x and hasattr(x, 'text') and re.search(r'\d+\s+(hour|day|minute|second)s?\s+ago', x.text.lower())),
                    ('*', lambda x: x and hasattr(x, 'text') and re.search(r'(today|yesterday|just now|\d{1,2}:\d{2})', x.text.lower())),
                    ('*', lambda x: x and hasattr(x, 'text') and re.search(r'\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s+\d{1,2}', x.text.lower())),
                    ('*', lambda x: x and hasattr(x, 'text') and re.search(r'\d{1,2}/\d{1,2}/\d{2,4}', x.text)),
                    ('*', lambda x: x and hasattr(x, 'text') and re.search(r'\d{4}-\d{2}-\d{2}', x.text)),
                    
                    # Last resort: look for any element containing time-like text
                    ('*', lambda x: x and hasattr(x, 'text') and re.search(r'\d{1,2}:\d{2}', x.text))
                ]
                
                # Try each selector until we find a timestamp
                for tag, selector in time_selectors:
                    try:
                        if callable(selector):
                            elements = article.find_all(tag)
                            for elem in elements:
                                if elem and selector(elem):  # Add None check
                                    time_elem = elem
                                    break
                        else:
                            time_elem = article.find(tag, class_=selector)
                        
                        if time_elem:
                            break
                    except Exception as e:
                        logging.debug(f"Selector failed: {tag}, {selector}: {e}")
                        continue
                
                # Extract timestamp text with enhanced extraction
                pub_time = None
                if time_elem:
                    # Try data attributes first (most reliable)
                    for attr in ['data-time', 'data-timestamp', 'data-published', 'datetime']:
                        if time_elem.get(attr):
                            pub_time = time_elem.get(attr)
                            break
                    
                    # Fall back to text content
                    if not pub_time:
                        pub_time = time_elem.text.strip()
                
                if not pub_time:
                    logging.debug(f"[YAHOO NEWS] No timestamp found for article: {title[:50]}...")
                    continue
                
                logging.debug(f"[YAHOO NEWS] Found timestamp '{pub_time}' for article: {title[:50]}...")
                
                # Parse timestamp using four-tier strategy
                pub_date = None
                try:
                    # Use current time as fetch context for relative timestamps
                    fetch_time = datetime.now(pytz.UTC).astimezone(EASTERN_TZ)
                    
                    # STEP 1: Try relative timestamp parsing first (for "1mo ago" format)
                    pub_date = try_relative_timestamp_parsing(pub_time, fetch_time)
                    
                    # STEP 2: Try direct parsing (fast path for absolute dates)
                    if not pub_date:
                        pub_date = try_direct_timestamp_parsing(pub_time)
                    
                    # STEP 3: Use dateutil.parser as fallback (robust)
                    if not pub_date:
                        pub_date = try_dateutil_timestamp_parsing(pub_time, fetch_time)
                    
                    # STEP 4: Fallback to original logic for edge cases
                    if not pub_date:
                        try:
                            pub_date = datetime.strptime(pub_time, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC).astimezone(EASTERN_TZ)
                        except ValueError:
                            try:
                                pub_date = datetime.strptime(pub_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=EASTERN_TZ)
                            except ValueError:
                                try:
                                    pub_date = datetime.strptime(pub_time, "%m/%d/%Y %I:%M %p").replace(tzinfo=EASTERN_TZ)
                                except ValueError:
                                    try:
                                        pub_date = datetime.strptime(pub_time, "%B %d, %Y").replace(tzinfo=EASTERN_TZ)
                                    except ValueError:
                                        pass
                    
                    if not pub_date:
                        logging.debug(f"[YAHOO NEWS] Could not parse timestamp '{pub_time}' for article: {title[:50]}...")
                        continue
                    
                    logging.debug(f"[YAHOO NEWS] Parsed timestamp '{pub_time}' -> {pub_date.strftime('%Y-%m-%d')} for article: {title[:50]}...")
                    
                    # Check if article is within our date range
                    if start_dt.date() <= pub_date.date() <= end_dt.date():
                        # Clean title for logging to avoid Unicode issues
                        clean_title = title.encode('utf-8', errors='replace').decode('utf-8')
                        news_items.append(f"{pub_date.strftime('%Y-%m-%d')}: {title}")
                        logging.info(f"[YAHOO NEWS] Added news: {pub_date.strftime('%Y-%m-%d')}: {clean_title[:50]}...")
                    else:
                        clean_title = title.encode('utf-8', errors='replace').decode('utf-8')
                        logging.debug(f"[YAHOO NEWS] Article outside date range: {pub_date.strftime('%Y-%m-%d')} - {clean_title[:50]}...")
                        
                except Exception as e:
                    logging.debug(f"[YAHOO NEWS] Error parsing timestamp '{pub_time}': {e}")
                    continue
                    
            except Exception as e:
                logging.debug(f"[YAHOO NEWS] Error processing article: {e}")
                continue
        
        logging.info(f"[YAHOO NEWS] Successfully parsed {len(news_items)} news items for {ticker}")
        return news_items
        
    except Exception as e:
        logging.warning(f"Error parsing Yahoo news for {ticker}: {e}")
        return []

def fetch_news_yahoo_traditional(ticker, session, target_date):
    """Fetch news from Yahoo Finance using traditional requests."""
    try:
        news_limiter.acquire()
        
        # Add small delay for rate limiting
        time.sleep(0.2)
        
        # Try multiple URL formats
        urls_to_try = [
            f"https://finance.yahoo.com/quote/{ticker}/news",
            f"https://finance.yahoo.com/quote/{ticker}",
            f"https://finance.yahoo.com/quote/{ticker}/?p={ticker}",
        ]
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        for url in urls_to_try:
            try:
                logging.info(f"[YAHOO TRADITIONAL] Trying {ticker} from {url}")
                
                response = session.get(url, headers=headers, timeout=15)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                news_items = parse_yahoo_news(soup, ticker, target_date)
                
                if news_items:
                    logging.info(f"[YAHOO TRADITIONAL] Found {len(news_items)} news items for {ticker} from {url}")
                    return news_items
                else:
                    logging.debug(f"[YAHOO TRADITIONAL] No news found at {url}, trying next URL...")
                    
            except Exception as e:
                logging.debug(f"[YAHOO TRADITIONAL] Failed to fetch from {url}: {e}")
                continue
        
        logging.warning(f"[YAHOO TRADITIONAL] All URLs failed for {ticker}")
        return []
        
    except Exception as e:
        logging.warning(f"Error fetching Yahoo traditional news for {ticker}: {e}")
        return []

def fetch_news_playwright(ticker, session, target_date):
    """Fetch news from Yahoo Finance using Playwright."""
    try:
        url = f"https://finance.yahoo.com/quote/{ticker}/news/"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.set_extra_http_headers(headers)
            
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=8000)
                page.wait_for_timeout(2000)
                html_content = page.content()
                soup = BeautifulSoup(html_content, 'html.parser')
                logging.info(f"Yahoo Finance response (Playwright) for {ticker}")
            except Exception as e:
                logging.warning(f"Playwright navigation failed for {ticker}: {e}")
                try:
                    page.goto(url, timeout=5000)
                    page.wait_for_timeout(1000)
                    html_content = page.content()
                    soup = BeautifulSoup(html_content, 'html.parser')
                    logging.info(f"Yahoo Finance response (Playwright fallback) for {ticker}")
                except Exception as e2:
                    logging.warning(f"Playwright fallback also failed for {ticker}: {e2}")
                    soup = None
            finally:
                browser.close()
        
        if soup:
            return parse_yahoo_news(soup, ticker, target_date)
        return []
    except Exception as e:
        logging.warning(f"Playwright failed for {ticker}: {e}")
        return []

def fetch_news_finviz(ticker, session, target_date):
    """Fetch news from Finviz."""
    try:
        news_limiter.acquire()
        url = f"https://finviz.com/quote.ashx?t={ticker}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        news_items = []
        
        # Find news table
        news_table = soup.find('table', {'id': 'news-table'})
        if not news_table:
            return []
        
        target_dt = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=EASTERN_TZ)
        start_dt = target_dt - timedelta(days=2)
        end_dt = target_dt + timedelta(days=1)
        
        for row in news_table.find_all('tr'):
            try:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    time_cell = cells[0].get_text(strip=True)
                    title_cell = cells[1].get_text(strip=True)
                    
                    if not time_cell or not title_cell:
                        continue
                    
                    # Parse time (format: "Oct 03 4:30PM" or "Oct 03")
                    try:
                        if ' ' in time_cell and len(time_cell.split()) >= 2:
                            date_part = time_cell.split()[0] + ' ' + time_cell.split()[1]
                            pub_date = datetime.strptime(f"{date_part} 2025", "%b %d %Y").replace(tzinfo=EASTERN_TZ)
                        else:
                            pub_date = datetime.strptime(f"{time_cell} 2025", "%b %d %Y").replace(tzinfo=EASTERN_TZ)
                        
                        if start_dt.date() <= pub_date.date() <= end_dt.date():
                            news_items.append(f"{pub_date.strftime('%Y-%m-%d')}: {title_cell}")
                    except ValueError:
                        continue
            except Exception as e:
                logging.debug(f"Error parsing Finviz row for {ticker}: {e}")
                continue
        
        return news_items
    except Exception as e:
        logging.warning(f"Finviz failed for {ticker}: {e}")
        return []

def fetch_news_finnhub(ticker, session, target_date):
    """Fetch news from Finnhub API."""
    if not finnhub_client:
        return []
        
    try:
        finnhub_limiter.acquire()
        quote = finnhub_client.quote(ticker)
        if not isinstance(quote, dict) or quote.get('c', 0) == 0:
            logging.warning(f"Invalid or delisted ticker {ticker} or no quote data")
            return []
    except Exception as e:
        logging.warning(f"Finnhub validation error for {ticker}: {e}")
        return []

    try:
        target_dt = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=EASTERN_TZ)
        start_dt = target_dt - timedelta(days=2)
        end_dt = target_dt
        
        finnhub_limiter.acquire()
        news = finnhub_client.company_news(ticker, _from=start_dt.strftime("%Y-%m-%d"), to=end_dt.strftime("%Y-%m-%d"))
        
        news_items = []
        for item in news:
            try:
                pub_date = datetime.fromtimestamp(item["datetime"], tz=pytz.UTC).astimezone(EASTERN_TZ)
                if start_dt.date() <= pub_date.date() <= end_dt.date():
                    headline = item.get("headline", "No headline")
                    news_items.append(f"{pub_date.strftime('%Y-%m-%d')}: {headline}")
            except (ValueError, TypeError) as e:
                logging.warning(f"Invalid timestamp in Finnhub news for {ticker}: {item.get('datetime')}")
                continue
        return news_items
    except Exception as e:
        logging.error(f"Error fetching Finnhub news for {ticker}: {e}")
        return []

def fetch_news_polygon(ticker, session, target_date):
    """Fetch news from Polygon.io API using the correct API parameters."""
    if not polygon_client:
        return []
        
    try:
        target_dt = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=EASTERN_TZ)
        start_dt = target_dt - timedelta(days=2)
        end_dt = target_dt
        
        polygon_limiter.acquire()
        
        # Use correct API parameters based on documentation
        news = polygon_client.list_ticker_news(
            ticker=ticker,
            published_utc_gte=start_dt.strftime("%Y-%m-%d"),
            published_utc_lte=end_dt.strftime("%Y-%m-%d"),
            order="desc",
            limit=10,
            sort="published_utc"
        )
        
        news_items = []
        for item in news:
            try:
                # Verify this is a TickerNews object as per sample code
                if hasattr(item, 'published_utc') and hasattr(item, 'title'):
                    pub_date = datetime.strptime(item.published_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC).astimezone(EASTERN_TZ)
                    if start_dt.date() <= pub_date.date() <= end_dt.date():
                        news_items.append(f"{pub_date.strftime('%Y-%m-%d')}: {item.title}")
            except Exception as e:
                logging.warning(f"Error processing Polygon.io news for {ticker}: {e}")
        return news_items
    except Exception as e:
        logging.error(f"Error fetching Polygon.io news for {ticker}: {e}")
        return []

def fetch_news(ticker, session, target_date, sequential_mode=False):
    """Fetch news from all sources with Polygon as first priority and report source."""
    try:
        # Create a separate session for this thread's parallel operations to avoid thread safety issues
        with requests.Session() as thread_session:
            # Configure the thread session
            retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
            adapter = HTTPAdapter(
                pool_connections=10,
                pool_maxsize=10,
                max_retries=retries
            )
            thread_session.mount("https://", adapter)
            
            if sequential_mode:
                # Sequential mode: fetch news sources one at a time to avoid overwhelming Playwright
                logging.debug(f"Fetching news sources sequentially for {ticker}")
                polygon_news = fetch_news_polygon(ticker, thread_session, target_date)
                playwright_news = fetch_news_playwright(ticker, thread_session, target_date)
                finviz_news = fetch_news_finviz(ticker, thread_session, target_date)
                yahoo_news = fetch_news_yahoo_traditional(ticker, thread_session, target_date)
                finnhub_news = fetch_news_finnhub(ticker, thread_session, target_date)
            else:
            # Run all news sources in parallel with the thread-safe session (optimized for paid Polygon plan)
            with ThreadPoolExecutor(max_workers=8) as executor:
                future_playwright = executor.submit(fetch_news_playwright, ticker, thread_session, target_date)
                future_finviz = executor.submit(fetch_news_finviz, ticker, thread_session, target_date)
                future_yahoo = executor.submit(fetch_news_yahoo_traditional, ticker, thread_session, target_date)
                future_finnhub = executor.submit(fetch_news_finnhub, ticker, thread_session, target_date)
                future_polygon = executor.submit(fetch_news_polygon, ticker, thread_session, target_date)

                # Gather results by source - MOVED INSIDE the with block
                polygon_news = future_polygon.result()
                playwright_news = future_playwright.result()
                finviz_news = future_finviz.result()
                yahoo_news = future_yahoo.result()
                finnhub_news = future_finnhub.result()

        # Combine with Polygon first
        all_news = []
        all_news.extend(polygon_news)
        all_news.extend(playwright_news)
        all_news.extend(finviz_news)
        all_news.extend(yahoo_news)
        all_news.extend(finnhub_news)

        # Deduplicate by URL/title similarity
        unique_news = []
        seen_titles = set()

        for news_item in all_news:
            # Extract title from "date: title" format
            if ': ' in str(news_item):
                title = str(news_item).split(': ', 1)[1]
            else:
                title = str(news_item)

            # Simple deduplication by title similarity
            title_lower = str(title).lower()
            is_duplicate = False

            for seen_title in seen_titles:
                if title_lower == seen_title.lower():
                    is_duplicate = True
                    break
                # Check for similar titles (80% similarity)
                if len(title_lower) > 10 and len(seen_title.lower()) > 10:
                    similarity = sum(1 for a, b in zip(title_lower, seen_title.lower()) if a == b) / max(len(title_lower), len(seen_title.lower()))
                    if similarity > 0.8:
                        is_duplicate = True
                        break

            if not is_duplicate:
                seen_titles.add(title_lower)
                unique_news.append(news_item)

        # Sort by date (newest first)
        unique_news = sorted(unique_news, key=lambda x: x.split(":")[0], reverse=True)

        # Determine primary source (first non-empty, with Polygon priority)
        primary_source = "Unknown"
        for source_name, lst in (
            ("Polygon", polygon_news),
            ("Playwright", playwright_news),
            ("Finviz", finviz_news),
            ("Yahoo", yahoo_news),
            ("Finnhub", finnhub_news),
        ):
            if lst:
                primary_source = source_name
                break

        return ticker, (", ".join(unique_news) if unique_news else "No news"), primary_source

    except Exception as e:
        logging.error(f"Error in parallel news fetching for {ticker}: {e}")
        return ticker, "No news", "Unknown"

def get_nasdaq_tickers(test_mode=False):
    """Get list of NASDAQ tickers using the same logic as PM script."""
    if test_mode:
        # Hardcoded test tickers for development/testing
        test_tickers = [
            "AAPL", "MSFT", "TSLA", "GOOGL", "AMZN",  # Just 5 tickers for quick testing
        ]
        logging.info(f"Using test mode with {len(test_tickers)} hardcoded tickers")
        return test_tickers
    
    # Try to load from cached file first
    cache_file = "nasdaq_tickers_cache.txt"
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r') as f:
                tickers = [line.strip() for line in f if line.strip()]
            logging.info(f"Loaded {len(tickers)} NASDAQ tickers from cache")
            return tickers
        except Exception as e:
            logging.warning(f"Error reading cache file: {e}")
    
    # Fallback to a reasonable subset if no cache
    logging.warning("No cache file found, using subset of popular NASDAQ tickers")
    return [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 'AMD', 'INTC',
        'ADBE', 'CRM', 'PYPL', 'CMCSA', 'PEP', 'COST', 'TXN', 'QCOM', 'AVGO', 'CHTR',
        'SBUX', 'INTU', 'ISRG', 'GILD', 'BKNG', 'ADP', 'VRTX', 'FISV', 'ATVI', 'CSX',
        'REGN', 'ILMN', 'LRCX', 'ADI', 'MU', 'AMAT', 'KLAC', 'MCHP', 'SNPS', 'CDNS',
        'ORLY', 'CTAS', 'WBA', 'EXC', 'AEP', 'SO', 'XEL', 'ES', 'SRE', 'D',
        'DUK', 'NEE', 'PEG', 'AWK', 'WEC', 'ED', 'EIX', 'ETR', 'AEE', 'CMS',
        'DTE', 'FE', 'PPL', 'EXR', 'EQR', 'AVB', 'MAA', 'UDR', 'ESS', 'BXP',
        'PLD', 'PSA', 'O', 'WELL', 'AMT', 'CCI', 'SBAC', 'EQIX', 'DLR', 'EXR'
    ]

def scan_tickers_for_news(target_date, max_workers=12, test_mode=False, ticker_date_pairs=None, sequential_mode=False):
    """Scan tickers for news data only."""
    all_news = []
    
    if ticker_date_pairs:
        # Excel mode: process specific ticker-date pairs
        logging.info(f"Starting news scan for {len(ticker_date_pairs)} ticker-date pairs from Excel")
        tickers_to_process = ticker_date_pairs  # Already contains (ticker, date, row_index)
    else:
        # Command line mode: get all NASDAQ tickers and filter by PM criteria
        tickers = get_nasdaq_tickers(test_mode=test_mode)
        logging.info(f"Starting news scan for {len(tickers)} tickers on {target_date}")
        tickers_to_process = [(ticker, target_date, 0) for ticker in tickers]
    
    # Create session with retry logic
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=retries)
    session.mount("https://", adapter)
    
    # Use sequential mode if requested (max_workers=1)
    if sequential_mode:
        max_workers = 1
        logging.info("Running in SEQUENTIAL MODE - processing tickers one by one")
    
    # Process tickers in parallel (or sequentially if max_workers=1)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_ticker = {}
        for ticker_data in tickers_to_process:
            if len(ticker_data) == 3:
                ticker, date, row_index = ticker_data
            else:
                ticker, date = ticker_data
                row_index = 0
            
            # Handle None dates (from Excel parsing errors)
            if date is None:
                # Create error row immediately to preserve order
                all_news.append({
                    'ticker': ticker,
                    'date': "Invalid Date",
                    'news': "No news",
                    'news_source': "Error",
                    '_excel_row_index': row_index
                })
                logging.warning(f"Row {row_index}: Skipping {ticker} - invalid date")
                continue
            
            future_to_ticker[executor.submit(fetch_news, ticker, session, date, sequential_mode)] = (ticker, date, row_index)
        
        # Process completed tasks
        for future in as_completed(future_to_ticker):
            ticker, date, row_index = future_to_ticker[future]
            try:
                ticker_result, news_text, news_source = future.result()
                
                # Include all tickers, whether they have news or not
                if news_text and news_text != "No news":
                    all_news.append({
                        'ticker': ticker,
                        'date': date,
                        'news': news_text,
                        'news_source': news_source,
                        '_excel_row_index': row_index
                    })
                    logging.info(f"Processed {ticker} ({date}): News found from {news_source}")
                else:
                    # Include tickers with no news
                    all_news.append({
                        'ticker': ticker,
                        'date': date,
                        'news': "No news",
                        'news_source': "None",
                        '_excel_row_index': row_index
                    })
                    logging.info(f"Processed {ticker} ({date}): No news found")
            except Exception as e:
                logging.error(f"Error processing {ticker} ({date}): {e}")
                # Create error row to preserve order
                all_news.append({
                    'ticker': ticker,
                    'date': date if date else "Invalid Date",
                    'news': "No news",
                    'news_source': "Error",
                    '_excel_row_index': row_index
                })
    
    logging.info(f"News scan completed. Total news items: {len(all_news)}")
    return all_news

def read_excel_tickers(excel_filename='Tickers.xlsx'):
    """Read ticker-date pairs from Excel file (default: Tickers.xlsx)."""
    try:
        # Handle filename without .xlsx extension
        if not excel_filename.endswith('.xlsx'):
            excel_filename = f"{excel_filename}.xlsx"
        
        df = pd.read_excel(excel_filename)
        ticker_date_pairs = []
        
        logging.info(f"Processing Excel file '{excel_filename}' with {len(df)} rows")
        
        for row_index, row in df.iterrows():
            ticker = str(row['Ticker']).strip().upper()
            date_str = str(row['Date']).strip()
            
            # Skip empty or NaN values
            if pd.isna(date_str) or date_str == '' or date_str.lower() == 'nan':
                logging.warning(f"Row {row_index}: Skipping empty date for {ticker}")
                # Still add to list with None date to preserve order
                ticker_date_pairs.append((ticker, None, row_index))
                continue
                
            try:
                # Try multiple date formats and handle datetime objects
                dt = None
                
                # First try to handle datetime objects directly
                if hasattr(date_str, 'date'):
                    dt = date_str
                elif isinstance(date_str, pd.Timestamp):
                    dt = date_str.to_pydatetime()
                else:
                    # Handle string dates with time components
                    date_str_clean = str(date_str).split(' ')[0]  # Remove time part
                    date_formats = [
                        '%b %d, %Y',    # "Dec 05, 2023"
                        '%d/%m/%Y',     # "29/01/2025"
                        '%Y-%m-%d',     # "2025-01-29"
                        '%m/%d/%Y',     # "01/29/2025"
                        '%d-%m-%Y',     # "29-01-2025"
                        '%Y/%m/%d'      # "2025/01/29"
                    ]
                    
                    for date_format in date_formats:
                        try:
                            dt = datetime.strptime(date_str_clean, date_format)
                            break
                        except ValueError:
                            continue
                
                    # Fallback to pandas to_datetime if all formats fail
                if dt is None:
                        try:
                            dt = pd.to_datetime(date_str_clean)
                            if pd.notna(dt):
                                dt = dt.to_pydatetime()
                        except:
                            pass
                
                if dt is None:
                    logging.error(f"Row {row_index}: Invalid date format in Excel: {date_str} for {ticker}")
                    # Still add to list with None date to preserve order
                    ticker_date_pairs.append((ticker, None, row_index))
                    continue
                
                # Convert to string format
                formatted_date = dt.strftime('%Y-%m-%d')
                ticker_date_pairs.append((ticker, formatted_date, row_index))
                
            except Exception as e:
                logging.error(f"Row {row_index}: Error processing row for {ticker}: {e}")
                # Still add to list with None date to preserve order
                ticker_date_pairs.append((ticker, None, row_index))
                continue
        
        logging.info(f"Successfully parsed {len(ticker_date_pairs)} ticker-date pairs from Excel")
        return ticker_date_pairs
        
    except FileNotFoundError:
        logging.error(f"Excel file '{excel_filename}' not found")
        return None
    except Exception as e:
        logging.error(f"Error reading Excel file '{excel_filename}': {e}")
        return None

def main():
    """Main function to run the news scanner."""
    import sys
    
    # print("News-Only Scanner")
    # print("=" * 50)
    
    # Check for sequential mode argument
    sequential_mode = False
    if 'seq' in [arg.lower() for arg in sys.argv]:
        sequential_mode = True
        logging.info("Sequential mode enabled - processing tickers one by one")
    
    # Check for command line arguments (excluding 'seq')
    args = [arg for arg in sys.argv[1:] if arg.lower() != 'seq']
    
    # Check if first argument is an Excel filename (tickers, tickers2, tickers3, etc.)
    excel_filename = None
    excel_mode = False
    
    if len(args) > 0:
        # Check if argument looks like an Excel filename (not a date)
        first_arg = args[0].lower()
        if first_arg in ['tickers', 'tickers2', 'tickers3'] or first_arg.startswith('tickers'):
            excel_filename = args[0]
            excel_mode = True
            target_date = None
            logging.info(f"Excel mode: Reading from {excel_filename}.xlsx")
        else:
            # Assume it's a date
            target_date = args[0]
        # print(f"Using date from command line: {target_date}")
        excel_mode = False
    else:
        # Check if Tickers.xlsx exists for Excel mode
        if os.path.exists('Tickers.xlsx'):
            # print("Found Tickers.xlsx file - using Excel mode")
            excel_filename = 'Tickers'
            excel_mode = True
            target_date = None
        else:
            # Get target date from user input
            target_date = input("Enter target date (YYYY-MM-DD) or press Enter for today: ").strip()
            if not target_date:
                target_date = datetime.now(EASTERN_TZ).strftime("%Y-%m-%d")
            excel_mode = False
    
    # Check for test mode argument (excluding 'seq')
    test_mode = False
    if len(args) > 1 and not excel_mode:
        test_mode = args[1].lower() in ['test', 't', 'true', '1', 'yes', 'y']
    else:
        # Ask for test mode (only in command line mode)
        if not excel_mode:
            test_mode_input = input("Run in test mode with 5 tickers? (y/n, default: n): ").strip().lower()
            test_mode = test_mode_input in ['y', 'yes']
    
    if excel_mode:
        # print(f"Reading ticker-date pairs from {excel_filename}.xlsx...")
        ticker_date_pairs = read_excel_tickers(excel_filename)
        if not ticker_date_pairs:
            # print("No valid ticker-date pairs found in Excel file")
            return
        # print(f"Found {len(ticker_date_pairs)} ticker-date pairs")
    else:
        ticker_date_pairs = None
        if test_mode:
            # print(f"Running in TEST MODE with 5 tickers on {target_date}")
            pass
        else:
            # print(f"Scanning for news on {target_date}")
            # print("This may take several minutes...")
            pass
    
    # Scan for news
    start_time = time.time()
    news_data = scan_tickers_for_news(target_date, test_mode=test_mode, ticker_date_pairs=ticker_date_pairs, sequential_mode=sequential_mode)
    end_time = time.time()
    
    # Calculate timing
    total_time = end_time - start_time
    total_minutes = total_time / 60
    
    # Create DataFrame
    if news_data:
        df = pd.DataFrame(news_data)
        
        # Sort by Excel row order if available (using internal _excel_row_index column)
        if '_excel_row_index' in df.columns:
            df = df.sort_values('_excel_row_index').reset_index(drop=True)
            # Remove the internal _excel_row_index column after sorting
            df = df.drop('_excel_row_index', axis=1)
            logging.info("Results sorted by Excel row order")
        
        # Save to Excel
        if excel_mode:
            output_file = f"news_scan_excel_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        else:
            output_file = f"news_scan_{target_date.replace('-', '')}.xlsx"
        df.to_excel(output_file, index=False)
        
        # Show completion message like stock_data_pm.py
        print(f"[SUCCESS] Processed {len(news_data)} news items in {total_minutes:.1f} minutes")
        print(f"[SCRIPT COMPLETION] Total execution time: {total_minutes:.1f} minutes")
        
        # Show sample results
        # print(f"\nSample results:")
        # print(df.head(10).to_string(index=False))
        
    else:
        print("No news items found for the specified criteria.")
        print(f"[SCRIPT COMPLETION] Total execution time: {total_minutes:.1f} minutes")

if __name__ == "__main__":
    main()
