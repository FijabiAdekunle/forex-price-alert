import os
import ssl
import time
import json
import requests
import pandas as pd
import numpy as np
import telegram
import psycopg2
import gspread
import asyncio
import traceback
from datetime import datetime, timedelta
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
from psycopg2 import OperationalError

# Temporary SSL fix (remove after successful testing)
ssl._create_default_https_context = ssl._create_unverified_context

# Load environment variables
load_dotenv()

# Configuration
PAIRS = {
    "EUR/USD": "EUR/USD",
    "GBP/USD": "GBP/USD",
    "USD/JPY": "USD/JPY"
}

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")

# Database Configuration
DB_CONFIG = {
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
    "dbname": os.getenv("PG_DB"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD")
}

# Initialize Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(os.getenv("GSPREAD_KEY_JSON")), scope)
sheets_client = gspread.authorize(creds)

# Enhanced logging
def log_message(msg, level="INFO"):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {level}: {msg}")

# Database Connection with SSL
def get_db_connection(retries=3, delay=2):
    """Establish connection to Supabase with SSL and retries"""
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(
                **DB_CONFIG,
                sslmode="require",
                connect_timeout=5
            )
            
            # Test connection
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                if cur.fetchone()[0] != 1:
                    raise OperationalError("Connection test failed")
            
            log_message("✅ Supabase connection established")
            return conn
            
        except OperationalError as e:
            log_message(f"⚠️ Connection attempt {attempt + 1} failed: {str(e)}", "WARNING")
            if attempt < retries - 1:
                time.sleep(delay)
    
    raise OperationalError("Could not establish database connection")

def update_supabase(data):
    """Insert data into Supabase with proper error handling"""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO forex_history (
                    timestamp, pair, open, high, low, close,
                    ema10, ema50, rsi, atr, support, resistance,
                    trend_direction, sentiment_summary, news_summary
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data["timestamp"], data["pair"],
                data["open"], data["high"], data["low"], data["close"],
                data["ema10"], data["ema50"],
                data["rsi"], data["atr"],
                data["support"], data["resistance"],
                data["trend_direction"], data["sentiment_summary"], data["news_summary"]
            ))
        conn.commit()
        log_message(f"✅ Successfully updated Supabase for {data['pair']}")
        
    except Exception as e:
        log_message(f"❌ Supabase update failed for {data['pair']}: {str(e)}", "ERROR")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

# Data Fetching and Processing
def fetch_data(symbol):
    """Fetch forex data from Twelve Data API"""
    try:
        params = {
            "symbol": symbol,
            "interval": "15min",
            "outputsize": 100,
            "apikey": os.getenv("TWELVE_DATA_API_KEY")
        }
        
        log_message(f"Fetching data for {symbol}")
        res = requests.get(
            "https://api.twelvedata.com/time_series",
            params=params,
            timeout=15
        )
        data = res.json()
        
        if "values" not in data:
            raise ValueError(f"API error: {data.get('message', 'Unknown error')}")
            
        df = pd.DataFrame(data["values"])
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values("datetime")
        df.set_index("datetime", inplace=True)
        
        numeric_cols = ["open", "high", "low", "close"]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
        
        log_message(f"Fetched {len(df)} records for {symbol}")
        return df
        
    except Exception as e:
        log_message(f"❌ Data fetch failed for {symbol}: {str(e)}", "ERROR")
        raise

def compute_indicators(df):
    """Calculate technical indicators with enhanced EMA crossover detection"""
    try:
        # EMAs
        df["ema10"] = df["close"].ewm(span=10, adjust=False).mean()
        df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()
        
        # EMA Crossover Signal (1 = bullish, -1 = bearish, 0 = neutral)
        df["ema_signal"] = np.where(
            (df["ema10"] > df["ema50"]) & (df["ema10"].shift() <= df["ema50"].shift()), 1,
            np.where(
                (df["ema10"] < df["ema50"]) & (df["ema10"].shift() >= df["ema50"].shift()), -1, 0
            )
        )
        
        # RSI
        delta = df["close"].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        df["rsi"] = 100 - (100 / (1 + rs))
        
        # ATR (Corrected calculation)
        high_low = df["high"] - df["low"]
        high_close = (df["high"] - df["close"].shift()).abs()
        low_close = (df["low"] - df["close"].shift()).abs()
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df["atr"] = true_range.rolling(14).mean()
        
        return df
        
    except Exception as e:
        log_message(f"❌ Indicator calculation failed: {str(e)}", "ERROR")
        raise

def detect_levels(df):
    """Calculate support/resistance with Fibonacci levels"""
    latest = df.iloc[-1]
    
    # Support/Resistance
    support = df["low"].rolling(20).min().iloc[-1]
    resistance = df["high"].rolling(20).max().iloc[-1]
    
    # Fibonacci Levels
    recent_low = df["low"].iloc[-20:].min()
    recent_high = df["high"].iloc[-20:].max()
    range_diff = recent_high - recent_low
    
    fib_levels = {
        "23.6": recent_high - range_diff * 0.236,
        "38.2": recent_high - range_diff * 0.382,
        "50.0": recent_high - range_diff * 0.5,
        "61.8": recent_high - range_diff * 0.618
    }
    
    return support, resistance, fib_levels

# News and Sentiment Analysis
def fetch_sentiment(pair):
    """Get sentiment from multiple sources"""
    try:
        symbol = pair.replace("/", "")
        url = "https://api.twelvedata.com/sentiment"
        params = {
            "symbol": symbol,
            "apikey": os.getenv("TWELVE_DATA_API_KEY")
        }
        
        res = requests.get(url, params=params, timeout=10)
        data = res.json()
        
        if data.get("status") == "ok" and data.get("data"):
            sentiment = data["data"][0].get("sentiment", "N/A")
            strength = data["data"][0].get("strength", "")
            return f"{sentiment} ({strength})" if strength else sentiment
            
    except Exception as e:
        log_message(f"⚠️ TwelveData sentiment failed for {pair}: {str(e)}", "WARNING")
    
    return "N/A"

def fetch_news(pair):
    """Get news from multiple sources"""
    try:
        symbol = pair.replace("/", "")
        url = "https://api.twelvedata.com/news"
        params = {
            "symbol": symbol,
            "apikey": os.getenv("TWELVE_DATA_API_KEY"),
            "limit": 3
        }
        
        res = requests.get(url, params=params, timeout=10)
        data = res.json()
        
        if data.get("data"):
            return " | ".join([
                f"{item['title']} ({item['source']})" 
                for item in data["data"][:3]
            ])
            
    except Exception as e:
        log_message(f"⚠️ TwelveData news failed for {pair}: {str(e)}", "WARNING")
    
    return "No major news"

# Alert Generation
async def send_telegram_alert(row):
    """Send formatted alert to Telegram"""
    try:
        bot = telegram.Bot(token=TELEGRAM_TOKEN)
        
        # Determine trend emoji
        if row["ema_signal"] == 1:
            emoji = "📈 BULLISH"
        elif row["ema_signal"] == -1:
            emoji = "📉 BEARISH"
        else:
            emoji = "➡️ NEUTRAL"
        
        # Format message
        message = (
            f"{emoji} CROSSOVER ALERT: {row['pair']}\n"
            f"🕒 {row['timestamp']}\n"
            f"Price: {row['close']:.5f}\n"
            f"EMA10/50: {row['ema10']:.5f}/{row['ema50']:.5f}\n"
            f"RSI: {row['rsi']:.2f} | ATR: {row['atr']:.5f}\n"
            f"Support: {row['support']:.5f} | Resistance: {row['resistance']:.5f}\n"
            f"Sentiment: {row['sentiment_summary']}\n"
            f"News: {row['news_summary']}\n"
            "#forex #alerts"
        )
        
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=message,
            parse_mode="Markdown"
        )
        log_message(f"✅ Telegram alert sent for {row['pair']}")
        
    except Exception as e:
        log_message(f"❌ Telegram alert failed for {row['pair']}: {str(e)}", "ERROR")

# Main Pipeline
async def process_pair(pair):
    """Process data for a single currency pair"""
    try:
        symbol = PAIRS[pair]
        
        # Fetch and process data
        df = fetch_data(symbol)
        df = compute_indicators(df)
        support, resistance, fib_levels = detect_levels(df)
        latest = df.iloc[-1]
        
        # Prepare data row
        row = {
            "timestamp": datetime.utcnow(),
            "pair": pair,
            "open": latest["open"],
            "high": latest["high"],
            "low": latest["low"],
            "close": latest["close"],
            "ema10": latest["ema10"],
            "ema50": latest["ema50"],
            "ema_signal": latest["ema_signal"],
            "rsi": latest["rsi"],
            "atr": latest["atr"],
            "support": support,
            "resistance": resistance,
            "fib_levels": json.dumps(fib_levels),
            "trend_direction": "Uptrend" if latest["ema_signal"] == 1 else "Downtrend" if latest["ema_signal"] == -1 else "Neutral",
            "sentiment_summary": fetch_sentiment(pair),
            "news_summary": fetch_news(pair)
        }
        
        # Send alerts and update databases
        await send_telegram_alert(row)
        update_supabase(row)
        update_google_sheets(row)
        
        return row
        
    except Exception as e:
        log_message(f"❌ Processing failed for {pair}: {str(e)}", "ERROR")
        raise

def update_google_sheets(data):
    """Update Google Sheets"""
    try:
        sheet = sheets_client.open(GOOGLE_SHEET_NAME).sheet1
        sheet.append_row([
            data["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
            data["pair"],
            data["open"],
            data["high"],
            data["low"],
            data["close"],
            data["ema10"],
            data["ema50"],
            data["rsi"],
            data["atr"],
            data["support"],
            data["resistance"],
            data["trend_direction"],
            data["sentiment_summary"],
            data["news_summary"]
        ])
        log_message(f"✅ Google Sheets updated for {data['pair']}")
    except Exception as e:
        log_message(f"❌ Google Sheets update failed for {data['pair']}: {str(e)}", "ERROR")

async def main():
    """Main execution function"""
    try:
        log_message("=== Starting Forex Pipeline ===")
        
        # Process all pairs concurrently
        tasks = [process_pair(pair) for pair in PAIRS]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Check for failures
        if any(isinstance(result, Exception) for result in results):
            raise Exception("Some currency pairs failed to process")
            
        log_message("=== Pipeline Completed Successfully ===")
        
    except Exception as e:
        log_message(f"❌ Pipeline failed: {str(e)}", "ERROR")
        log_message(traceback.format_exc(), "DEBUG")
        raise

if __name__ == "__main__":
    asyncio.run(main())