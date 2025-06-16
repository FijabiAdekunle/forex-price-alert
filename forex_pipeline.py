import os
import pandas as pd
import requests
import telegram
import psycopg2
import gspread
from datetime import datetime, timedelta
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import numpy as np
import asyncio
import traceback

load_dotenv()

# Configuration
PAIRS = {
    "EUR/USD": "EUR/USD",
    "GBP/USD": "GBP/USD",
    "USD/JPY": "USD/JPY"
}

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
POSTGRES_URL = os.getenv("POSTGRES_URL")  # Format: postgresql://user:password@host:port/dbname
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")

# Enhanced logging
def log_message(msg, level="INFO"):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {level}: {msg}")

# Database connection with retries
def get_db_connection(retries=3, delay=2):
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(POSTGRES_URL)
            log_message("Successfully connected to Supabase")
            return conn
        except Exception as e:
            log_message(f"Supabase connection attempt {attempt + 1} failed: {str(e)}", "WARNING")
            if attempt < retries - 1:
                time.sleep(delay)
    raise Exception("Could not establish database connection")

# Enhanced data fetching with caching
def fetch_data(symbol):
    try:
        api_key = os.getenv("TWELVE_DATA_API_KEY")
        url = "https://api.twelvedata.com/time_series"
        params = {
            "symbol": symbol,
            "interval": "15min",
            "outputsize": 100,  # Increased for better EMA/ATR calculation
            "apikey": api_key,
            "timezone": "UTC"
        }
        
        log_message(f"Fetching data for {symbol}")
        res = requests.get(url, params=params, timeout=15)
        data = res.json()
        
        if "values" not in data:
            raise ValueError(f"API response error: {data.get('message', 'Unknown error')}")
            
        df = pd.DataFrame(data["values"])
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values("datetime")
        df.set_index("datetime", inplace=True)
        
        # Convert numeric columns
        numeric_cols = ["open", "high", "low", "close"]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
        
        log_message(f"Successfully fetched {len(df)} records for {symbol}")
        return df
        
    except Exception as e:
        log_message(f"Error fetching data for {symbol}: {str(e)}", "ERROR")
        raise

# Enhanced indicator calculation
def compute_indicators(df):
    try:
        # EMAs
        df["ema10"] = df["close"].ewm(span=10, adjust=False).mean()
        df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()
        
        # EMA Crossover signal (1 for bullish, -1 for bearish, 0 for neutral)
        df["ema_signal"] = np.where(df["ema10"] > df["ema50"], 1, 
                                  np.where(df["ema10"] < df["ema50"], -1, 0))
        
        # RSI
        delta = df["close"].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        df["rsi"] = 100 - (100 / (1 + rs))
        
        # ATR (corrected calculation)
        high_low = df["high"] - df["low"]
        high_close = (df["high"] - df["close"].shift()).abs()
        low_close = (df["low"] - df["close"].shift()).abs()
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df["atr"] = true_range.rolling(14).mean()
        
        return df
        
    except Exception as e:
        log_message(f"Error computing indicators: {str(e)}", "ERROR")
        raise

# Support/Resistance with Fibonacci levels
def detect_levels(df):
    latest = df.iloc[-1]
    
    # Basic support/resistance
    support = df["low"].rolling(20).min().iloc[-1]
    resistance = df["high"].rolling(20).max().iloc[-1]
    
    # Fibonacci levels (if you want to add them)
    recent_low = df["low"].iloc[-20:].min()
    recent_high = df["high"].iloc[-20:].max()
    fib_levels = {
        "fib_23.6": recent_high - (recent_high - recent_low) * 0.236,
        "fib_38.2": recent_high - (recent_high - recent_low) * 0.382,
        "fib_50.0": recent_high - (recent_high - recent_low) * 0.5,
        "fib_61.8": recent_high - (recent_high - recent_low) * 0.618
    }
    
    return support, resistance, fib_levels

# Enhanced sentiment analysis
def fetch_sentiment(pair):
    sources = {
        "tradingview": fetch_tradingview_sentiment,
        "twelvedata": fetch_twelve_data_sentiment
    }
    
    for source_name, func in sources.items():
        try:
            sentiment = func(pair)
            if sentiment != "N/A":
                return f"{source_name}: {sentiment}"
        except Exception as e:
            log_message(f"{source_name} sentiment error for {pair}: {str(e)}", "WARNING")
    
    return "N/A"

def fetch_twelve_data_sentiment(pair):
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
            return data["data"][0].get("sentiment", "N/A")
    except Exception as e:
        log_message(f"TwelveData sentiment error: {str(e)}", "WARNING")
    return "N/A"

# News analysis with multiple sources
def fetch_news(pair):
    sources = {
        "forexfactory": fetch_forex_factory_news,
        "twelvedata": fetch_twelve_data_news
    }
    
    news_items = []
    for source_name, func in sources.items():
        try:
            news = func(pair)
            if news and news != "No major news":
                news_items.append(f"{source_name}: {news}")
        except Exception as e:
            log_message(f"{source_name} news error for {pair}: {str(e)}", "WARNING")
    
    return " | ".join(news_items[:3]) if news_items else "No major news"

def fetch_twelve_data_news(pair):
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
            return "; ".join([item["title"] for item in data["data"][:3]])
    except Exception as e:
        log_message(f"TwelveData news error: {str(e)}", "WARNING")
    return "No major news"

# Main pipeline with enhanced features
async def main():
    try:
        log_message("Starting forex pipeline")
        
        # Initialize services
        google_sheet = await initialize_google_sheets()
        db_conn = get_db_connection()
        
        rows = []
        for pair in PAIRS:
            try:
                symbol = PAIRS[pair]
                
                # Fetch and process data
                df = fetch_data(symbol)
                df = compute_indicators(df)
                support, resistance, fib_levels = detect_levels(df)
                latest = df.iloc[-1]
                
                # Determine trend based on EMA crossover
                trend = "Uptrend" if latest["ema_signal"] == 1 else "Downtrend" if latest["ema_signal"] == -1 else "Neutral"
                
                # Get sentiment and news
                sentiment = fetch_sentiment(pair)
                news = fetch_news(pair)
                
                # Prepare row data
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
                    "trend": trend,
                    "sentiment": sentiment,
                    "news": news,
                    "fib_levels": json.dumps(fib_levels)
                }
                rows.append(row)
                
                # Send Telegram alert
                await send_telegram_alert(row)
                
            except Exception as e:
                log_message(f"Error processing {pair}: {str(e)}", "ERROR")
                continue
        
        # Update databases
        if rows:
            update_supabase(db_conn, rows)
            update_google_sheets(google_sheet, rows)
            
        log_message("Pipeline completed successfully")
        
    except Exception as e:
        log_message(f"Pipeline failed: {str(e)}", "ERROR")
        log_message(traceback.format_exc(), "DEBUG")
    finally:
        if 'db_conn' in locals():
            db_conn.close()

async def send_telegram_alert(row):
    try:
        bot = telegram.Bot(token=TELEGRAM_TOKEN)
        
        # Prepare alert message with emoji based on trend
        emoji = "📈" if row["trend"] == "Uptrend" else "📉" if row["trend"] == "Downtrend" else "➡️"
        message = f"{emoji} <b>{row['pair']} {row['trend'].upper()}</b>\n"
        message += f"🕒 {row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}\n"
        message += f"Price: {row['close']:.5f} | RSI: {row['rsi']:.2f}\n"
        message += f"EMA10/50: {row['ema10']:.5f}/{row['ema50']:.5f}\n"
        message += f"ATR: {row['atr']:.5f}\n"
        message += f"Support: {row['support']:.5f} | Resistance: {row['resistance']:.5f}\n"
        message += f"Sentiment: {row['sentiment']}\n"
        message += f"News: {row['news']}\n"
        message += "#forex #alerts"
        
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=message,
            parse_mode="HTML"
        )
        
    except Exception as e:
        log_message(f"Telegram alert failed: {str(e)}", "ERROR")

def update_supabase(conn, rows):
    try:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute("""
                    INSERT INTO forex_data (
                        timestamp, pair, open, high, low, close,
                        ema10, ema50, ema_signal, rsi, atr,
                        support, resistance, trend, sentiment, news, fib_levels
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row["timestamp"], row["pair"], row["open"], row["high"], row["low"], row["close"],
                    row["ema10"], row["ema50"], row["ema_signal"], row["rsi"], row["atr"],
                    row["support"], row["resistance"], row["trend"], row["sentiment"], row["news"], row["fib_levels"]
                ))
        conn.commit()
        log_message(f"Successfully updated Supabase with {len(rows)} records")
    except Exception as e:
        log_message(f"Supabase update failed: {str(e)}", "ERROR")
        conn.rollback()

if __name__ == "__main__":
    asyncio.run(main())