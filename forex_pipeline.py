#!/usr/bin/env python3
import os
import pandas as pd
import requests
import telegram
import gspread
import psycopg2
from datetime import datetime, time as dt_time
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import numpy as np
import asyncio
import logging
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('forex_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
PAIRS = {
    "EUR/USD": "EUR/USD",
    "GBP/USD": "GBP/USD",
    "USD/JPY": "USD/JPY"
}

# Initialize services
try:
    # Google Sheets
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("gspread_key.json", scope)
    gs_client = gspread.authorize(creds)
    sheet = gs_client.open(os.getenv("GOOGLE_SHEET_NAME")).sheet1
    logger.info("Google Sheets initialized")
except Exception as e:
    logger.error(f"Google Sheets init failed: {str(e)}")
    sheet = None

try:
    # Telegram
    bot = telegram.Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
    logger.info("Telegram bot initialized")
except Exception as e:
    logger.error(f"Telegram init failed: {str(e)}")
    bot = None

try:
    # Supabase
    conn = psycopg2.connect(os.getenv("POSTGRES_URL"))
    logger.info("Supabase connected")
except Exception as e:
    logger.error(f"Supabase connection failed: {str(e)}")
    conn = None

def fetch_data(symbol):
    """Fetch market data from Twelve Data API"""
    try:
        url = "https://api.twelvedata.com/time_series"
        params = {
            "symbol": symbol,
            "interval": "15min",
            "outputsize": 50,
            "apikey": os.getenv("TWELVE_DATA_API_KEY")
        }
        response = requests.get(url, params=params, timeout=15)
        data = response.json()
        df = pd.DataFrame(data["values"])
        df["datetime"] = pd.to_datetime(df["datetime"])
        return df.sort_values("datetime").set_index("datetime")
    except Exception as e:
        logger.error(f"Data fetch failed for {symbol}: {str(e)}")
        return None

def compute_indicators(df):
    """Calculate technical indicators"""
    try:
        # EMAs
        df["ema10"] = df["close"].ewm(span=10, adjust=False).mean()
        df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()
        
        # RSI
        delta = df["close"].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df["rsi"] = 100 - (100 / (1 + rs))
        
        # ATR
        high_low = df["high"] - df["low"]
        high_close = (df["high"] - df["close"].shift()).abs()
        low_close = (df["low"] - df["close"].shift()).abs()
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df["atr"] = true_range.rolling(window=14).mean()
        
        return df
    except Exception as e:
        logger.error(f"Indicator calculation failed: {str(e)}")
        return None

def detect_levels(df):
    """Calculate support and resistance"""
    try:
        support = df["low"].rolling(10).min().iloc[-1]
        resistance = df["high"].rolling(10).max().iloc[-1]
        return round(support, 5), round(resistance, 5)
    except Exception as e:
        logger.error(f"Level detection failed: {str(e)}")
        return 0.0, 0.0

def get_crossover_status(prev_ema10, prev_ema50, curr_ema10, curr_ema50):
    """Determine EMA crossover status"""
    try:
        if prev_ema10 < prev_ema50 and curr_ema10 > curr_ema50:
            return "Bullish Crossover"
        elif prev_ema10 > prev_ema50 and curr_ema10 < curr_ema50:
            return "Bearish Crossover"
        elif curr_ema10 > curr_ema50:
            diff = ((curr_ema10 - curr_ema50) / curr_ema50) * 100
            return f"EMA10 > EMA50 by {diff:.2f}% (Bullish)"
        else:
            diff = ((curr_ema50 - curr_ema10) / curr_ema10) * 100
            return f"EMA10 < EMA50 by {diff:.2f}% (Bearish)"
    except Exception as e:
        logger.error(f"Crossover detection failed: {str(e)}")
        return "Crossover Unknown"

def fetch_sentiment(pair):
    """Fetch market sentiment"""
    try:
        # Implement your sentiment analysis here
        return "Neutral (No clear signal)"
    except Exception as e:
        logger.error(f"Sentiment fetch failed: {str(e)}")
        return "Sentiment Unavailable"

def fetch_news(pair):
    """Fetch relevant news"""
    try:
        if datetime.utcnow().weekday() >= 5:
            return "Weekend: No scheduled news"
        return "No major news"
    except Exception as e:
        logger.error(f"News fetch failed: {str(e)}")
        return "News Unavailable"

async def send_telegram_alert(message):
    """Send alert to Telegram"""
    if not bot:
        logger.error("Telegram bot not initialized")
        return False
    try:
        await bot.send_message(
            chat_id=os.getenv("TELEGRAM_CHAT_ID"),
            text=message,
            parse_mode="Markdown"
        )
        return True
    except Exception as e:
        logger.error(f"Telegram send failed: {str(e)}")
        return False

def save_to_supabase(row):
    """Save data to Supabase"""
    if not conn:
        logger.error("Supabase connection not available")
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO forex_analysis (
                timestamp, pair, open, high, low, close,
                ema10, ema50, rsi, atr, support, resistance,
                trend_direction, crossover, sentiment_summary, news_summary
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row["timestamp"], row["pair"], row["open"], row["high"], row["low"], row["close"],
            row["ema10"], row["ema50"], row["rsi"], row["atr"], row["support"], row["resistance"],
            row["trend_direction"], row["crossover"], row["sentiment_summary"], row["news_summary"]
        ))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Supabase save failed: {str(e)}")
        return False

def append_to_sheet(row):
    """Append data to Google Sheet"""
    if not sheet:
        logger.error("Google Sheets not initialized")
        return False
    try:
        sheet.append_row([
            row["timestamp"], row["pair"], row["open"], row["high"], row["low"], row["close"],
            row["ema10"], row["ema50"], row["rsi"], row["atr"], row["support"], row["resistance"],
            row["trend_direction"], row["crossover"], row["sentiment_summary"], row["news_summary"]
        ])
        return True
    except Exception as e:
        logger.error(f"Google Sheets append failed: {str(e)}")
        return False

def generate_alert_message(row):
    """Generate formatted alert message"""
    return f"""
🚨 {row['pair']} {row['trend_direction']}
🕒 {row['timestamp']}
💰 Price: {row['close']:.5f} | RSI: {row['rsi']:.2f}
📊 EMA10: {row['ema10']:.5f} | EMA50: {row['ema50']:.5f}
🔀 {row['crossover']} | ATR: {row['atr']:.5f}
🔽 Support: {row['support']:.5f} | 🔼 Resistance: {row['resistance']:.5f}
📢 Sentiment: {row['sentiment_summary']}
🗞️ News: {row['news_summary']}
"""

def main():
    logger.info("Starting forex pipeline")
    alerts = []
    
    for pair_name, pair_symbol in PAIRS.items():
        try:
            logger.info(f"Processing {pair_name}")
            
            # Fetch and process data
            df = fetch_data(pair_symbol)
            if df is None:
                continue
                
            df = compute_indicators(df)
            if df is None:
                continue
                
            # Get current and previous values
            latest = df.iloc[-1]
            prev = df.iloc[-2]
            
            # Calculate levels and trends
            support, resistance = detect_levels(df)
            crossover = get_crossover_status(prev["ema10"], prev["ema50"], latest["ema10"], latest["ema50"])
            trend = "Uptrend" if latest["ema10"] > latest["ema50"] else "Downtrend"
            
            # Prepare alert data
            alert_data = {
                "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "pair": pair_name,
                "open": latest["open"],
                "high": latest["high"],
                "low": latest["low"],
                "close": latest["close"],
                "ema10": latest["ema10"],
                "ema50": latest["ema50"],
                "rsi": latest["rsi"],
                "atr": latest["atr"],
                "support": support,
                "resistance": resistance,
                "trend_direction": trend,
                "crossover": crossover,
                "sentiment_summary": fetch_sentiment(pair_name),
                "news_summary": fetch_news(pair_name)
            }
            
            alerts.append(alert_data)
            
        except Exception as e:
            logger.error(f"Failed to process {pair_name}: {str(e)}")
            continue
    
    # Send alerts and save data
    for alert in alerts:
        try:
            message = generate_alert_message(alert)
            
            # Send Telegram alert
            if not asyncio.run(send_telegram_alert(message)):
                logger.error(f"Failed to send Telegram alert for {alert['pair']}")
            
            # Save to Google Sheets
            if not append_to_sheet(alert):
                logger.error(f"Failed to update Google Sheets for {alert['pair']}")
            
            # Save to Supabase
            if not save_to_supabase(alert):
                logger.error(f"Failed to update Supabase for {alert['pair']}")
                
        except Exception as e:
            logger.error(f"Failed to process output for {alert['pair']}: {str(e)}")
    
    logger.info("Forex pipeline completed")

if __name__ == "__main__":
    main()