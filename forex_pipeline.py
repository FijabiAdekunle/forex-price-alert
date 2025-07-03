#!/usr/bin/env python3
import os
import pandas as pd
import requests
import telegram
import gspread
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import numpy as np
import asyncio
import logging
import time
from typing import Dict, Tuple, Optional

# Load environment variables
load_dotenv()

# Configuration
PAIRS = {
    "EUR/USD": "EUR/USD",
    "GBP/USD": "GBP/USD",
    "USD/JPY": "USD/JPY"
}

# Constants
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}
REQUEST_DELAY = 2  # seconds between API calls

# ENV variables
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
POSTGRES_URL = os.getenv("POSTGRES_URL")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")

# Logging setup
logging.basicConfig(
    filename="forex_pipeline.log",
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def log_message(msg: str, level: str = "INFO"):
    """Log messages with timestamp."""
    print(f"[{datetime.utcnow()}] {msg}")
    if level == "INFO":
        logging.info(msg)
    elif level == "WARNING":
        logging.warning(msg)
    elif level == "ERROR":
        logging.error(msg)

# Google Sheets Setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("gspread_key.json", scope)
client = gspread.authorize(creds)
sheet = client.open(GOOGLE_SHEET_NAME).sheet1

def fetch_data(symbol: str, retries: int = 3) -> pd.DataFrame:
    """Fetch forex data from Twelve Data API with retry logic."""
    api_key = os.getenv("TWELVE_DATA_API_KEY")
    url = "https://api.twelvedata.com/time_series"
    
    for attempt in range(retries):
        try:
            params = {
                "symbol": symbol,
                "interval": "15min",
                "outputsize": 100,  # Increased for better EMA calculation
                "apikey": api_key,
                "timezone": "UTC"
            }
            
            response = requests.get(url, params=params, headers=REQUEST_HEADERS, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if "values" not in data:
                raise ValueError(f"Invalid API response: {data.get('message', 'No values key')}")
                
            df = pd.DataFrame(data["values"])
            df["datetime"] = pd.to_datetime(df["datetime"])
            df = df.sort_values("datetime")
            df.set_index("datetime", inplace=True)
            
            # Convert to numeric and handle potential missing data
            numeric_cols = ["open", "high", "low", "close"]
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
            df = df.dropna()
            
            return df
            
        except Exception as e:
            if attempt < retries - 1:
                wait_time = (attempt + 1) * 5
                log_message(f"⚠️ Retry {attempt + 1}/{retries} for {symbol} in {wait_time}s. Error: {str(e)}", "WARNING")
                time.sleep(wait_time)
                continue
            raise ValueError(f"Failed to fetch data for {symbol} after {retries} attempts: {str(e)}")

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate technical indicators."""
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
        log_message(f"❌ Indicator calculation error: {str(e)}", "ERROR")
        raise

def detect_levels(df: pd.DataFrame, window: int = 20) -> Tuple[float, float]:
    """Detect support and resistance levels."""
    try:
        # Use recent data for more relevant levels
        recent_df = df.iloc[-window:]
        support = recent_df["low"].min()
        resistance = recent_df["high"].max()
        return round(support, 5), round(resistance, 5)
    except Exception as e:
        log_message(f"❌ Level detection error: {str(e)}", "ERROR")
        return 0.0, 0.0

def get_crossover_status(current_ema10: float, current_ema50: float,
                       prev_ema10: float, prev_ema50: float) -> str:
    """Determine EMA crossover status with more detailed information."""
    try:
        if prev_ema10 < prev_ema50 and current_ema10 > current_ema50:
            return "Bullish Crossover (Golden Cross)"
        elif prev_ema10 > prev_ema50 and current_ema10 < current_ema50:
            return "Bearish Crossover (Death Cross)"
        elif current_ema10 > current_ema50:
            diff_percent = ((current_ema10 - current_ema50) / current_ema50) * 100
            return f"EMA10 > EMA50 by {diff_percent:.2f}% (Bullish)"
        else:
            diff_percent = ((current_ema50 - current_ema10) / current_ema10) * 100
            return f"EMA10 < EMA50 by {diff_percent:.2f}% (Bearish)"
    except Exception as e:
        log_message(f"❌ Crossover detection error: {str(e)}", "ERROR")
        return "Crossover Unknown"

def fetch_tradingview_sentiment(pair: str) -> str:
    """Fetch market sentiment from TradingView with improved reliability."""
    try:
        symbol_map = {
            "EUR/USD": "EURUSD",
            "GBP/USD": "GBPUSD",
            "USD/JPY": "USDJPY"
        }
        url = f"https://www.tradingview.com/symbols/{symbol_map[pair]}/technicals/"
        
        response = requests.get(url, headers=REQUEST_HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Method 1: Check technical summary
        summary = soup.find("div", class_="speedometerWrapper-3tq6nXeC")
        if summary:
            sentiment = summary.get_text().strip().upper()
            if "BUY" in sentiment:
                return "Bullish"
            elif "SELL" in sentiment:
                return "Bearish"
            return "Neutral"
        
        # Method 2: Check recommendation
        recommendation = soup.find("div", class_="container-Rq8qIgPj")
        if recommendation:
            text = recommendation.get_text().upper()
            if "STRONG_BUY" in text:
                return "Strong Bullish"
            elif "BUY" in text:
                return "Bullish"
            elif "STRONG_SELL" in text:
                return "Strong Bearish"
            elif "SELL" in text:
                return "Bearish"
        
        return "Neutral (No clear signal)"
        
    except Exception as e:
        log_message(f"⚠️ Sentiment fetch failed for {pair}: {str(e)}", "WARNING")
        return "Sentiment Unavailable"

def fetch_forex_factory_news(pair: str) -> str:
    """Fetch relevant news from Forex Factory with improved filtering."""
    try:
        # Skip weekends
        if datetime.utcnow().weekday() >= 5:
            return "Weekend: No scheduled news"
        
        currency_map = {
            "EUR/USD": ["EUR", "USD"],
            "GBP/USD": ["GBP", "USD"],
            "USD/JPY": ["USD", "JPY"]
        }
        
        url = "https://www.forexfactory.com/calendar?day=today"
        response = requests.get(url, headers=REQUEST_HEADERS, timeout=20)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, "html.parser")
        events = soup.find_all("tr", class_="calendar__row")
        important_news = []
        
        for event in events:
            if "calendar__row--gray" in event.get("class", []):
                continue  # Skip header rows
            
            # Check currency relevance
            currency_cell = event.find("td", class_="calendar__currency")
            if not currency_cell:
                continue
                
            event_currencies = currency_cell.text.strip().split()
            if not any(c in event_currencies for c in currency_map[pair]):
                continue
                
            # Check impact level
            impact = event.find("td", class_="impact")
            if not impact or "high" not in impact.get("class", []):
                continue
                
            # Extract event details
            time_cell = event.find("td", class_="calendar__time")
            event_cell = event.find("td", class_="calendar__event")
            
            if time_cell and event_cell:
                time_str = time_cell.text.strip()
                event_str = event_cell.text.strip()
                important_news.append(f"{time_str} {event_str}")
        
        return " | ".join(important_news[:3]) if important_news else "No high-impact news today"
        
    except Exception as e:
        log_message(f"⚠️ News fetch error for {pair}: {str(e)}", "WARNING")
        return "News Unavailable"

def save_to_supabase(row: Dict) -> bool:
    """Save analysis data to Supabase with error handling."""
    try:
        conn = psycopg2.connect(POSTGRES_URL)
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO forex_analysis (
                timestamp, pair, open, high, low, close,
                ema10, ema50, rsi, atr, support, resistance,
                trend_direction, crossover, sentiment_summary, news_summary
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp, pair) DO NOTHING
        """, (
            row["timestamp"], row["pair"], row["open"], row["high"], row["low"], row["close"],
            row["ema10"], row["ema50"], row["rsi"], row["atr"], row["support"], row["resistance"],
            row["trend_direction"], row["crossover"], row["sentiment_summary"], row["news_summary"]
        ))
        
        conn.commit()
        cur.close()
        conn.close()
        log_message(f"📊 Supabase updated for {row['pair']}")
        return True
        
    except Exception as e:
        log_message(f"❌ Supabase error for {row['pair']}: {str(e)}", "ERROR")
        return False

def generate_telegram_alert(row: Dict) -> str:
    """Generate formatted Telegram alert message."""
    try:
        alert_msg = (
            f"\n🚨 *{row['pair']} {row['trend_direction']}*\n"
            f"🕒 {row['timestamp']}\n"
            f"💰 *Price:* {row['close']:.5f} | *RSI:* {row['rsi']:.2f}\n"
            f"📊 *EMA10:* {row['ema10']:.5f} | *EMA50:* {row['ema50']:.5f}\n"
            f"🔀 *{row['crossover']}*\n"
            f"📈 *ATR:* {row['atr']:.5f} | *Range:* {row['high']:.5f}-{row['low']:.5f}\n"
            f"🔽 *Support:* {row['support']:.5f} | 🔼 *Resistance:* {row['resistance']:.5f}\n"
            f"📢 *Sentiment:* {row['sentiment_summary']}\n"
            f"🗞️ *News:* {row['news_summary']}\n"
        )
        return alert_msg
    except Exception as e:
        log_message(f"❌ Alert generation error: {str(e)}", "ERROR")
        return f"⚠️ Error generating alert for {row.get('pair', 'unknown')}"

async def send_telegram_alert(message: str) -> bool:
    """Send alert to Telegram with error handling."""
    try:
        bot = telegram.Bot(token=TELEGRAM_TOKEN)
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=message,
            parse_mode="Markdown",
            disable_web_page_preview=True
        )
        return True
    except Exception as e:
        log_message(f"❌ Telegram send error: {str(e)}", "ERROR")
        return False

def main():
    """Main pipeline execution."""
    log_message("🏁 Starting Forex Pipeline")
    start_time = time.time()
    
    try:
        rows = []
        for pair in PAIRS:
            try:
                symbol = PAIRS[pair]
                log_message(f"🔍 Processing {pair}...")
                
                # Fetch and process data
                df = fetch_data(symbol)
                df = compute_indicators(df)
                support, resistance = detect_levels(df)
                
                # Get current and previous values
                prev = df.iloc[-2]
                latest = df.iloc[-1]
                
                # Determine market status
                crossover = get_crossover_status(
                    latest["ema10"], latest["ema50"],
                    prev["ema10"], prev["ema50"]
                )
                trend = "Uptrend" if latest["ema10"] > latest["ema50"] else "Downtrend"
                
                # Fetch sentiment and news (with delay between requests)
                sentiment = fetch_tradingview_sentiment(pair)
                time.sleep(REQUEST_DELAY)
                news = fetch_forex_factory_news(pair)
                time.sleep(REQUEST_DELAY)
                
                # Prepare data row
                row = {
                    "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    "pair": pair,
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
                    "sentiment_summary": sentiment,
                    "news_summary": news
                }
                rows.append(row)
                
                log_message(f"✅ Processed {pair}")
                
            except Exception as e:
                log_message(f"❌ Failed to process {pair}: {str(e)}", "ERROR")
                continue
        
        # Process all collected data
        for row in rows:
            # Send Telegram alert
            alert_msg = generate_telegram_alert(row)
            asyncio.run(send_telegram_alert(alert_msg))
            
            # Save to Google Sheets
            try:
                sheet.append_row([
                    row["timestamp"], row["pair"], row["open"], row["high"], row["low"], row["close"],
                    row["ema10"], row["ema50"], row["rsi"], row["atr"], row["support"], row["resistance"],
                    row["trend_direction"], row["crossover"], row["sentiment_summary"], row["news_summary"]
                ])
            except Exception as e:
                log_message(f"❌ Google Sheets error for {row['pair']}: {str(e)}", "ERROR")
            
            # Save to Supabase
            save_to_supabase(row)
            
        execution_time = time.time() - start_time
        log_message(f"🏁 Pipeline completed in {execution_time:.2f} seconds")
        
    except Exception as e:
        log_message(f"❌ Pipeline failed: {str(e)}", "ERROR")
        raise

if __name__ == "__main__":
    main()