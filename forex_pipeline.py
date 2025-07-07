#!/usr/bin/env python3
import os
import pandas as pd
import requests
import telegram
import gspread
import psycopg2
from datetime import datetime, time as dt_time, timedelta
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import numpy as np
import asyncio
import logging
from typing import Dict, Tuple, Optional, List

# Load environment variables
load_dotenv()

# ======================
# CONFIGURATION
# ======================
PAIRS = {
    "EUR/USD": "EUR/USD",
    "GBP/USD": "GBP/USD", 
    "USD/JPY": "USD/JPY"
}

MARKET_SESSIONS = {
    "Asian": (dt_time(0,0), dt_time(6,59)),
    "London": (dt_time(7,0), dt_time(15,59)), 
    "NY": (dt_time(13,0), dt_time(20,59)),
    "Closed": (dt_time(21,0), dt_time(23,59))
}

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

# ======================
# INITIALIZATION
# ======================
logging.basicConfig(
    filename="forex_pipeline.log",
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Initialize services
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("gspread_key.json", scope)
gs_client = gspread.authorize(creds)
sheet = gs_client.open(os.getenv("GOOGLE_SHEET_NAME")).sheet1

# ======================
# CORE FUNCTIONS
# ======================
def get_market_session() -> str:
    """Determine current market session with overlap detection"""
    now = datetime.utcnow().time()
    
    # Check session overlaps first
    if time(13,0) <= now < time(16,0):
        return "London/NY Overlap (High Volatility)"
    elif time(7,0) <= now < time(9,0):
        return "London/Asian Overlap"
    
    # Regular sessions
    for session, (start, end) in MARKET_SESSIONS.items():
        if start <= now <= end:
            return f"{session} Session"
    return "After Hours"

def fetch_webpage(url: str) -> Optional[str]:
    """Robust webpage fetcher with error handling"""
    try:
        response = requests.get(url, headers=REQUEST_HEADERS, timeout=15)
        response.raise_for_status()
        return response.text
    except Exception as e:
        logging.error(f"Webpage fetch failed for {url}: {str(e)}")
        return None

def enhanced_sentiment(pair: str, rsi: float, ema_diff: float) -> str:
    """Multi-factor sentiment analysis"""
    factors = []
    
    # RSI Analysis
    if rsi > 70:
        factors.append("Overbought (RSI {:.1f})".format(rsi))
    elif rsi < 30:
        factors.append("Oversold (RSI {:.1f})".format(rsi))
    
    # EMA Analysis
    if ema_diff > 0.2:
        factors.append("Strong Bullish EMA")
    elif ema_diff > 0.05:
        factors.append("Mild Bullish EMA")
    elif ema_diff < -0.2:
        factors.append("Strong Bearish EMA") 
    elif ema_diff < -0.05:
        factors.append("Mild Bearish EMA")
    
    return " | ".join(factors) if factors else "Neutral (Technical Indicators)"

# ======================
# DATA FETCHING & PROCESSING
# ======================
def fetch_data(symbol: str) -> pd.DataFrame:
    """Fetch forex data from Twelve Data API"""
    api_key = os.getenv("TWELVE_DATA_API_KEY")
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": "15min",
        "outputsize": 100,
        "apikey": api_key
    }
    
    try:
        response = requests.get(url, params=params, timeout=15)
        data = response.json()
        df = pd.DataFrame(data["values"])
        df["datetime"] = pd.to_datetime(df["datetime"])
        return df.sort_values("datetime").set_index("datetime")
    except Exception as e:
        logging.error(f"Data fetch failed for {symbol}: {str(e)}")
        raise

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
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
        logging.error(f"Indicator calculation failed: {str(e)}")
        raise

# ======================
# NEWS & SENTIMENT
# ======================
def fetch_tradingview_sentiment(pair: str) -> str:
    """Fetch market sentiment from TradingView"""
    try:
        symbol_map = {
            "EUR/USD": "EURUSD",
            "GBP/USD": "GBPUSD",
            "USD/JPY": "USDJPY"
        }
        url = f"https://www.tradingview.com/symbols/{symbol_map[pair]}/technicals/"
        html = fetch_webpage(url)
        if not html:
            return "Sentiment unavailable"
            
        soup = BeautifulSoup(html, 'html.parser')
        sentiment_tag = soup.find("div", class_="speedometerWrapper-")
        if sentiment_tag:
            return sentiment_tag.get_text(strip=True)
        return "Neutral (No clear signal)"
    except Exception as e:
        logging.error(f"Sentiment fetch failed: {str(e)}")
        return "Sentiment unavailable"

def fetch_forex_factory_news(pair: str) -> str:
    """Fetch news from Forex Factory"""
    try:
        if datetime.utcnow().weekday() >= 5:
            return "Weekend: No scheduled news"
            
        url = "https://www.forexfactory.com/calendar?day=today"
        html = fetch_webpage(url)
        if not html:
            return "News unavailable"
            
        soup = BeautifulSoup(html, 'html.parser')
        events = soup.find_all("tr", class_="calendar__row")
        news_items = []
        
        for event in events[:5]:  # Check first 5 events
            if "high" in event.get("class", []):
                time_tag = event.find("td", class_="calendar__time")
                title_tag = event.find("td", class_="calendar__event")
                if time_tag and title_tag:
                    news_items.append(f"{time_tag.text.strip()}: {title_tag.text.strip()}")
        
        return " | ".join(news_items[:2]) if news_items else "No high-impact news"
    except Exception as e:
        logging.error(f"News fetch failed: {str(e)}")
        return "News unavailable"

# ======================
# ALERT GENERATION
# ======================
def generate_alert(row: Dict) -> str:
    """Generate formatted alert message"""
    ema_diff = ((row["ema10"] - row["ema50"]) / row["ema50"]) * 100
    session = get_market_session()
    
    alert_msg = (
        f"\n🌐 *Market Session:* {session}\n"
        f"🚨 *{row['pair']} {row['trend_direction']}*\n"
        f"🕒 {row['timestamp']}\n"
        f"💰 *Price:* {row['close']:.5f} | *RSI:* {row['rsi']:.2f}\n"
        f"📊 *EMAs:* {row['ema10']:.5f} (10) | {row['ema50']:.5f} (50)\n"
        f"🔀 *Crossover:* {row['crossover']} ({ema_diff:.2f}% diff)\n"
        f"📈 *Volatility:* ATR {row['atr']:.5f} | Range {row['low']:.5f}-{row['high']:.5f}\n"
        f"🔽 *Support:* {row['support']:.5f} | 🔼 *Resistance:* {row['resistance']:.5f}\n"
        f"📢 *Sentiment:* {row['sentiment_summary']}\n"
        f"🗞️ *News:* {row['news_summary']}\n"
    )
    
    # Add warnings if needed
    if row["rsi"] > 70 and "Bullish" in row["crossover"]:
        alert_msg += "\n⚠️ *Warning:* Overbought with Bullish Crossover\n"
    elif row["rsi"] < 30 and "Bearish" in row["crossover"]:
        alert_msg += "\n⚠️ *Warning:* Oversold with Bearish Crossover\n"
    
    return alert_msg

async def send_telegram_alert(message: str) -> bool:
    """Send alert to Telegram"""
    try:
        bot = telegram.Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
        await bot.send_message(
            chat_id=os.getenv("TELEGRAM_CHAT_ID"),
            text=message,
            parse_mode="Markdown"
        )
        return True
    except Exception as e:
        logging.error(f"Telegram send failed: {str(e)}")
        return False

# ======================
# MAIN PIPELINE
# ======================
def main():
    """Main execution flow"""
    logging.info("Starting Forex Pipeline")
    rows = []
    
    for pair in PAIRS:
        try:
            # Data Processing
            df = compute_indicators(fetch_data(PAIRS[pair]))
            latest = df.iloc[-1]
            prev = df.iloc[-2]
            
            # Technical Analysis
            support, resistance = detect_levels(df)
            ema_diff = (latest["ema10"] - latest["ema50"]) / latest["ema50"] * 100
            crossover = "Bullish Crossover" if (prev["ema10"] < prev["ema50"] and latest["ema10"] > latest["ema50"]) else \
                       "Bearish Crossover" if (prev["ema10"] > prev["ema50"] and latest["ema10"] < latest["ema50"]) else \
                       "No Crossover"
            
            # Sentiment & News
            sentiment = fetch_tradingview_sentiment(pair)
            news = fetch_forex_factory_news(pair)
            
            # Prepare alert data
            rows.append({
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
                "trend_direction": "Uptrend" if latest["ema10"] > latest["ema50"] else "Downtrend",
                "crossover": crossover,
                "sentiment_summary": sentiment,
                "news_summary": news
            })
            
        except Exception as e:
            logging.error(f"Failed processing {pair}: {str(e)}")
            continue
    
    # Send alerts and save data
    for row in rows:
        try:
            # Telegram Alert
            asyncio.run(send_telegram_alert(generate_alert(row)))
            
            # Google Sheets
            sheet.append_row(list(row.values()))
            
            # Database (optional)
            # save_to_database(row)
            
        except Exception as e:
            logging.error(f"Failed alert processing for {row['pair']}: {str(e)}")

if __name__ == "__main__":
    main()