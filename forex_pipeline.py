#!/usr/bin/env python3
import os
import pandas as pd
import requests
import telegram
import gspread
import psycopg2
from datetime import datetime, time, timedelta
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import numpy as np
import asyncio
import logging
import time
from typing import Dict, Tuple, Optional, List
from requests_html import HTMLSession  # For JS-rendered sites

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
    "Asian": (time(0,0), time(6,59)),
    "London": (time(7,0), time(15,59)), 
    "NY": (time(13,0), time(20,59)),
    "Closed": (time(21,0), time(23,59))
}

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

# ======================
# INITIALIZATION
# ======================
logging.basicConfig(
    filename="forex_enhanced.log",
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

def enhanced_sentiment(pair: str, rsi: float, ema_diff: float) -> str:
    """Multi-factor sentiment analysis"""
    factors = []
    
    # RSI Analysis
    if rsi > 70:
        factors.append("Overbought (RSI {:.1f})".format(rsi))
    elif rsi < 30:
        factors.append("Oversold (RSI {:.1f})".format(rsi))
    elif 45 < rsi < 55:
        factors.append("Neutral RSI")
    
    # EMA Analysis
    if ema_diff > 0.2:
        factors.append("Strong Bullish EMA")
    elif ema_diff > 0.05:
        factors.append("Mild Bullish EMA")
    elif ema_diff < -0.2:
        factors.append("Strong Bearish EMA") 
    elif ema_diff < -0.05:
        factors.append("Mild Bearish EMA")
    
    # Price Action (would need historical data)
    
    # Generate summary
    if not factors:
        return "Neutral (No clear signals)"
    
    # Special cases
    if "Overbought" in factors and "Bullish" in factors:
        return "Caution: Bullish but Overbought"
    if "Oversold" in factors and "Bearish" in factors:
        return "Caution: Bearish but Oversold"
        
    return " | ".join(factors)

# ======================
# NEWS IMPROVEMENTS
# ======================
def fetch_multi_source_news(pair: str) -> str:
    """Aggregate news from multiple sources"""
    news_sources = [
        fetch_forex_factory_news,
        fetch_fxstreet_news,
        fetch_investing_com_news
    ]
    
    collected_news = []
    for source in news_sources:
        try:
            news = source(pair)
            if news and "unavailable" not in news.lower():
                collected_news.append(news)
                if len(collected_news) >= 2:  # Limit to 2 sources
                    break
        except Exception as e:
            logging.warning(f"News source failed: {str(e)}")
            continue
    
    return format_news_output(collected_news)

def fetch_fxstreet_news(pair: str) -> str:
    """Fetch news from FXStreet"""
    try:
        session = HTMLSession()
        url = "https://www.fxstreet.com/economic-calendar"
        r = session.get(url, headers=REQUEST_HEADERS, timeout=15)
        
        # Wait for JS execution
        r.html.render(timeout=20)
        
        # Extract relevant news
        events = r.html.find(".ec-event-item", first=False)
        today = datetime.utcnow().strftime("%b %-d")
        currency_codes = {
            "EUR/USD": ["EUR", "USD"],
            "GBP/USD": ["GBP", "USD"],
            "USD/JPY": ["USD", "JPY"]
        }
        
        relevant_events = []
        for event in events[:5]:  # Check top 5 events
            if today not in event.text:
                continue
            if any(currency in event.text for currency in currency_codes[pair]):
                time_element = event.find(".ec-tz", first=True)
                title_element = event.find(".ec-title", first=True)
                if time_element and title_element:
                    relevant_events.append(
                        f"{time_element.text.strip()} {title_element.text.strip()}"
                    )
        
        return "FXStreet: " + " | ".join(relevant_events[:2]) if relevant_events else ""
    
    except Exception as e:
        logging.warning(f"FXStreet news error: {str(e)}")
        return ""

def fetch_investing_com_news(pair: str) -> str:
    """Fetch news from Investing.com"""
    try:
        symbol_map = {
            "EUR/USD": "eur-usd",
            "GBP/USD": "gbp-usd",
            "USD/JPY": "usd-jpy"
        }
        url = f"https://www.investing.com/currencies/{symbol_map[pair]}-news"
        
        response = requests.get(url, headers=REQUEST_HEADERS, timeout=15)
        soup = BeautifulSoup(response.text, "html.parser")
        
        articles = soup.select("article.articleItem")[:3]
        news_items = []
        
        for article in articles:
            time_tag = article.find("span", class_="date")
            title_tag = article.find("a", class_="title")
            if time_tag and title_tag:
                news_items.append(
                    f"{time_tag.text.strip()}: {title_tag.text.strip()}"
                )
        
        return "Investing.com: " + " | ".join(news_items) if news_items else ""
    
    except Exception as e:
        logging.warning(f"Investing.com news error: {str(e)}")
        return ""

def format_news_output(news_items: List[str]) -> str:
    """Format news from multiple sources"""
    if not news_items:
        current_session = get_market_session()
        if "Overlap" in current_session or "London" in current_session:
            return "No high-impact news (Check unexpected events)"
        return "No scheduled high-impact news"
    
    # Remove duplicate news
    unique_news = []
    seen = set()
    for item in news_items:
        key = item.split(":", 1)[-1].strip()[:50]  # First 50 chars as key
        if key not in seen:
            seen.add(key)
            unique_news.append(item)
    
    return " | ".join(unique_news[:2])  # Max 2 news items

# ======================
# ENHANCED ALERT GENERATION
# ======================
def generate_enhanced_alert(row: Dict) -> str:
    """Generate alert with market context"""
    session_status = get_market_session()
    ema_diff_pct = ((row["ema10"] - row["ema50"]) / row["ema50"]) * 100
    
    alert_msg = (
        f"\n🌐 *Market Status:* {session_status}\n"
        f"🚨 *{row['pair']} {row['trend_direction']}*\n"
        f"🕒 {row['timestamp']}\n"
        f"💰 *Price:* {row['close']:.5f} | *RSI:* {row['rsi']:.2f}\n"
        f"📊 *EMAs:* {row['ema10']:.5f} (10) | {row['ema50']:.5f} (50)\n"
        f"🔀 *Crossover:* {row['crossover']} ({ema_diff_pct:.2f}% diff)\n"
        f"📈 *Volatility:* ATR {row['atr']:.5f} | Range {row['low']:.5f}-{row['high']:.5f}\n"
        f"🔽 *Support:* {row['support']:.5f} | 🔼 *Resistance:* {row['resistance']:.5f}\n"
        f"📢 *Sentiment:* {row['sentiment_summary']}\n"
        f"🗞️ *News:* {row['news_summary']}\n"
    )
    
    # Add special warnings
    if row["rsi"] > 70 and "Bullish" in row["crossover"]:
        alert_msg += "\n⚠️ *Warning:* Overbought with Bullish Crossover - Potential Reversal Risk\n"
    elif row["rsi"] < 30 and "Bearish" in row["crossover"]:
        alert_msg += "\n⚠️ *Warning:* Oversold with Bearish Crossover - Potential Reversal Risk\n"
    
    return alert_msg

# ======================
# MAIN EXECUTION
# ======================
def main():
    """Enhanced main pipeline"""
    logging.info("🚀 Starting Enhanced Forex Pipeline")
    
    try:
        for pair in PAIRS:
            try:
                # [Previous data fetching and processing...]
                
                # Enhanced sentiment analysis
                ema_diff = (latest["ema10"] - latest["ema50"]) / latest["ema50"] * 100
                sentiment = enhanced_sentiment(pair, latest["rsi"], ema_diff)
                
                # Multi-source news
                news = fetch_multi_source_news(pair)
                
                # Generate enhanced alert
                row = {
                    # [Previous data fields...],
                    "sentiment_summary": sentiment,
                    "news_summary": news
                }
                
                alert = generate_enhanced_alert(row)
                asyncio.run(send_telegram_alert(alert))
                
            except Exception as e:
                logging.error(f"Pair {pair} processing failed: {str(e)}")
                
    except Exception as e:
        logging.critical(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()