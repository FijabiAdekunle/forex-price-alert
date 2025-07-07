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

# Enhanced logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('forex_pipeline_debug.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables with verification
load_dotenv()

def verify_env_vars():
    """Verify all required environment variables are set"""
    required_vars = [
        'TWELVE_DATA_API_KEY',
        'TELEGRAM_BOT_TOKEN',
        'TELEGRAM_CHAT_ID',
        'GSPREAD_KEY_JSON',
        'GOOGLE_SHEET_NAME',
        'POSTGRES_URL'
    ]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing environment variables: {', '.join(missing_vars)}")
        raise EnvironmentError(f"Missing required environment variables: {missing_vars}")

verify_env_vars()

# Initialize services with error handling
try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("gspread_key.json", scope)
    gs_client = gspread.authorize(creds)
    sheet = gs_client.open(os.getenv("GOOGLE_SHEET_NAME")).sheet1
    logger.info("Successfully connected to Google Sheets")
except Exception as e:
    logger.error(f"Google Sheets initialization failed: {str(e)}")
    sheet = None

try:
    conn = psycopg2.connect(os.getenv("POSTGRES_URL"))
    logger.info("Successfully connected to Supabase")
except Exception as e:
    logger.error(f"Supabase connection failed: {str(e)}")
    conn = None

# Telegram bot initialization
bot = telegram.Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))

async def send_telegram_alert(message: str):
    """Send alert with comprehensive error handling"""
    try:
        await bot.send_message(
            chat_id=os.getenv("TELEGRAM_CHAT_ID"),
            text=message,
            parse_mode="Markdown"
        )
        logger.info(f"Telegram alert sent for {message.split()[1]}")  # Log pair name
    except Exception as e:
        logger.error(f"Telegram send failed: {str(e)}\n{traceback.format_exc()}")

def save_to_supabase(row: dict):
    """Save data to Supabase with error handling"""
    if not conn:
        logger.warning("Skipping Supabase update - no connection")
        return
        
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
        logger.info(f"Supabase updated for {row['pair']}")
    except Exception as e:
        logger.error(f"Supabase update failed for {row['pair']}: {str(e)}\n{traceback.format_exc()}")

def append_to_sheet(row: dict):
    """Append data to Google Sheet with error handling"""
    if not sheet:
        logger.warning("Skipping Google Sheets update - no connection")
        return
        
    try:
        sheet.append_row([
            row["timestamp"], row["pair"], row["open"], row["high"], row["low"], row["close"],
            row["ema10"], row["ema50"], row["rsi"], row["atr"], row["support"], row["resistance"],
            row["trend_direction"], row["crossover"], row["sentiment_summary"], row["news_summary"]
        ])
        logger.info(f"Google Sheet updated for {row['pair']}")
    except Exception as e:
        logger.error(f"Google Sheets update failed for {row['pair']}: {str(e)}\n{traceback.format_exc()}")

def main():
    logger.info("Starting Forex Pipeline Execution")
    
    try:
        # Your existing data processing logic here
        # For each pair, create a row dictionary with all the required fields
        
        # Example row - replace with your actual data processing
        example_row = {
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "pair": "EUR/USD",
            "open": 1.0800,
            "high": 1.0820,
            "low": 1.0790,
            "close": 1.0815,
            "ema10": 1.0805,
            "ema50": 1.0795,
            "rsi": 60.5,
            "atr": 0.0025,
            "support": 1.0780,
            "resistance": 1.0830,
            "trend_direction": "Uptrend",
            "crossover": "Bullish Crossover",
            "sentiment_summary": "Bullish",
            "news_summary": "ECB rate decision upcoming"
        }
        
        # Generate alert message
        alert_message = f"""
🚨 {example_row['pair']} {example_row['trend_direction']}
Price: {example_row['close']} | RSI: {example_row['rsi']}
EMA10: {example_row['ema10']} | EMA50: {example_row['ema50']}
Crossover: {example_row['crossover']} | ATR: {example_row['atr']}
Support: {example_row['support']} | Resistance: {example_row['resistance']}
Sentiment: {example_row['sentiment_summary']} | News: {example_row['news_summary']}
"""
        
        # Execute all output methods
        asyncio.run(send_telegram_alert(alert_message))
        append_to_sheet(example_row)
        save_to_supabase(example_row)
        
        logger.info("Pipeline execution completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}\n{traceback.format_exc()}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()