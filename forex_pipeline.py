# forex_pipeline.py (Updated for Supabase and clean Telegram + GSheet integration)

import os
import pandas as pd
import requests
import telegram
import gspread
from datetime import datetime
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import numpy as np
import psycopg2
import asyncio
import logging

load_dotenv()

PAIRS = {
    "EUR/USD": "EUR/USD",
    "GBP/USD": "GBP/USD",
    "USD/JPY": "USD/JPY"
}

# === ENV Variables ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")

# PostgreSQL connection (Supabase)
def connect_supabase():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        sslmode="require"
    )

# Logging
logging.basicConfig(filename="log.txt", level=logging.INFO, format="[%(asctime)s] %(message)s")

def log_message(msg):
    print(f"[{datetime.utcnow()}] {msg}")
    logging.info(msg)

# Google Sheets Setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("gspread_key.json", scope)
client = gspread.authorize(creds)
sheet = client.open(GOOGLE_SHEET_NAME).sheet1

# === Fetch from Twelve Data ===
def fetch_data(symbol):
    api_key = os.getenv("TWELVE_DATA_API_KEY")
    url = f"https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": "15min",
        "outputsize": 50,
        "apikey": api_key
    }
    res = requests.get(url, params=params)
    data = res.json()
    if "values" not in data:
        raise ValueError(f"Twelve Data returned error: {data}")
    df = pd.DataFrame(data["values"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").set_index("datetime")
    df = df.astype(float)
    return df

# === Technical Indicators ===
def compute_indicators(df):
    df["ema10"] = df["close"].ewm(span=10).mean()
    df["ema50"] = df["close"].ewm(span=50).mean()
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = -delta.where(delta < 0, 0).rolling(14).mean()
    rs = gain / loss
    df["rsi"] = 100 - (100 / (1 + rs))
    df["atr"] = df[["high", "low", "close"]].apply(
        lambda x: max(x[0] - x[1], abs(x[0] - x[2]), abs(x[1] - x[2])), axis=1
    ).rolling(14).mean()
    return df

def detect_levels(df):
    support = df["low"].rolling(10).min().iloc[-1]
    resistance = df["high"].rolling(10).max().iloc[-1]
    return support, resistance

# === Telegram Alert ===
def send_telegram_alert(msg):
    try:
        bot = telegram.Bot(token=TELEGRAM_TOKEN)
        asyncio.run(bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg))
        log_message("Telegram alert sent.")
    except Exception as e:
        log_message(f"Telegram error: {e}")

# === PostgreSQL Storage ===
def save_to_postgres(row):
    try:
        conn = connect_supabase()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO forex_enriched (
                timestamp, pair, open, high, low, close,
                ema10, ema50, rsi, atr, support, resistance,
                trend_direction, crossover, sentiment_summary, news_summary
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, tuple(row.values()))
        conn.commit()
        cur.close()
        conn.close()
        log_message(f"Saved {row['pair']} to PostgreSQL.")
    except Exception as e:
        log_message(f"PostgreSQL error: {e}")

# === Google Sheets Push ===
def save_to_sheets(row):
    try:
        sheet.append_row(list(row.values()))
        log_message(f"Logged {row['pair']} to Google Sheets.")
    except Exception as e:
        log_message(f"Sheets error: {e}")

# === Main Script ===
def main():
    for pair in PAIRS:
        try:
            symbol = PAIRS[pair]
            df = fetch_data(symbol)
            df = compute_indicators(df)
            support, resistance = detect_levels(df)
            prev, latest = df.iloc[-2], df.iloc[-1]
            crossover = "Bullish" if prev["ema10"] < prev["ema50"] and latest["ema10"] > latest["ema50"] \
                else "Bearish" if prev["ema10"] > prev["ema50"] and latest["ema10"] < latest["ema50"] else "None"
            trend = "Uptrend" if latest["ema10"] > latest["ema50"] else "Downtrend"

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
                "sentiment_summary": "N/A",
                "news_summary": "No news"
            }

            alert = f"\n\n🚨 {row['pair']} {row['trend_direction']}\nPrice: {row['close']} | RSI: {round(row['rsi'],2)}\n"
            alert += f"EMA10: {round(row['ema10'],5)} | EMA50: {round(row['ema50'],5)}\n"
            alert += f"ATR: {round(row['atr'],4)} | Support: {round(row['support'],4)} | Resistance: {round(row['resistance'],4)}"
            alert += f"\nCrossover: {row['crossover']}"
            send_telegram_alert(f"FJ Forex Alert: {alert}")

            save_to_postgres(row)
            save_to_sheets(row)

        except Exception as e:
            log_message(f"Pipeline error for {pair}: {e}")

if __name__ == "__main__":
    main()
