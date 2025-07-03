# forex_pipeline.py

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

# ENV variables
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")

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

# Fetch from Twelve Data
def fetch_data(symbol):
    api_key = os.getenv("TWELVE_DATA_API_KEY")
    url = "https://api.twelvedata.com/time_series"
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
    df = df.sort_values("datetime")
    df.set_index("datetime", inplace=True)
    df = df.astype(float)
    return df

def compute_indicators(df):
    df["ema10"] = df["close"].ewm(span=10).mean()
    df["ema50"] = df["close"].ewm(span=50).mean()
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = -delta.where(delta < 0, 0).rolling(14).mean()
    rs = gain / loss
    df["rsi"] = 100 - (100 / (1 + rs))
    df["atr"] = df[["high", "low", "close"]].apply(
        lambda x: max(x[0] - x[1], abs(x[0] - x[2]), abs(x[1] - x[2])),
        axis=1
    ).rolling(14).mean()
    return df

def detect_levels(df):
    support = df["low"].rolling(10).min().iloc[-1]
    resistance = df["high"].rolling(10).max().iloc[-1]
    return support, resistance

def fetch_tradingview_sentiment(pair):
    try:
        symbol_map = {
            "EUR/USD": "FX:EURUSD",
            "GBP/USD": "FX:GBPUSD",
            "USD/JPY": "FX:USDJPY"
        }
        symbol = symbol_map[pair]
        url = f"https://www.tradingview.com/symbols/{symbol.replace(':', '-')}/technicals/"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        tag = soup.select_one("div.speedometerSignal-pyzN--tL")
        if tag:
            return tag.text.strip()
    except Exception as e:
        log_message(f"TradingView sentiment error for {pair}: {e}")
    return "N/A"

def fetch_forex_factory_news(pair):
    try:
        if datetime.utcnow().weekday() >= 5:
            return "Weekend: No scheduled news."
        response = requests.get("https://www.forexfactory.com/calendar", headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        soup = BeautifulSoup(response.content, "html.parser")
        today = datetime.utcnow().strftime("%a")
        events = soup.find_all("tr", {"class": "calendar__row"})
        headlines = []
        for event in events:
            if today not in event.text:
                continue
            if pair.split("/")[0] in event.text or pair.split("/")[1] in event.text:
                impact = event.find("td", class_="impact")
                if impact and "high" in impact.get("class", []):
                    desc = event.find("td", class_="event")
                    if desc:
                        headlines.append(desc.text.strip())
        return ", ".join(headlines[:3]) if headlines else "No major news"
    except Exception as e:
        log_message(f"ForexFactory news error for {pair}: {e}")
    return "No news"

# PostgreSQL push
def push_to_postgres(row):
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD
        )
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO forex_history (
                timestamp, pair, open, high, low, close,
                ema10, ema50, rsi, atr, support, resistance,
                trend, crossover, sentiment, news
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row["timestamp"], row["pair"], row["open"], row["high"], row["low"], row["close"],
            row["ema10"], row["ema50"], row["rsi"], row["atr"], row["support"], row["resistance"],
            row["trend_direction"], row["crossover"], row["sentiment_summary"], row["news_summary"]
        ))
        conn.commit()
        cur.close()
        conn.close()
        log_message(f"📦 PostgreSQL saved: {row['pair']}")
    except Exception as e:
        log_message(f"PostgreSQL error: {e}")

# Main
def main():
    for pair in PAIRS:
        df = fetch_data(pair)
        df = compute_indicators(df)
        support, resistance = detect_levels(df)
        prev = df.iloc[-2]
        latest = df.iloc[-1]

        if prev["ema10"] < prev["ema50"] and latest["ema10"] > latest["ema50"]:
            crossover = "Bullish Crossover"
        elif prev["ema10"] > prev["ema50"] and latest["ema10"] < latest["ema50"]:
            crossover = "Bearish Crossover"
        else:
            crossover = "No Crossover"

        row = {
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "pair": pair,
            "open": latest["open"], "high": latest["high"],
            "low": latest["low"], "close": latest["close"],
            "ema10": latest["ema10"], "ema50": latest["ema50"], "rsi": latest["rsi"],
            "atr": latest["atr"], "support": support, "resistance": resistance,
            "trend_direction": "Uptrend" if latest["ema10"] > latest["ema50"] else "Downtrend",
            "crossover": crossover,
            "sentiment_summary": fetch_tradingview_sentiment(pair),
            "news_summary": fetch_forex_factory_news(pair)
        }

        # Telegram alert
        try:
            alert_msg = f"🚨 {row['pair']} {row['trend_direction']}\n🕒 {row['timestamp']}\n" \
                        f"Price: {row['close']} | RSI: {round(row['rsi'], 2)}\n" \
                        f"EMA10: {round(row['ema10'], 5)} | EMA50: {round(row['ema50'], 5)}\n" \
                        f"{row['crossover']} | ATR: {round(row['atr'], 4)}\n" \
                        f"Support: {round(row['support'], 4)} | Resistance: {round(row['resistance'], 4)}\n" \
                        f"Sentiment: {row['sentiment_summary']} | News: {row['news_summary']}"
            bot = telegram.Bot(token=TELEGRAM_TOKEN)
            asyncio.run(bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=alert_msg))
        except Exception as e:
            log_message(f"Telegram send error: {e}")

        # Google Sheets
        try:
            sheet.append_row(list(row.values()))
        except Exception as e:
            log_message(f"Google Sheets error: {e}")

        # PostgreSQL
        push_to_postgres(row)

if __name__ == "__main__":
    main()
