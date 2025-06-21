import os
import pandas as pd
import requests
import telegram
import psycopg2
import gspread
from datetime import datetime
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import numpy as np
import asyncio
import logging
from sqlalchemy import create_engine

load_dotenv()

# Configure logging
logging.basicConfig(filename="log.txt", level=logging.INFO, format='[%(asctime)s] %(message)s')

PAIRS = {
    "EUR/USD": "EUR/USD",
    "GBP/USD": "GBP/USD",
    "USD/JPY": "USD/JPY"
}

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
POSTGRES_URL = os.getenv("POSTGRES_URL")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")

# Google Sheets Setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("gspread_key.json", scope)
client = gspread.authorize(creds)
sheet = client.open(GOOGLE_SHEET_NAME).sheet1

# Logging helper
def log_message(msg):
    print(f"[{datetime.utcnow()}] {msg}")
    logging.info(msg)

# Fetch from Twelve Data

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
    df = df.sort_values("datetime")
    df.set_index("datetime", inplace=True)
    df = df.astype(float)
    return df

# Indicators

def compute_indicators(df):
    df["ema10"] = df["close"].ewm(span=10).mean()
    df["ema50"] = df["close"].ewm(span=50).mean()
    delta = df["close"].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = -delta.clip(upper=0).rolling(14).mean()
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
        result_tag = soup.find("div", {"class": "speedometerSignal-pyzN--tL"})
        if result_tag:
            return result_tag.text.strip()
    except Exception as e:
        log_message(f"TradingView sentiment error for {pair}: {e}")
    return "N/A"

def fetch_forex_factory_news(pair):
    try:
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

def insert_to_postgres(df):
    try:
        engine = create_engine(POSTGRES_URL)
        df.to_sql("forex_history", engine, if_exists="append", index=False)
        log_message("✅ Supabase updated")
    except Exception as e:
        log_message(f"❌ Supabase error: {e}")

# Main

def main():
    rows = []
    for pair in PAIRS:
        try:
            symbol = PAIRS[pair].replace("/", "")
            df = fetch_data(symbol)
            df = compute_indicators(df)
            support, resistance = detect_levels(df)
            latest = df.iloc[-1]
            trend = "Uptrend" if latest["ema10"] > latest["ema50"] else "Downtrend"
            crossover = (
                "Golden Cross" if latest["ema10"] > latest["ema50"] and df["ema10"].iloc[-2] < df["ema50"].iloc[-2] else
                "Death Cross" if latest["ema10"] < latest["ema50"] and df["ema10"].iloc[-2] > df["ema50"].iloc[-2] else
                "No Crossover"
            )
            sentiment = fetch_tradingview_sentiment(pair)
            news = fetch_forex_factory_news(pair)

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
                "trend": trend,
                "crossover": crossover,
                "sentiment": sentiment,
                "news": news
            }
            rows.append(row)
        except Exception as e:
            log_message(f"❌ Error processing {pair}: {e}")

    for row in rows:
        alert_msg = (
            f"🚨 {row['pair']} {row['trend'].upper()}\n"
            f"🕒 {row['timestamp']}\n"
            f"Price: {row['close']} | RSI: {round(row['rsi'], 2)}\n"
            f"EMA10: {round(row['ema10'], 5)} | EMA50: {round(row['ema50'], 5)}\n"
            f"Crossover: {row['crossover']}\n"
            f"ATR: {round(row['atr'], 4)} | Support: {round(row['support'], 4)} | Resistance: {round(row['resistance'], 4)}\n"
            f"Sentiment: {row['sentiment']}\n"
            f"News: {row['news']}\n"
            f"#forex #RSI #EMA"
        )
        try:
            bot = telegram.Bot(token=TELEGRAM_TOKEN)
            asyncio.run(bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=alert_msg))
            log_message("✅ Telegram alert sent")
        except Exception as e:
            log_message(f"Telegram send error: {e}")

        try:
            sheet.append_row([
                row["timestamp"], row["pair"], row["open"], row["high"], row["low"], row["close"],
                row["ema10"], row["ema50"], row["rsi"], row["atr"], row["support"], row["resistance"],
                row["trend"], row["crossover"], row["sentiment"], row["news"]
            ])
            log_message("✅ Google Sheet updated")
        except Exception as e:
            log_message(f"Google Sheets error: {e}")

        try:
            insert_to_postgres(pd.DataFrame([row]))
        except Exception as e:
            log_message(f"Supabase insert error: {e}")

if __name__ == "__main__":
    main()
