# forex_pipeline.py

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
import smtplib
from email.message import EmailMessage

load_dotenv()

PAIRS = {
    "EUR/USD": "EUR/USD",
    "GBP/USD": "GBP/USD",
    "USD/JPY": "USD/JPY"
}

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
POSTGRES_URL = os.getenv("POSTGRES_URL")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
ALERT_EMAIL = os.getenv("ALERT_EMAIL")

# Logging
logging.basicConfig(filename="log.txt", level=logging.INFO, format="[%(asctime)s] %(message)s")

def log_message(msg):
    print(f"[{datetime.utcnow()}] {msg}")
    logging.info(msg)

# Email alerts for backup or error

def send_email_alert(subject, body):
    try:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = SMTP_USER
        msg["To"] = ALERT_EMAIL
        msg.set_content(body)

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
        logging.info("📧 Email alert sent.")
    except Exception as e:
        logging.error(f"Email send error: {e}")

# Google Sheets Setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("gspread_key.json", scope)
client = gspread.authorize(creds)
sheet = client.open(GOOGLE_SHEET_NAME).sheet1
try:
    trade_journal = client.open(GOOGLE_SHEET_NAME).worksheet("TradeJournal")
except Exception as e:
    logging.error("Trade Journal update error: TradeJournal")
    trade_journal = None

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

# Compute indicators

def compute_indicators(df):
    df["ema10"] = df["close"].ewm(span=10).mean()
    df["ema50"] = df["close"].ewm(span=50).mean()
    delta = df["close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    df["rsi"] = 100 - (100 / (1 + rs))
    df["atr"] = df[["high", "low", "close"]].apply(
        lambda x: max(x[0] - x[1], abs(x[0] - x[2]), abs(x[1] - x[2])), axis=1
    ).rolling(14).mean()
    return df

# Support/Resistance

def detect_levels(df):
    support = df["low"].rolling(10).min().iloc[-1]
    resistance = df["high"].rolling(10).max().iloc[-1]
    return support, resistance

# TradingView sentiment

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
        tag = soup.select_one("div.js-category-header > span")
        if tag:
            return tag.text.strip()
    except Exception as e:
        log_message(f"TradingView sentiment error for {pair}: {e}")
    return "N/A"

# Forex Factory news

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

# Main pipeline

def main():
    rows = []
    for pair in PAIRS:
        try:
            symbol = PAIRS[pair]
            df = fetch_data(symbol)
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

            trend = "Uptrend" if latest["ema10"] > latest["ema50"] else "Downtrend"
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
                "trend_direction": trend,
                "crossover": crossover,
                "sentiment_summary": sentiment,
                "news_summary": news
            }
            rows.append(row)
        except Exception as e:
            log_message(f"❌ Error processing {pair}: {e}")
            send_email_alert("Forex Pipeline Error", f"❌ Error processing {pair}: {e}")

    for row in rows:
        alert_msg = f"🚨 {row['pair']} {row['trend_direction'].upper()}\n"
        alert_msg += f"🕒 {row['timestamp']}\n"
        alert_msg += f"Price: {row['close']} | RSI: {round(row['rsi'], 2)}\n"
        alert_msg += f"EMA10: {round(row['ema10'], 5)} | EMA50: {round(row['ema50'], 5)}\n"
        alert_msg += f"Crossover: {row['crossover']}\n"
        alert_msg += f"ATR: {round(row['atr'], 4)} | Support: {round(row['support'], 4)} | Resistance: {round(row['resistance'], 4)}\n"
        alert_msg += f"Sentiment: {row['sentiment_summary']}\n"
        alert_msg += f"News: {row['news_summary']}\n"
        alert_msg += "#forex #RSI #EMA"

        try:
            bot = telegram.Bot(token=TELEGRAM_TOKEN)
            asyncio.run(bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=alert_msg))
        except Exception as e:
            log_message(f"Telegram send error: {e}")

        try:
            sheet.append_row([
                row["timestamp"], row["pair"], row["open"], row["high"], row["low"], row["close"],
                row["ema10"], row["ema50"], row["rsi"], row["atr"], row["support"], row["resistance"],
                row["trend_direction"], row["crossover"], row["sentiment_summary"], row["news_summary"]
            ])
        except Exception as e:
            log_message(f"Google Sheets error: {e}")

        try:
            if trade_journal:
                trade_journal.append_row([
                    row["timestamp"], row["pair"], row["trend_direction"], row["crossover"],
                    row["rsi"], row["atr"], row["sentiment_summary"], row["news_summary"], row["close"]
                ])
        except Exception as e:
            logging.error("Trade Journal update error: TradeJournal")

    send_email_alert("Backup complete", "All systems ran successfully.")

if __name__ == "__main__":
    main()
