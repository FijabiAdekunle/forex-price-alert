import os
import requests
import pandas as pd
import telegram
from datetime import datetime
from dotenv import load_dotenv
from ta.trend import EMAIndicator
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import psycopg2

load_dotenv()

PAIRS = {
    "EUR/USD": "EURUSD",
    "GBP/USD": "GBPUSD",
    "USD/JPY": "USDJPY"
}

# === Fetch Forex Data ===
def fetch_data(symbol):
    url = f"https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": "15min",
        "apikey": os.getenv("TWELVE_DATA_API_KEY"),
        "outputsize": 100
    }
    res = requests.get(url, params=params)
    data = res.json()
    df = pd.DataFrame(data["values"])
    df = df.rename(columns={"datetime": "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp").sort_index()
    df = df.astype(float)
    return df

# === Compute Indicators ===
def compute_indicators(df):
    df["EMA 10"] = EMAIndicator(close=df["close"], window=10).ema_indicator()
    df["EMA 50"] = EMAIndicator(close=df["close"], window=50).ema_indicator()
    df["RSI"] = RSIIndicator(close=df["close"], window=14).rsi()
    df["ATR"] = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"]).average_true_range()
    df["Support"] = df["low"].rolling(window=10).min()
    df["Resistance"] = df["high"].rolling(window=10).max()
    return df

# === TradingView Sentiment ===
def get_tradingview_sentiment(symbol):
    try:
        url = "https://symbol-screener.tradingview.com/forex/scan"
        headers = {"Content-Type": "application/json"}
        payload = {
            "symbols": {"tickers": [f"FX:{symbol}"], "query": {"types": []}},
            "columns": ["technical_analysis_summary"]
        }
        res = requests.post(url, headers=headers, json=payload)
        data = res.json()
        summary = data["data"][0]["d"][0] if data["data"] else "N/A"
        return summary
    except Exception as e:
        return f"Error: {e}"

# === Telegram Alert ===
def send_telegram_alert(data):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("Missing Telegram credentials")
        return

    bot = telegram.Bot(token=token)
    for row in data:
        msg = (
            f"\U0001F4C8 *{row['pair']} Update*\n"
            f"Price: {row['close']} | RSI: {round(row['RSI'], 2)}\n"
            f"EMA10: {round(row['EMA 10'], 2)} | EMA50: {round(row['EMA 50'], 2)}\n"
            f"ATR: {round(row['ATR'], 2)} | S: {round(row['support'], 2)} | R: {round(row['resistance'], 2)}\n"
            f"\U0001F4AC Sentiment: _{row['sentiment_summary']}_\n"
            f"\U0001F4F0 News: _{row['news_summary']}_")
        bot.send_message(chat_id=chat_id, text=msg, parse_mode=telegram.constants.ParseMode.MARKDOWN)

# === Append to Google Sheets ===
def append_to_google_sheets(data):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(os.getenv("GSPREAD_KEY_JSON"), scope)
    client = gspread.authorize(creds)
    sheet = client.open(os.getenv("GOOGLE_SHEET_NAME")).sheet1

    for row in data:
        sheet.append_row([
            row['timestamp'].strftime("%Y-%m-%d %H:%M:%S"), row['pair'], row['open'], row['high'], row['low'], row['close'],
            row['EMA 10'], row['EMA 50'], row['RSI'], row['ATR'], row['support'], row['resistance'],
            row['sentiment_summary'], row['news_summary']
        ])

# === Insert into PostgreSQL ===
def insert_to_postgres(data):
    conn = psycopg2.connect(
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT")
    )
    cur = conn.cursor()
    for row in data:
        cur.execute("""
            INSERT INTO forex_history (timestamp, pair, open, high, low, close,
                ema10, ema50, rsi, atr, support, resistance, sentiment, news)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            row['timestamp'], row['pair'], row['open'], row['high'], row['low'], row['close'],
            row['EMA 10'], row['EMA 50'], row['RSI'], row['ATR'], row['support'], row['resistance'],
            row['sentiment_summary'], row['news_summary']
        ))
    conn.commit()
    cur.close()
    conn.close()

# === Optional: Use Twelve Data for News if needed ===
def fetch_latest_news(symbol):
    try:
        url = "https://api.twelvedata.com/news"
        res = requests.get(url, params={"symbol": symbol, "apikey": os.getenv("TWELVE_DATA_API_KEY"), "limit": 1})
        news = res.json()
        if "data" in news and news["data"]:
            return news["data"][0]["title"]
    except:
        pass
    return "No news found"

# === Main Execution ===
def main():
    all_data = []
    for pair, symbol in PAIRS.items():
        df = fetch_data(symbol)
        df = compute_indicators(df)
        latest = df.iloc[-1]
        sentiment = get_tradingview_sentiment(symbol)
        news = fetch_latest_news(symbol)

        row = {
            "timestamp": latest.name,
            "pair": pair,
            "open": latest["open"],
            "high": latest["high"],
            "low": latest["low"],
            "close": latest["close"],
            "EMA 10": latest["EMA 10"],
            "EMA 50": latest["EMA 50"],
            "RSI": latest["RSI"],
            "ATR": latest["ATR"],
            "support": latest["Support"],
            "resistance": latest["Resistance"],
            "sentiment_summary": sentiment,
            "news_summary": news
        }
        all_data.append(row)

    insert_to_postgres(all_data)
    append_to_google_sheets(all_data)
    send_telegram_alert(all_data)

if __name__ == "__main__":
    main()
