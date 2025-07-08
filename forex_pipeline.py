import os
import requests
import pandas as pd
import numpy as np
import telegram
import asyncio
import gspread
import logging
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials

# === Load ENV ===
load_dotenv()

# === Logging ===
logging.basicConfig(filename="log.txt", level=logging.INFO, format="[%(asctime)s] %(message)s")

def log(msg):
    print(f"[{datetime.utcnow()}] {msg}")
    logging.info(msg)

# === Config ===
PAIRS = {
    "EUR/USD": "EUR/USD",
    "GBP/USD": "GBP/USD",
    "USD/JPY": "USD/JPY"
}
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")

# === Neon PostgreSQL Config ===
def connect_neon():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT", 5432),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        sslmode="require"
    )

# === Google Sheets Setup ===
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("gspread_key.json", scope)
client = gspread.authorize(creds)
sheet = client.open(GOOGLE_SHEET_NAME).sheet1

# === Fetch Forex Data ===
def fetch_data(symbol):
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": "15min",
        "outputsize": 50,
        "apikey": os.getenv("TWELVE_DATA_API_KEY")
    }
    response = requests.get(url, params=params)
    data = response.json()
    if "values" not in data:
        raise ValueError(f"Data fetch error: {data}")
    df = pd.DataFrame(data["values"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    df.set_index("datetime", inplace=True)
    df = df.astype(float).sort_index()
    return df

# === Indicators ===
def compute_indicators(df):
    df["ema10"] = df["close"].ewm(span=10).mean()
    df["ema50"] = df["close"].ewm(span=50).mean()
    delta = df["close"].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta).clip(lower=0).rolling(14).mean()
    rs = gain / loss
    df["rsi"] = 100 - (100 / (1 + rs))
    df["atr"] = (df["high"] - df["low"]).rolling(14).mean()
    return df

# === Support/Resistance ===
def detect_levels(df):
    return df["low"].rolling(10).min().iloc[-1], df["high"].rolling(10).max().iloc[-1]

# === News & Sentiment ===
def fetch_news(pair):
    base = pair.split("/")[0]
    url = f"https://newsapi.org/v2/everything?q={base}&sortBy=publishedAt&apiKey={NEWSAPI_KEY}&language=en"
    try:
        r = requests.get(url)
        articles = r.json().get("articles", [])
        return articles[0]["title"] if articles else "No major news"
    except:
        return "News fetch error"

def fetch_sentiment(pair):
    symbol_map = {
        "EUR/USD": "EURUSD",
        "GBP/USD": "GBPUSD",
        "USD/JPY": "USDJPY"
    }
    symbol = symbol_map.get(pair)
    url = f"https://finnhub.io/api/v1/news-sentiment?symbol={symbol}&token={FINNHUB_API_KEY}"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        score = data.get("companyNewsScore")
        if score is not None:
            if score > 0.3:
                return "Strongly Bullish"
            elif 0.1 < score <= 0.3:
                return "Bullish"
            elif -0.1 <= score <= 0.1:
                return "Neutral"
            elif -0.3 <= score < -0.1:
                return "Bearish"
            else:
                return "Strongly Bearish"
    except Exception as e:
        log(f"Sentiment fetch error for {pair}: {e}")
    return "N/A"

# === Crossover Detection ===
def get_crossover_status(current_ema10, current_ema50, prev_ema10, prev_ema50):
    try:
        if prev_ema10 < prev_ema50 and current_ema10 > current_ema50:
            return "Bullish Crossover (Golden Cross)"
        elif prev_ema10 > prev_ema50 and current_ema10 < current_ema50:
            return "Bearish Crossover (Death Cross)"
        elif current_ema10 > current_ema50:
            diff = ((current_ema10 - current_ema50) / current_ema50) * 100
            return f"EMA10 > EMA50 by {diff:.2f}% (Bullish)"
        else:
            diff = ((current_ema50 - current_ema10) / current_ema10) * 100
            return f"EMA10 < EMA50 by {diff:.2f}% (Bearish)"
    except Exception as e:
        log(f"Crossover detection error: {e}")
        return "Crossover Unknown"

# === Save to Neon DB ===
def save_to_neon(row):
    try:
        conn = connect_neon()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO forex_analysis (
                timestamp, pair, open, high, low, close, ema10, ema50, rsi, atr,
                support, resistance, trend_direction, crossover, sentiment_summary, news_summary
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row["timestamp"],
            row["pair"],
            float(row["open"]),
            float(row["high"]),
            float(row["low"]),
            float(row["close"]),
            float(row["ema10"]),
            float(row["ema50"]),
            float(row["rsi"]),
            float(row["atr"]),
            float(row["support"]),
            float(row["resistance"]),
            row["trend_direction"],
            row["crossover"],
            row["sentiment_summary"],
            row["news_summary"]
        ))
        conn.commit()
        cur.close()
        conn.close()
        log(f"✅ Saved {row['pair']} to Neon DB")
    except Exception as e:
        log(f"❌ Neon insert error: {e}")

# === Main ===
def main():
    rows = []
    for pair in PAIRS:
        symbol = PAIRS[pair]
        df = fetch_data(symbol)
        df = compute_indicators(df)
        support, resistance = detect_levels(df)
        latest = df.iloc[-1]
        prev = df.iloc[-2]

        trend = "Uptrend" if latest["ema10"] > latest["ema50"] else "Downtrend"
        crossover = get_crossover_status(latest["ema10"], latest["ema50"], prev["ema10"], prev["ema50"])
        news = fetch_news(pair)
        sentiment = fetch_sentiment(pair)

        row = {
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "pair": pair,
            "open": latest["open"], "high": latest["high"], "low": latest["low"], "close": latest["close"],
            "ema10": latest["ema10"], "ema50": latest["ema50"], "rsi": latest["rsi"], "atr": latest["atr"],
            "support": support, "resistance": resistance, "trend_direction": trend,
            "crossover": crossover, "sentiment_summary": sentiment, "news_summary": news
        }
        rows.append(row)

    for row in rows:
        alert = f"\n🚨 *{row['pair']} {row['trend_direction'].upper()}*\n"
        alert += f"🕒 {row['timestamp']}\n"
        alert += f"💰 *Price*: {round(row['close'], 5)} | *RSI*: {round(row['rsi'], 2)}\n"
        alert += f"📊 *EMA10*: {round(row['ema10'], 5)} | *EMA50*: {round(row['ema50'], 5)}\n"
        alert += f"🔀 *{row['crossover']}*\n"
        alert += f"📈 *Range*: {round(row['high'], 5)} - {round(row['low'], 5)} | *ATR*: {round(row['atr'], 5)}\n"
        alert += f"🔽 *Support*: {round(row['support'], 5)} | 🔼 *Resistance*: {round(row['resistance'], 5)}\n"
        alert += f"📢 *Sentiment*: {row['sentiment_summary']}\n"
        alert += f"🗞️ *News*: {row['news_summary']}"

        try:
            bot = telegram.Bot(token=TELEGRAM_TOKEN)
            asyncio.run(bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=f"FJ Forex Alert:\n{alert}", parse_mode=telegram.constants.ParseMode.MARKDOWN))
        except Exception as e:
            log(f"Telegram error: {e}")

        try:
            sheet.append_row(list(row.values()))
        except Exception as e:
            log(f"Google Sheets error: {e}")

        save_to_neon(row)

if __name__ == "__main__":
    main()
