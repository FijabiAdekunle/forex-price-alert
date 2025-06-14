import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from ta.trend import EMAIndicator
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange
import psycopg2
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import telegram
from dotenv import load_dotenv

load_dotenv()

PAIRS = {
    "EUR/USD": "EUR/USD",
    "GBP/USD": "GBP/USD",
    "USD/JPY": "USD/JPY"
}

THRESHOLDS = {
    "EUR/USD": 1.1200,
    "GBP/USD": 1.3300,
    "USD/JPY": 153.0000
}

INTERVAL = "15min"
TWELVE_DATA_URL = "https://api.twelvedata.com/time_series"

last_alert_times = {}
ALERT_COOLDOWN_MINUTES = 60
LOCAL_OFFSET_HOURS = 1


def fetch_data(symbol):
    params = {
        "symbol": symbol,
        "interval": INTERVAL,
        "apikey": os.getenv("TWELVE_DATA_API_KEY"),
        "outputsize": 50,
        "format": "JSON"
    }
    r = requests.get(TWELVE_DATA_URL, params=params)
    data = r.json()
    if "values" not in data:
        raise Exception(f"Failed to fetch {symbol}: {data}")

    df = pd.DataFrame(data["values"])
    df = df.rename(columns={
        "datetime": "timestamp",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close"
    })
    df = df.astype({
        "open": "float",
        "high": "float",
        "low": "float",
        "close": "float"
    })
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.set_index("timestamp", inplace=True)
    df = df.sort_index()
    return df


def compute_indicators(df):
    df["EMA 10"] = EMAIndicator(df["close"], window=10).ema_indicator()
    df["EMA 50"] = EMAIndicator(df["close"], window=50).ema_indicator()
    df["RSI"] = RSIIndicator(df["close"], window=14).rsi()
    df["ATR"] = AverageTrueRange(df["high"], df["low"], df["close"], window=14).average_true_range()
    df["Support"] = df["close"] - 2 * df["ATR"]
    df["Resistance"] = df["close"] + 2 * df["ATR"]
    return df


def insert_to_postgres(rows):
    try:
        print("\U0001F504 Connecting to Supabase PostgreSQL...")
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            database=os.getenv("PG_DB"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD")
        )
        print("✅ Connected to DB.")
        cur = conn.cursor()
        for row in rows:
            print("Inserting row:", row)
            cur.execute("""
                INSERT INTO forex_history (
                 timestamp, pair, open, high, low, close, ema10, ema50, rsi, atr, support, resistance
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row["timestamp"], row["pair"], row["open"], row["high"], row["low"], row["close"],
                row["EMA 10"], row["EMA 50"], row["RSI"], row["ATR"], row["support"], row["resistance"]
            ))
        conn.commit()
        cur.close()
        conn.close()
        print("✅ PostgreSQL updated.")
    except Exception as e:
        print("❌ PostgreSQL error:", e)


def append_to_google_sheets(rows):
    try:
        print("\U0001F504 Connecting to Google Sheets...")
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds_dict = json.loads(os.getenv("GSPREAD_KEY_JSON"))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1n6CtgC-niE5NYCMsA_MLNOwy_79ID_2oMnTP64DUx28/edit")
        ws = sheet.sheet1
        print("✅ Connected. Appending rows...")
        for row in rows:
            timestamp_local = row["timestamp"] + pd.Timedelta(hours=LOCAL_OFFSET_HOURS)
            ws.append_row([
                timestamp_local.strftime("%Y-%m-%d %H:%M:%S"),
                row["pair"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["EMA 10"],
                row["EMA 50"],
                row["RSI"],
                row["ATR"],
                row["support"],
                row["resistance"],
                row.get("trend_direction", ""),
                row.get("sentiment_summary", ""),
                row.get("news_summary", "")
            ])
        print("✅ Google Sheets updated successfully.")
    except Exception as e:
        print("❌ Google Sheets error:", e)


def send_telegram_alert(rows):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    bot = telegram.Bot(token=token)
    now = datetime.utcnow()

    for row in rows:
        pair = row["pair"]
        price = row["close"]
        rsi = row["RSI"]
        threshold = THRESHOLDS[pair]
        support = row["support"]
        resistance = row["resistance"]
        trend = row.get("trend_direction", "")
        sentiment = row.get("sentiment_summary", "")
        news = row.get("news_summary", "")

        alert_key = (pair, "above" if price > threshold else "below")
        last_alert_time = last_alert_times.get(alert_key)

        if last_alert_time and (now - last_alert_time < timedelta(minutes=ALERT_COOLDOWN_MINUTES)):
            continue

        timestamp_local = row["timestamp"] + pd.Timedelta(hours=LOCAL_OFFSET_HOURS)
        alert_msg = f"\U0001F6A8 {pair} {'ABOVE' if price > threshold else 'BELOW'} {threshold:.4f} at {price:.4f}\n"
        alert_msg += f"\n🕒 {timestamp_local.strftime('%Y-%m-%d %H:%M:%S')}\n"
        alert_msg += f"Price: {price:.4f} | RSI: {rsi:.2f}\nTrend: {trend}\n"

        if price <= support:
            alert_msg += "\n📉 Price at or below support zone"
        elif price >= resistance:
            alert_msg += "\n📈 Price at or above resistance zone"

        if sentiment:
            alert_msg += f"\n🧠 Sentiment: {sentiment}"
        if news:
            alert_msg += f"\n📰 News: {news}"

        alert_msg += "\n#forex #RSI #EMA"

        bot.send_message(chat_id=chat_id, text=alert_msg)
        last_alert_times[alert_key] = now

def send_news_and_sentiment_alerts():
    print("\U0001F4F0 Fetching news and sentiment alerts...")
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    bot = telegram.Bot(token=token)
    now = datetime.utcnow()
    hour = now.hour

    # Send only at 06:00 or 18:00 UTC
    if hour not in [6, 18]:
        return

    headers = []
    for pair in PAIRS:
        symbol = PAIRS[pair].replace("/", "")
        # --- News ---
        news_url = "https://api.twelvedata.com/news"
        news_params = {
            "symbol": symbol,
            "apikey": os.getenv("TWELVE_DATA_API_KEY"),
            "limit": 3
        }
        try:
            news_res = requests.get(news_url, params=news_params)
            news_data = news_res.json()
            if "data" in news_data:
                for item in news_data["data"]:
                    published_time = item["date_published"]
                    headline = item["title"]
                    source = item["source"]
                    link = item["url"]
                    headers.append(f"\U0001F4F0 *{pair} News*\n• {headline}\n_Source: {source}_\n[Read More]({link})")
        except Exception as e:
            print(f"❌ News fetch error for {pair}:", e)

        # --- Sentiment ---
        sentiment_url = f"https://api.twelvedata.com/sentiment"
        sentiment_params = {
            "symbol": symbol,
            "apikey": os.getenv("TWELVE_DATA_API_KEY")
        }
        try:
            sent_res = requests.get(sentiment_url, params=sentiment_params)
            sent_data = sent_res.json()
            if "data" in sent_data and len(sent_data["data"]) > 0:
                sentiment_score = sent_data["data"][0].get("sentiment", "N/A")
                headers.append(f"\U0001F4AC *{pair} Sentiment*: _{sentiment_score}_")
        except Exception as e:
            print(f"❌ Sentiment fetch error for {pair}:", e)

    if headers:
        bot.send_message(chat_id=chat_id, text="\n\n".join(headers), parse_mode=telegram.constants.ParseMode.MARKDOWN)
        print("✅ News and sentiment alerts sent.")
    else:
        print("⚠️ No news or sentiment alerts found.")

def main():
    all_data = []
    for pair, symbol in PAIRS.items():
        df = fetch_data(symbol)
        df = compute_indicators(df)
        latest = df.iloc[-1]
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
            "resistance": latest["Resistance"]
        }
        all_data.append(row)

    insert_to_postgres(all_data)
    append_to_google_sheets(all_data)
    send_telegram_alert(all_data)
    send_news_and_sentiment_alerts() 
    if __name__ == "__main__":
          main()







