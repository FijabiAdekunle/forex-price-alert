import os
import json
import requests
import pandas as pd
from datetime import datetime
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
INTERVAL = "15min"
TWELVE_DATA_URL = "https://api.twelvedata.com/time_series"

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
    return df

def insert_to_postgres(rows):
    try:
        print("🔄 Connecting to Supabase PostgreSQL...")
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
                row["timestamp"],
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
                row["resistance"]
            ))
        conn.commit()
        cur.close()
        conn.close()
        print("✅ PostgreSQL updated.")
    except Exception as e:
        print("❌ PostgreSQL error:", e)

def append_to_google_sheets(rows):
    try:
        print("🔄 Connecting to Google Sheets...")
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds_dict = json.loads(os.getenv("GSPREAD_KEY_JSON"))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1n6CtgC-niE5NYCMsA_MLNOwy_79ID_2oMnTP64DUx28/edit")
        ws = sheet.sheet1
        print("✅ Connected. Appending rows...")
        for row in rows:
            print("Appending row:", row)
            ws.append_row([
                row["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
                row["pair"],
                row["close"],
                row["high"],
                row["low"],
                row["close"],
                row["EMA 10"],
                row["EMA 50"],
                row["RSI"],
                row["ATR"],
                row["support"],
                row["resistance"]
            ])
        print("✅ Google Sheets updated successfully.")
    except Exception as e:
        print("❌ Google Sheets error:", e)


def send_telegram_alert(rows):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    bot = telegram.Bot(token=token)
    for row in rows:
        if row["RSI"] < 30 or row["RSI"] > 70:
            msg = f"⚠️ {row['pair']} ALERT\nPrice: {row['close']:.4f}\nRSI: {row['RSI']:.2f}\nTime: {row['timestamp']}"
            bot.send_message(chat_id=chat_id, text=msg)

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
            "support": None,
            "resistance": None
        }
        all_data.append(row)

    insert_to_postgres(all_data)
    append_to_google_sheets(all_data)
    send_telegram_alert(all_data)

if __name__ == "__main__":
    main()
