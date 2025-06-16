import os
import requests
import logging
import psycopg2
import pandas as pd
import gspread
import telegram
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from telegram.constants import ParseMode
from sqlalchemy import create_engine

# Setup Logging
log_filename = f"log_{datetime.utcnow().strftime('%Y-%m-%d')}.txt"
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def log_message(msg):
    print(msg)
    logging.info(msg)


# Pairs config
PAIRS = {
    "EUR/USD": "EUR/USD",
    "GBP/USD": "GBP/USD",
    "USD/JPY": "USD/JPY"
}


def fetch_data(symbol):
    url = f"https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": "15min",
        "outputsize": 50,
        "apikey": os.getenv("TWELVE_DATA_API_KEY")
    }
    response = requests.get(url, params=params)
    data = response.json()
    if "values" not in data:
        raise ValueError(f"Twelve Data returned error: {data}")
    df = pd.DataFrame(data["values"])
    df = df.rename(columns={"datetime": "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.astype(float, errors="ignore")
    return df


def calculate_indicators(df):
    df = df.sort_values("timestamp")
    df["ema10"] = df["close"].ewm(span=10).mean()
    df["ema50"] = df["close"].ewm(span=50).mean()
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -1 * delta.clip(upper=0)
    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()
    rs = avg_gain / avg_loss
    df["rsi"] = 100 - (100 / (1 + rs))
    df["atr"] = df[["high", "low"]].max(axis=1) - df[["high", "low"]].min(axis=1)
    df["trend"] = df.apply(lambda row: "Uptrend" if row["ema10"] > row["ema50"] else "Downtrend", axis=1)
    df["cross"] = df["ema10"] > df["ema50"]
    return df


def push_to_postgres(df, pair):
    try:
        engine = create_engine(os.getenv("POSTGRES_URL"))
        df["pair"] = pair
        df.to_sql("forex_history", engine, if_exists="append", index=False)
        log_message(f"✅ Logged {pair} to Supabase")
    except Exception as e:
        log_message(f"❌ Supabase error for {pair}: {e}")


def push_to_gsheet(df):
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("gspread_key.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open(os.getenv("GOOGLE_SHEET_NAME")).sheet1
        latest = df.iloc[-1]
        row = [
            latest.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            latest.pair,
            latest["open"], latest["high"], latest["low"], latest["close"],
            latest["ema10"], latest["ema50"], latest["rsi"], latest["atr"],
            latest["trend"], "Cross" if latest.cross else "No Cross"
        ]
        sheet.append_row(list(map(str, row)))
        log_message("✅ Updated Google Sheet")
    except Exception as e:
        log_message(f"❌ Google Sheet error: {e}")


def send_telegram_alert(df, pair):
    try:
        bot = telegram.Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        row = df.iloc[-1]
        message = (
            f"🚨 {pair} {row['trend'].upper()}\n"
            f"🕒 {row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Price: {row['close']:.5f} | RSI: {row['rsi']:.2f}\n"
            f"Trend: {row['trend']}\n"
            f"EMA Cross: {'Yes' if row.cross else 'No'}\n"
            f"ATR: {row['atr']:.2f}\n"
            f"#forex #RSI #EMA"
        )
        bot.send_message(chat_id=chat_id, text=message)
        log_message(f"✅ Telegram alert sent for {pair}")
    except Exception as e:
        log_message(f"❌ Telegram error for {pair}: {e}")


def main():
    for pair, symbol in PAIRS.items():
        try:
            df = fetch_data(symbol.replace("/", ""))
            df = calculate_indicators(df)
            df["pair"] = pair
            push_to_postgres(df, pair)
            push_to_gsheet(df)
            send_telegram_alert(df, pair)
        except Exception as e:
            log_message(f"❌ Error processing {pair}: {e}")


if __name__ == "__main__":
    main()
