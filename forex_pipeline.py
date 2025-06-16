import os
import requests
import logging
import psycopg2
import pandas as pd
import gspread
import telegram
from datetime import datetime
import asyncio
from oauth2client.service_account import ServiceAccountCredentials

import os
import requests
import pandas as pd
import numpy as np
import logging
from datetime import datetime
import psycopg2
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Bot
import asyncio
from telegram.constants import ParseMode
from sqlalchemy import create_engine


# Set up logging to file
logging.basicConfig(
    filename='log.txt',
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
)

# Load environment variables
POSTGRES_URL = os.getenv("POSTGRES_URL")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

# Validate critical env vars
if not POSTGRES_URL:
    raise ValueError("POSTGRES_URL not set in environment")

if not GOOGLE_SHEET_NAME:
    raise ValueError("GOOGLE_SHEET_NAME not set in environment")

bot = Bot(token=TELEGRAM_TOKEN)

# Google Sheets setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("gspread_key.json", scope)
client = gspread.authorize(creds)

try:
    sheet = client.open(GOOGLE_SHEET_NAME).sheet1
except Exception as e:
    logging.error(f"Google Sheet open error: {e}")
    sheet = None

# Supabase/PSQL setup
def insert_to_postgres(df, table="forex_history"):
    try:
        import sqlalchemy
        engine = sqlalchemy.create_engine(POSTGRES_URL)
        df.to_sql(table, engine, if_exists="append", index=False)
        logging.info("Data pushed to PostgreSQL")
    except Exception as e:
        logging.error(f"Supabase error: {e}")

# Telegram send
async def send_telegram_alert(message):
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
        logging.info("Telegram alert sent")
    except Exception as e:
        logging.error(f"Telegram send error: {e}")

# Main processing

def main():
    pairs = ["EUR/USD", "GBP/USD", "USD/JPY"]

    for pair in pairs:
        try:
            # --- Replace with real fetching logic
            response = requests.get(
                f"https://api.twelvedata.com/time_series?symbol={pair}&interval=15min&apikey={TWELVE_DATA_API_KEY}&outputsize=1"
            )
            data = response.json()
            if "values" not in data:
                raise ValueError(f"Twelve Data returned error: {data}")

            df = pd.DataFrame(data["values"])
            df["pair"] = pair
            df["timestamp"] = pd.to_datetime(df["datetime"])
            df["rsi"] = np.random.uniform(40, 70)  # Replace with real RSI
            df["atr"] = np.random.uniform(0.5, 2.0)  # Replace with real ATR
            df["trend"] = np.where(df["rsi"] > 50, "Uptrend", "Downtrend")
            df["sentiment"] = "N/A"  # Placeholder for real sentiment
            df["news"] = "No major news"  # Placeholder for real news

            # --- Telegram alert
            msg = (
                f"🚨 {pair} {df['trend'].iloc[0].upper()}\n"
                f"🕒 {df['timestamp'].iloc[0]}\n"
                f"Price: {df['close'].iloc[0]} | RSI: {df['rsi'].iloc[0]:.2f}\n"
                f"Trend: {df['trend'].iloc[0]}\n"
                f"Sentiment: {df['sentiment'].iloc[0]}\n"
                f"News: {df['news'].iloc[0]}\n#forex #RSI #EMA"
            )
            asyncio.run(send_telegram_alert(msg))

            # --- Save to Google Sheet
            if sheet:
                row = [
                    df['timestamp'].iloc[0], pair, df['open'].iloc[0], df['high'].iloc[0],
                    df['low'].iloc[0], df['close'].iloc[0], df['rsi'].iloc[0], df['atr'].iloc[0],
                    df['trend'].iloc[0], df['sentiment'].iloc[0], df['news'].iloc[0]
                ]
                sheet.append_row([str(x) for x in row])

            # --- Push to Supabase
            insert_to_postgres(df[["timestamp", "pair", "open", "high", "low", "close", "rsi", "atr", "trend"]])

        except Exception as e:
            logging.error(f"Error processing {pair}: {e}")

if __name__ == "__main__":
    main()
