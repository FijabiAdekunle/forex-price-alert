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

# Configuration
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
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1n6CtgC-niE5NYCMsA_MLNOwy_79ID_2oMnTP64DUx28/edit"

last_alert_times = {}
ALERT_COOLDOWN_MINUTES = 60
LOCAL_OFFSET_HOURS = 1

def log_message(message):
    """Helper function for consistent logging"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def fetch_data(symbol):
    """Fetch forex data from Twelve Data API"""
    try:
        params = {
            "symbol": symbol,
            "interval": INTERVAL,
            "apikey": os.getenv("TWELVE_DATA_API_KEY"),
            "outputsize": 50,
            "format": "JSON"
        }
        log_message(f"Fetching data for {symbol}...")
        r = requests.get(TWELVE_DATA_URL, params=params, timeout=10)
        data = r.json()
        
        if "values" not in data:
            raise Exception(f"API response missing 'values': {data}")
        
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
        log_message(f"Successfully fetched {len(df)} records for {symbol}")
        return df
        
    except Exception as e:
        log_message(f"❌ Error fetching data for {symbol}: {str(e)}")
        raise

def compute_indicators(df):
    """Calculate technical indicators"""
    try:
        df["EMA 10"] = EMAIndicator(df["close"], window=10).ema_indicator()
        df["EMA 50"] = EMAIndicator(df["close"], window=50).ema_indicator()
        df["RSI"] = RSIIndicator(df["close"], window=14).rsi()
        df["ATR"] = AverageTrueRange(df["high"], df["low"], df["close"], window=14).average_true_range()
        df["Support"] = df["close"] - 2 * df["ATR"]
        df["Resistance"] = df["close"] + 2 * df["ATR"]
        return df
    except Exception as e:
        log_message(f"❌ Error computing indicators: {str(e)}")
        raise

def insert_to_postgres(rows):
    """Insert data into Supabase PostgreSQL"""
    try:
        log_message("Connecting to Supabase PostgreSQL...")
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            database=os.getenv("PG_DB"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            connect_timeout=5
        )
        
        # Test connection
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            if cur.fetchone()[0] != 1:
                raise Exception("Connection test failed")
        
        log_message("✅ Connected to DB. Inserting rows...")
        with conn.cursor() as cur:
            for row in rows:
                cur.execute("""
                    INSERT INTO forex_history (
                        timestamp, pair, open, high, low, close, 
                        ema10, ema50, rsi, atr, support, resistance
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row["timestamp"], row["pair"], 
                    row["open"], row["high"], row["low"], row["close"],
                    row["EMA 10"], row["EMA 50"], 
                    row["RSI"], row["ATR"], 
                    row["support"], row["resistance"]
                ))
        conn.commit()
        log_message(f"✅ Successfully inserted {len(rows)} rows to PostgreSQL")
        
    except Exception as e:
        log_message(f"❌ PostgreSQL error: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

def append_to_google_sheets(rows):
    """Append data to Google Sheets"""
    try:
        log_message("Connecting to Google Sheets...")
        
        # Verify credentials
        gspread_json = os.getenv("GSPREAD_KEY_JSON")
        if not gspread_json:
            raise ValueError("GSPREAD_KEY_JSON environment variable is missing")
            
        creds_dict = json.loads(gspread_json)
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(GOOGLE_SHEET_URL)
        ws = sheet.sheet1
        
        log_message("✅ Connected. Appending rows...")
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
        log_message("✅ Google Sheets updated successfully")
        
    except Exception as e:
        log_message(f"❌ Google Sheets error: {str(e)}")

def send_telegram_alert(rows):
    """Send price alerts via Telegram"""
    try:
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        
        if not token or not chat_id:
            raise ValueError("Missing Telegram credentials")
            
        bot = telegram.Bot(token=token)
        now = datetime.utcnow()
        alerts_sent = 0
        
        for row in rows:
            pair = row["pair"]
            price = row["close"]
            threshold = THRESHOLDS[pair]
            
            # Check if we should send alert
            alert_key = (pair, "above" if price > threshold else "below")
            last_alert_time = last_alert_times.get(alert_key)
            
            if last_alert_time and (now - last_alert_time < timedelta(minutes=ALERT_COOLDOWN_MINUTES)):
                continue
                
            # Prepare alert message
            timestamp_local = row["timestamp"] + pd.Timedelta(hours=LOCAL_OFFSET_HOURS)
            message = f"\U0001F6A8 {pair} {'ABOVE' if price > threshold else 'BELOW'} {threshold:.4f} at {price:.4f}\n"
            message += f"\n🕒 {timestamp_local.strftime('%Y-%m-%d %H:%M:%S')}\n"
            message += f"Price: {price:.4f} | RSI: {row['RSI']:.2f}\n"
            
            if price <= row["support"]:
                message += "\n📉 Price at or below support zone"
            elif price >= row["resistance"]:
                message += "\n📈 Price at or above resistance zone"
                
            message += "\n#forex #alerts"
            
            # Send alert
            bot.send_message(chat_id=chat_id, text=message)
            last_alert_times[alert_key] = now
            alerts_sent += 1
            
        log_message(f"✅ Sent {alerts_sent} Telegram alerts")
        
    except Exception as e:
        log_message(f"❌ Telegram alert failed: {str(e)}")

def send_news_and_sentiment_alerts():
    """Send scheduled news/sentiment updates"""
    try:
        log_message("Checking for news/sentiment alerts...")
        
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not token or not chat_id:
            raise ValueError("Missing Telegram credentials")
            
        bot = telegram.Bot(token=token)
        now = datetime.utcnow()
        hour = now.hour
        
        # Only send at 06:00 or 18:00 UTC
        if hour not in [6, 18]:
            log_message("Not scheduled time for news alerts")
            return
            
        alerts = []
        api_key = os.getenv("TWELVE_DATA_API_KEY")
        
        for pair in PAIRS:
            symbol = PAIRS[pair].replace("/", "")
            
            # Fetch news
            try:
                news_res = requests.get(
                    "https://api.twelvedata.com/news",
                    params={"symbol": symbol, "apikey": api_key, "limit": 3},
                    timeout=10
                )
                news_data = news_res.json()
                
                if "data" in news_data and news_data["data"]:
                    for item in news_data["data"][:3]:  # Limit to 3 news items
                        alerts.append(
                            f"\U0001F4F0 *{pair} News*\n"
                            f"• {item['title']}\n"
                            f"_Source: {item['source']}_\n"
                            f"[Read More]({item['url']})"
                        )
            except Exception as e:
                log_message(f"News fetch error for {pair}: {str(e)}")
                
            # Fetch sentiment
            try:
                sent_res = requests.get(
                    "https://api.twelvedata.com/sentiment",
                    params={"symbol": symbol, "apikey": api_key},
                    timeout=10
                )
                sent_data = sent_res.json()
                
                if "data" in sent_data and sent_data["data"]:
                    sentiment = sent_data["data"][0].get("sentiment", "N/A")
                    alerts.append(f"\U0001F4AC *{pair} Sentiment*: _{sentiment}_")
            except Exception as e:
                log_message(f"Sentiment fetch error for {pair}: {str(e)}")
                
        # Send all alerts if any
        if alerts:
            bot.send_message(
                chat_id=chat_id,
                text="\n\n".join(alerts),
                parse_mode=telegram.constants.ParseMode.MARKDOWN,
                disable_web_page_preview=True
            )
            log_message(f"✅ Sent {len(alerts)} news/sentiment alerts")
        else:
            log_message("⚠️ No news or sentiment alerts found")
            
    except Exception as e:
        log_message(f"❌ News/sentiment alerts failed: {str(e)}")

def main():
    """Main pipeline function"""
    try:
        log_message("=== Starting Forex Pipeline ===")
        
        # Verify critical environment variables
        required_vars = [
            "TWELVE_DATA_API_KEY",
            "PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASSWORD",
            "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID",
            "GSPREAD_KEY_JSON"
        ]
        
        for var in required_vars:
            if not os.getenv(var):
                raise ValueError(f"Missing required environment variable: {var}")
        
        # Process each currency pair
        all_data = []
        for pair, symbol in PAIRS.items():
            try:
                df = fetch_data(symbol)
                df = compute_indicators(df)
                latest = df.iloc[-1]  # Get most recent data point
                
                all_data.append({
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
                })
                
            except Exception as e:
                log_message(f"❌ Error processing {pair}: {str(e)}")
                continue
        
        if not all_data:
            raise Exception("No data was successfully processed")
        
        # Save and alert
        insert_to_postgres(all_data)
        append_to_google_sheets(all_data)
        send_telegram_alert(all_data)
        send_news_and_sentiment_alerts()
        
        log_message("=== Pipeline Completed Successfully ===")
        
    except Exception as e:
        log_message(f"❌ Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()