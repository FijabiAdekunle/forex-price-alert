import os
import asyncio
import psycopg2
import gspread
import telegram
import pandas as pd
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

# Initialize services
def init_services():
    """Initialize all connections with error handling"""
    services = {}
    
    # Telegram
    try:
        services['telegram'] = telegram.Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
        print("✅ Telegram initialized")
    except Exception as e:
        print(f"❌ Telegram init failed: {str(e)}")
    
    # Supabase
    try:
        services['supabase'] = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            dbname=os.getenv("PG_DB"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            sslmode="require"
        )
        print("✅ Supabase connected")
    except Exception as e:
        print(f"❌ Supabase connection failed: {str(e)}")
    
    # Google Sheets
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            json.loads(os.getenv("GSPREAD_KEY_JSON")), scope)
        services['sheets'] = gspread.authorize(creds)
        print("✅ Google Sheets authorized")
    except Exception as e:
        print(f"❌ Google Sheets auth failed: {str(e)}")
    
    return services

async def run_pipeline():
    services = init_services()
    
    # 1. Fetch Data (from your working debug version)
    df = fetch_data("EUR/USD")
    latest = df.iloc[-1]
    
    # 2. Prepare data
    data = {
        "timestamp": datetime.utcnow(),
        "pair": "EUR/USD",
        "price": latest["close"],
        # ... add other fields ...
    }
    
    # 3. Send alerts and update services
    try:
        # Telegram
        if 'telegram' in services:
            await services['telegram'].send_message(
                chat_id=os.getenv("TELEGRAM_CHAT_ID"),
                text=f"EUR/USD Price Alert: {latest['close']}"
            )
        
        # Supabase
        if 'supabase' in services:
            with services['supabase'].cursor() as cur:
                cur.execute("""
                    INSERT INTO forex_history (timestamp, pair, close)
                    VALUES (%s, %s, %s)
                """, (data["timestamp"], data["pair"], data["price"]))
                services['supabase'].commit()
        
        # Google Sheets
        if 'sheets' in services:
            sheet = services['sheets'].open(os.getenv("GOOGLE_SHEET_NAME")).sheet1
            sheet.append_row([
                data["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
                data["pair"],
                data["price"]
            ])
            
        print("✅ Pipeline completed successfully")
        
    except Exception as e:
        print(f"❌ Pipeline error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(run_pipeline())