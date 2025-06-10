import os
import requests
import pandas as pd
from ta.trend import EMAIndicator
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange
from datetime import datetime

# Load secrets from GitHub Actions env or .env (if running locally)
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
PAIRS = {
    "EUR/USD": "EURUSD",
    "GBP/USD": "GBPUSD",
    "USD/JPY": "USDJPY"
}

def fetch_forex_data(pair_code):
    url = f"https://www.alphavantage.co/query?function=FX_INTRADAY&from_symbol={pair_code[:3]}&to_symbol={pair_code[3:]}&interval=5min&apikey={ALPHAVANTAGE_API_KEY}&outputsize=compact"
    r = requests.get(url)
    data = r.json()

    if "Time Series FX (5min)" not in data:
        raise Exception(f"API Error: {data}")

    df = pd.DataFrame.from_dict(data["Time Series FX (5min)"], orient="index", dtype=float)
    df.columns = ['Open', 'High', 'Low', 'Close']
    df.index = pd.to_datetime(df.index)
    df.sort_index(inplace=True)

    return df

def calculate_indicators(df):
    df["EMA_10"] = EMAIndicator(df["Close"], window=10).ema_indicator()
    df["EMA_50"] = EMAIndicator(df["Close"], window=50).ema_indicator()
    df["RSI"] = RSIIndicator(df["Close"]).rsi()
    df["ATR"] = AverageTrueRange(df["High"], df["Low"], df["Close"]).average_true_range()
    return df

def main():
    results = []

    for name, code in PAIRS.items():
        try:
            df = fetch_forex_data(code)
            df = calculate_indicators(df)
            latest = df.iloc[-1]

            print(f"[{name}] {latest.name} | Price: {latest['Close']:.5f} | RSI: {latest['RSI']:.2f}")
            results.append({
                "timestamp": latest.name,
                "pair": name,
                "rate": latest["Close"],
                "high": latest["High"],
                "low": latest["Low"],
                "close": latest["Close"],
                "EMA 10": latest["EMA_10"],
                "EMA 50": latest["EMA_50"],
                "RSI": latest["RSI"],
                "ATR": latest["ATR"]
            })
        except Exception as e:
            print(f"Error processing {name}: {e}")
    
    # Placeholder for Google Sheets, Supabase, Telegram
    # We'll add each of them next.
    
if __name__ == "__main__":
    main()
