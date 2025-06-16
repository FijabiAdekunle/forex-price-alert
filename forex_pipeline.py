import os
import requests
import pandas as pd
from datetime import datetime

def fetch_data(symbol):
    """Robust data fetching with proper error handling"""
    try:
        api_key = os.getenv("TWELVE_DATA_API_KEY")
        if not api_key:
            raise ValueError("API key not found in environment variables")

        params = {
            "symbol": symbol,
            "interval": "15min",
            "apikey": api_key,
            "outputsize": 5  # Reduced for testing
        }

        print(f"DEBUG: Attempting to fetch {symbol} with params: {params}")  # Debug line

        response = requests.get(
            "https://api.twelvedata.com/time_series",
            params=params,
            timeout=10
        )

        print(f"DEBUG: Response status: {response.status_code}")  # Debug line
        print(f"DEBUG: Response text: {response.text[:200]}")  # Debug line

        response.raise_for_status()  # Raises HTTPError for bad responses
        data = response.json()

        if "values" not in data:
            error_msg = data.get("message", "No 'values' in response")
            raise ValueError(f"API Error: {error_msg}")

        df = pd.DataFrame(data["values"])
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values("datetime")
        df.set_index("datetime", inplace=True)
        
        numeric_cols = ["open", "high", "low", "close"]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
        
        return df

    except Exception as e:
        print(f"❌ Critical error fetching {symbol}: {str(e)}")
        raise

def main():
    print("=== Starting Debug Run ===")
    
    # Verify environment
    print("Environment variables:", {
        k: v for k, v in os.environ.items() 
        if "TWELVE" in k or "PG_" in k
    })
    
    # Test with just one pair first
    try:
        print("Testing EUR/USD...")
        df = fetch_data("EUR/USD")
        print("Success! Sample data:")
        print(df.tail(2))
    except Exception as e:
        print(f"Failed with error: {e}")

if __name__ == "__main__":
    main()