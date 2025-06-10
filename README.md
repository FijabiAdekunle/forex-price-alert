# 📊 Forex Data Pipeline
[![Chat-GPT-Image-Jun-1-2025-10-08-53-PM.png](https://i.postimg.cc/WbVnfVJC/Chat-GPT-Image-Jun-1-2025-10-08-53-PM.png)](https://postimg.cc/QV61HRmJ)

A robust end-to-end pipeline that pulls live Forex data using the Alpha Vantage API, calculates key technical indicators (EMA, RSI, ATR), stores data in both PostgreSQL and Google Sheets, and sends price alerts via Telegram.

---

## 🚀 Features

- 📈 **Live Forex Rates** for EUR/USD, GBP/USD, and USD/JPY
- 🧮 **Technical Indicators**:
  - RSI (Relative Strength Index)
  - EMA (Exponential Moving Average)
  - ATR (Average True Range)
- 🗄️ **PostgreSQL Database Logging**
- 📤 **Google Sheets Logging** (via Service Account)
- 📢 **Telegram Alerts** for user-defined price thresholds

---

## 🛠️ Tech Stack

- Python 3.10+
- [Alpha Vantage API](https://www.alphavantage.co/)
- PostgreSQL
- gspread + Google Service Account
- Telegram Bot API
- `python-dotenv` for secrets
- `psycopg2`, `gspread`, `requests`, `pandas`, `ta`, etc.

---

## 📦 Installation

1. **Clone the repository:**


git clone https://github.com/yourusername/forex-data-pipeline.git
cd forex-data-pipeline

2. **Install dependencies:**
- pip install -r requirements.txt

3. **Set up environment variables:**
- Create a .env file in the root directory with the following:

# AlphaVantage API
ALPHAVANTAGE_API_KEY="your_alpha_vantage_key"

# PostgreSQL Configuration
PG_HOST=localhost
PG_PORT=5432
PG_DB=forex_db
PG_USER=postgres
PG_PASSWORD=yourpassword

# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN="your_bot_token"
TELEGRAM_CHAT_ID="your_chat_id"

# Google Sheets JSON (only for CI/CD environments)
GSPREAD_KEY_JSON='{"type":"service_account", ...}'  # Optional: encoded JSON as a string

# 🧪 Usage
- Run the main script:

`python forex_pipeline.py`

This will:

- Fetch Forex rates and compute indicators

- Log results to Google Sheets and PostgreSQL

- Send alerts via Telegram (if thresholds are breached)


## 📊 Example Google Sheet
- The output logs include: `timestamp`, `symbol`, `price`, `RSI`, `EMA`, `ATR`, `alert_triggered`

- Useful for real-time monitoring and strategy refinement.

## 🔒 Security

- All API keys and credentials are stored in `.env` or `GitHub Secrets`.

*Do not commit .env or gspread_key.json files to public repositories.*


# 👨‍💻 Author
**Fijabi J. Adekunle**

*Data Scientist | Trader | Marine Engineer*

**Motto: Navigating Data | Unveiling Insights | Driving Impacts**
