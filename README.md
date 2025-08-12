# 📊 Forex Data Pipeline & Price Dashboard
[![Forex-pipeline-Automation.png](https://i.postimg.cc/gkvPT8KX/Forex-pipeline-Automation.png)](https://postimg.cc/dLtX77pw)

A robust, end-to-end **Forex Data Pipeline** that:
- Pulls **live Forex data** via multiple APIs
- Calculates **EMA, RSI, ATR** and other key indicators
- Logs data into **PostgreSQL** & **Google Sheets**
- Sends **price alerts** to Telegram
- Displays a **real-time Google Sheets Dashboard**

---

## 🚀 Features

- **Live Forex Rates** for:
  - EUR/USD
  - GBP/USD
  - USD/JPY
- **Technical Indicators:**
  - EMA (10 & 50)
  - RSI
  - ATR
- **Data Storage:**
  - PostgreSQL (Neon) for historical logging
  - Google Sheets for real-time display
- **Alerts:**
  - Telegram notifications with trend, crossover, sentiment, and news
- **Visual Dashboard:**
  - Real-time KPI summary
  - Signal Table
  - Rate vs Time chart
  - RSI Trend chart

---

## 🖥 Dashboard Preview

**Signal Table + KPI Summary**  
[![Fx-Signl-kpi-Dashboard.png](https://i.postimg.cc/mkLbp7wY/Fx-Signl-kpi-Dashboard.png)](https://postimg.cc/XGPSpGtq)

**Rate vs Timestamp Chart**  
[![Fx-Rate-Chart-Dashboard.png](https://i.postimg.cc/RFQSTgcC/Fx-Rate-Chart-Dashboard.png)](https://postimg.cc/WFzcN6Wx)

**RSI Trend Chart**  
[![Fx-RSI-Chart-Dashboard.png](https://i.postimg.cc/Hn6TqjZf/Fx-RSI-Chart-Dashboard.png)](https://postimg.cc/vx6R98dz)

---

## 🌐 APIs Used & Purpose

| API | Purpose |
|------|---------|
| **Twelve Data API** | Fetch real-time & historical Forex OHLC prices for EUR/USD, GBP/USD, USD/JPY |
| **NewsAPI** | Pull latest market-related headlines for the base currency |
| **Finnhub API** | Retrieve sentiment scores and categorize them (Bullish, Bearish, etc.) |
| **Google Sheets API** | Store data in Google Sheets for real-time dashboard display |
| **Telegram Bot API** | Send instant price alerts with market context to the user’s chat |

---

## 🛠 Tech Stack

- **Backend:** Python 3.10+
- **APIs:** Twelve Data, NewsAPI, Finnhub, Google Sheets API, Telegram Bot API
- **Database:** PostgreSQL (Neon)
- **Libraries:** pandas, requests, psycopg2, gspread, python-dotenv, asyncio, ta
- **Environment Management:** python-dotenv
- **Visualization:** Google Sheets dashboard

---





## Set up your .env file:
### API Keys
TWELVE_DATA_API_KEY=your_twelve_data_key
NEWSAPI_KEY=your_newsapi_key
FINNHUB_API_KEY=your_finnhub_key

### PostgreSQL (Neon)
PG_HOST=your_host
PG_PORT=5432
PG_DB=forex_db
PG_USER=postgres
PG_PASSWORD=your_password

### Telegram Bot
TELEGRAM_BOT_TOKEN=your_telegram_token
TELEGRAM_CHAT_ID=your_chat_id

### Google Sheets
GOOGLE_SHEET_NAME=YourGoogleSheetName

## ▶ Usage
- Run the main pipeline:
> python forex_pipeline.py

This will:

- Fetch Forex rates and compute technical indicators

- Log results to PostgreSQL and Google Sheets

- Fetch latest news and sentiment

- Send formatted alerts to Telegram

- Update the Google Sheets dashboard in real-time

### 📊 Example Output in Google 
| Timestamp           | Pair    | Open   | High   | Low    | Close  | EMA10  | EMA50  | RSI  | ATR     | Support | Resistance | Trend   | Crossover               | Sentiment | News                       |
| ------------------- | ------- | ------ | ------ | ------ | ------ | ------ | ------ | ---- | ------- | ------- | ---------- | ------- | ----------------------- | --------- | -------------------------- |
| 2025-08-09 09:23:18 | EUR/USD | 1.1660 | 1.1670 | 1.1655 | 1.1661 | 1.1662 | 1.1659 | 55.2 | 0.00064 | 1.1650  | 1.1675     | Uptrend | EMA10 > EMA50 (Bullish) | Bullish   | ECB rate decision expected |

## 🔒 Security
*All API keys and credentials are stored in  GitHub Secrets.*

**Never commit .env or gspread_key.json to publicrepositories.**

👨‍💻 Author

**Fijabi J. Adekunle**

>**Motto**: *Navigating Data | Unveiling Insights | Driving Impacts*
