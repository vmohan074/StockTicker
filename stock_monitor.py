import configparser
from pushover_handler import send_pushover_notification
import yfinance as yf
import sys
import os
import google.generativeai as genai
#from dotenv import load_dotenv
#load_dotenv()

def read_config(config_path='config.ini'):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config


def get_stock_data(ticker):
    data = yf.Ticker(ticker)
    hist = data.history(period="3d")
    if len(hist) < 2:
        return None, None
    closes = hist['Close'].iloc[-2:]
    return float(closes.iloc[1]), float(closes.iloc[0])


def calculate_percentage_change(current, previous):
    if previous == 0:
        return 0.0
    return abs((current - previous) / previous) * 100


def main():
    config = read_config()
    tickers = [t.strip() for t in config['STOCKS']['tickers'].split(',')]
    threshold = float(config['SETTINGS']['volatility_threshold'])
    alerts = []

    # Set Google Gemini API key from environment variable only (for GitHub Actions security)
    gemini_api_key = os.getenv('GOOGLE_API_KEY')
    if gemini_api_key:
        genai.configure(api_key=gemini_api_key)
        gemini_model = genai.GenerativeModel('gemini-2.5-flash')
    else:
        gemini_model = None

    for ticker in tickers:
        current, previous = get_stock_data(ticker)
        if current is None or previous is None:
            continue
        pct_change = calculate_percentage_change(current, previous)
        if pct_change >= threshold:
            sign = '+' if current > previous else '-'
            alert_msg = f"{ticker}: {sign}{pct_change:.2f}% ({current:.2f} from {previous:.2f})"
            # If underperforming (negative change), get Gemini analysis
            if gemini_model and current < previous:
                user_prompt = f"For {ticker}, report the single, specific news event or market factor (e.g., earnings miss, downgrade, block deal, macro event) driving today's underperformance. Limit the entire answer to 40 words."
                try:
                    response = gemini_model.generate_content(user_prompt)
                    gemini_reason = response.text.strip()
                    alert_msg += f"\n {gemini_reason}"
                except Exception as e:
                    alert_msg += f"\nReason: (Gemini error: {e})"
            alerts.append(f"\n{alert_msg}")

    if alerts:
        title = "StockPulse ⚡ Alerts"
        message = '\n'.join(alerts)
        send_pushover_notification(title, message, config)

if __name__ == "__main__":
    main()

