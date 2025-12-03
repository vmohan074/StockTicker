import configparser
from pushover_handler import send_pushover_notification
import yfinance as yf
import sys
import os
from groq import Groq


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

    # Set Groq API key from environment variable only (for GitHub Actions security)
    groq_api_key = os.getenv('GROQ_API_KEY')
    if groq_api_key:
        groq_client = Groq()
    else:
        groq_client = None

    for ticker in tickers:
        current, previous = get_stock_data(ticker)
        if current is None or previous is None:
            continue
        pct_change = calculate_percentage_change(current, previous)
        if pct_change >= threshold:
            sign = '+' if current > previous else '-'
            alert_msg = f"{ticker}: {sign}{pct_change:.2f}% ({current:.2f} from {previous:.2f})"
            # If underperforming (negative change), get Groq analysis
            if groq_client and current < previous:
                user_prompt = f"For {ticker}, report the single, specific news event or market factor (e.g., earnings miss, downgrade, block deal, macro event) driving today's underperformance. State the reason and its short-term nature (Fundamental/Non-Fundamental). Limit the entire answer to 50 words."
                messages_payload = [
                    {"role": "system", "content": "You are an Emergency Market Analyst. Your sole function is to perform rapid, high-priority root cause analysis for sudden stock price drops. You must be concise, accurate, and focus on events within the last 24-72 hours. Your output MUST ONLY contain the structured analysis; absolutely no greetings, introductory text, or conversational filler. If you cannot determine a reason based on public information, respond with 'No significant public information available.'"},
                    {"role": "user", "content": user_prompt}
                ]
                try:
                    chat_completion = groq_client.chat.completions.create(
                        messages=messages_payload,
                        model="llama-3.1-8b-instant",
                        temperature=0.7
                    )
                    groq_reason = chat_completion.choices[0].message.content.strip()
                    alert_msg += f"\n {groq_reason}"
                except Exception as e:
                    alert_msg += f"\nReason: (Groq error: {e})"
            alerts.append(f"\n{alert_msg}")

    if alerts:
        title = "StockPulse ⚡ Alert"
        message = '\n'.join(alerts)
        send_pushover_notification(title, message, config)

if __name__ == "__main__":
    main()
