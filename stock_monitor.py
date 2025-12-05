import configparser
from pushover_handler import send_pushover_notification
import yfinance as yf
import sys
import os
from groq import Groq
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


def analyze_with_groq(groq_client, ticker, percentage_change):
    """Generate concise analysis using Groq AI with improved prompting"""
    try:
        # Improved prompt - more specific and constrained
        user_prompt = f"""Indian stock {ticker} dropped {abs(percentage_change):.2f}% today.

Task: Explain in sentence (max 50 words) the most likely market reason.
Focus on: sector trends, company news, or market-wide factors.
Format: "Likely due to [specific reason]."
Do NOT speculate or use phrases like "might be" or "could be"."""

        response = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {
                    "role": "system",
                    "content": "You are a financial analyst specializing in Indian stock markets (NSE/BSE). Provide factual, concise explanations only."
                },
                {
                    "role": "user",
                    "content": user_prompt
                }
            ],
            max_tokens=50,
            temperature=0.3,
            top_p=0.9
        )
        
        result = response.choices[0].message.content.strip()
        
        # Filter out garbage responses
        garbage_phrases = ["i don't have", "i cannot", "as an ai", "no specific information", "i'm not able"]
        if any(phrase in result.lower() for phrase in garbage_phrases):
            return "Market volatility affected this stock today."
        
        return result
        
    except Exception as e:
        return f"Analysis unavailable: {str(e)}"


def main():
    config = read_config()
    tickers = [t.strip() for t in config['STOCKS']['tickers'].split(',')]
    threshold = float(config['SETTINGS']['volatility_threshold'])
    alerts = []

    # Set Groq API key from environment variable only (for GitHub Actions security)
    groq_api_key = os.getenv('GROQ_API_KEY')
    if groq_api_key:
        groq_client = Groq(api_key=groq_api_key)
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
                groq_reason = analyze_with_groq(groq_client, ticker, pct_change)
                alert_msg += f"\n {groq_reason}"
            alerts.append(f"\n{alert_msg}")

    if alerts:
        # Split alerts into batches of 3 to avoid notification size limits
        batch_size = 3
        for i in range(0, len(alerts), batch_size):
            batch = alerts[i:i + batch_size]
            batch_number = (i // batch_size) + 1
            total_batches = (len(alerts) + batch_size - 1) // batch_size
            
            # Add batch info if multiple batches
            if total_batches > 1:
                title = f"StockPulse ⚡ Alerts ({batch_number}/{total_batches})"
            else:
                title = "StockPulse ⚡ Alerts"
            
            message = '\n'.join(batch)
            send_pushover_notification(title, message, config)

if __name__ == "__main__":
    main()

