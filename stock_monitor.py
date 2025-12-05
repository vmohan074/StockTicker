import configparser
from pushover_handler import send_pushover_notification
import yfinance as yf
import sys
import os
from groq import Groq
from datetime import datetime
#from dotenv import load_dotenv
#load_dotenv()

def read_config(config_path='config.ini'):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config


def get_stock_data(ticker):
    """Fetch stock data with better error handling and debugging"""
    try:
        data = yf.Ticker(ticker)
        # Fetch last 5 days to ensure we have enough data
        hist = data.history(period="5d")
        
        if len(hist) < 2:
            print(f"⚠️ {ticker}: Not enough data (got {len(hist)} days)")
            return None, None, None, None
        
        # Get last 2 trading days
        closes = hist['Close'].iloc[-2:]
        dates = hist.index[-2:]
        
        previous_price = float(closes.iloc[0])
        current_price = float(closes.iloc[1])
        previous_date = dates[0].strftime('%Y-%m-%d')
        current_date = dates[1].strftime('%Y-%m-%d')
        
        print(f"📊 {ticker}: {previous_date} = ₹{previous_price:.2f} → {current_date} = ₹{current_price:.2f}")
        
        return current_price, previous_price, current_date, previous_date
        
    except Exception as e:
        print(f"❌ {ticker}: Error fetching data - {str(e)}")
        return None, None, None, None


def calculate_percentage_change(current, previous):
    """Calculate percentage change with proper sign handling"""
    if previous == 0:
        return 0.0
    # Don't use abs() here - preserve the sign
    return ((current - previous) / previous) * 100


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

    print(f"\n🕐 StockPulse Monitor running at {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}")
    print(f"📍 Threshold: {threshold}%\n")

    # Set Groq API key from environment variable only (for GitHub Actions security)
    groq_api_key = os.getenv('GROQ_API_KEY')
    if groq_api_key:
        groq_client = Groq(api_key=groq_api_key)
    else:
        groq_client = None
        print("⚠️ GROQ_API_KEY not found - AI analysis disabled\n")

    for ticker in tickers:
        current, previous, current_date, previous_date = get_stock_data(ticker)
        if current is None or previous is None:
            continue
        
        # Calculate with proper sign
        pct_change = calculate_percentage_change(current, previous)
        abs_pct_change = abs(pct_change)
        
        # Only alert if absolute change exceeds threshold
        if abs_pct_change >= threshold:
            sign = '+' if pct_change > 0 else ''  # Negative sign is already in the number
            alert_msg = f"{ticker}: {sign}{pct_change:.2f}% (₹{current:.2f} from ₹{previous:.2f})"
            
            # If underperforming (negative change), get Groq analysis
            if groq_client and pct_change < 0:
                groq_reason = analyze_with_groq(groq_client, ticker, pct_change)
                alert_msg += f"\n💡 {groq_reason}"
            
            alerts.append(f"\n{alert_msg}")
            print(f"✅ Alert triggered for {ticker}")
        else:
            print(f"⏭️ {ticker}: {pct_change:+.2f}% (below threshold)")

    if alerts:
        print(f"\n📲 Sending {len(alerts)} alert(s) via Pushover...")
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
    else:
        print("\n✨ No alerts triggered - all stocks within normal range")

if __name__ == "__main__":
    main()

