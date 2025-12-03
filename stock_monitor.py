import configparser
from pushover_handler import send_pushover_notification
import yfinance as yf
import sys


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

    for ticker in tickers:
        current, previous = get_stock_data(ticker)
        if current is None or previous is None:
            continue
        pct_change = calculate_percentage_change(current, previous)
        if pct_change >= threshold:
            sign = '+' if current > previous else '-'
            alerts.append(f"{ticker}: {sign}{pct_change:.2f}% ({current:.2f} from {previous:.2f})")

    if alerts:
        title = "StockPulse ⚡ High Volatility Alert"
        message = '\n'.join(alerts)
        send_pushover_notification(title, message, config)

if __name__ == "__main__":
    main()
