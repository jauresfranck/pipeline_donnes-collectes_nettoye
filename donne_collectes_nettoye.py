# -*- coding: utf-8 -*-
"""
PIPELINE PRIX → CSV + SIGNAUX dans data/
GitHub Actions compatible ✅
"""
import os
import pandas as pd
import numpy as np
from datetime import datetime
import yfinance as yf
from tiingo import TiingoClient

# ====================== CONFIG ======================
TIINGO_API_KEY = os.getenv('TIINGO_API_KEY')
if not TIINGO_API_KEY:
    raise ValueError("TIINGO_API_KEY manquante ! (définis-la dans les secrets GitHub)")

DATA_FOLDER = "data"
TIINGO_FOLDER = os.path.join(DATA_FOLDER, "tiingo")
os.makedirs(DATA_FOLDER, exist_ok=True)
os.makedirs(TIINGO_FOLDER, exist_ok=True)

# ====================== CLEAN DATA ======================
def clean_data(df):
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df['date'] = df['date'].dt.normalize()
    df = df.dropna()
    df = df[df['High'] >= df['Low']]
    df = df[df['Volume'] >= 0]
    df = df.sort_values('date').drop_duplicates('date')
    Q1, Q3 = df['Close'].quantile([0.25, 0.75])
    IQR = Q3 - Q1
    df['Close'] = np.clip(df['Close'], Q1 - 3 * IQR, Q3 + 3 * IQR)
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    return df

# ====================== YFINANCE ======================
def collect_yfinance():
    symbols = ["AAPL", "TSLA", "MSFT", "BTC-USD", "GOOGL"]
    all_data = []
    print("Collecte yfinance...")
    for s in symbols:
        df = yf.Ticker(s).history(period="1y").reset_index()
        df['symbol'] = s
        df['date'] = df['Date']
        df = df[['date', 'Open', 'High', 'Low', 'Close', 'Volume', 'symbol']]
        df = clean_data(df)
        all_data.append(df)
        df.to_csv(os.path.join(DATA_FOLDER, f"{s.replace('-USD', '')}.csv"), index=False)  # BTC-USD → BTC.csv
    pd.concat(all_data).to_csv(os.path.join(DATA_FOLDER, "ALL_YFINANCE.csv"), index=False)
    print("yfinance OK")

# ====================== TIINGO ======================
def collect_tiingo():
    client = TiingoClient({'api_key': TIINGO_API_KEY, 'session': True})
    symbols = ["AAPL", "TSLA", "MSFT", "GOOGL"]
    all_data = []
    print("Collecte Tiingo...")
    start_date = datetime.now().replace(year=datetime.now().year - 1)
    for s in symbols:
        df = client.get_dataframe(s, frequency='daily', startDate=start_date)
        df = df.reset_index()
        df['symbol'] = s
        df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'symbol']]
        df.columns = ['date', 'Open', 'High', 'Low', 'Close', 'Volume', 'symbol']
        df = clean_data(df)
        all_data.append(df)
    pd.concat(all_data).to_csv(os.path.join(TIINGO_FOLDER, "ALL_TIINGO.csv"), index=False)
    print("Tiingo OK")

# ====================== SIGNAUX TECHNIQUES ======================
def add_indicators(df):
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    # Bollinger Bands (20)
    df['ma20'] = df['Close'].rolling(20).mean()
    df['std20'] = df['Close'].rolling(20).std()
    df['upper_bb'] = df['ma20'] + 2 * df['std20']
    df['lower_bb'] = df['ma20'] - 2 * df['std20']

    # RSI 14
    delta = df['Close'].diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    roll_up = up.ewm(alpha=1/14, adjust=False).mean()
    roll_down = down.ewm(alpha=1/14, adjust=False).mean()
    rs = roll_up / roll_down
    df['rsi'] = 100 - (100 / (1 + rs))

    # MACD
    ema12 = df['Close'].ewm(span=12, adjust=False).mean()
    ema26 = df['Close'].ewm(span=26, adjust=False).mean()
    df['macd'] = ema12 - ema26
    df['signal_line'] = df['macd'].ewm(span=9, adjust=False).mean()

    return df

def generate_signals():
    all_df = pd.read_csv(os.path.join(DATA_FOLDER, "ALL_YFINANCE.csv"))
    signals = []

    for symbol in all_df['symbol'].unique():
        df = all_df[all_df['symbol'] == symbol].copy()
        if len(df) < 60:
            continue

        df = add_indicators(df)
        last = df.iloc[-1]
        prev = df.iloc[-2]

        buy_count = sum([
            last['rsi'] < 30,
            last['Close'] < last['lower_bb'],
            (last['macd'] > last['signal_line']) or (last['macd'] > last['signal_line'] and prev['macd'] <= prev['signal_line'])
        ])
        sell_count = sum([
            last['rsi'] > 70,
            last['Close'] > last['upper_bb'],
            last['macd'] < last['signal_line'] and prev['macd'] >= prev['signal_line']
        ])

        signal = "BUY" if buy_count >= 2 else "SELL" if sell_count >= 2 else "HOLD"

        signals.append({
            'symbol': symbol,
            'date': last['date'].strftime('%Y-%m-%d'),
            'close': round(last['Close'], 2),
            'rsi': round(last['rsi'], 2),
            'bb_position': 'BELOW' if last['Close'] < last['lower_bb'] else 'ABOVE' if last['Close'] > last['upper_bb'] else 'INSIDE',
            'macd_hist': round(last['macd'] - last['signal_line'], 4),
            'recommendation': signal
        })

    signals_df = pd.DataFrame(signals)
    
    # Sauvegarde quotidienne + historique
    signals_df.to_csv(os.path.join(DATA_FOLDER, "latest_signals.csv"), index=False)
    
    history_path = os.path.join(DATA_FOLDER, "signals_history.csv")
    if os.path.exists(history_path):
        history_df = pd.read_csv(history_path)
        signals_df = pd.concat([history_df, signals_df], ignore_index=True)
    signals_df.to_csv(history_path, index=False)
    
    print("\n=== SIGNAUX DU JOUR (20 novembre 2025) ===")
    print(signals_df.to_markdown(index=False))

# ====================== MAIN ======================
def main():
    print("DÉBUT PIPELINE -", datetime.now().strftime("%Y-%m-%d %H:%M"))
    collect_yfinance()
    collect_tiingo()
    generate_signals()
    print("TERMINÉ – PRIX + SIGNAUX dans data/")

if __name__ == "__main__":
    main()
