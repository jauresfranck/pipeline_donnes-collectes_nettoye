# -*- coding: utf-8 -*-
"""
PIPELINE PRIX → CSV dans le dossier data/
À utiliser avec GitHub Actions.
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

    # 1) on force la colonne date en datetime
    df['date'] = pd.to_datetime(df['date'])

    # 2) on enlève l'heure (on garde juste la date)
    df['date'] = df['date'].dt.normalize()  # équivalent à mettre 00:00:00

    # 3) nettoyage classique
    df = df.dropna()
    df = df[df['High'] >= df['Low']]
    df = df[df['Volume'] >= 0]
    df = df.sort_values('date').drop_duplicates('date')

    # 4) gestion des outliers sur Close (clip 3*IQR)
    Q1, Q3 = df['Close'].quantile([0.25, 0.75])
    IQR = Q3 - Q1
    df['Close'] = np.clip(df['Close'], Q1 - 3 * IQR, Q3 + 3 * IQR)

    # 5) format final des dates pour les CSV : YYYY-MM-DD
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
        df['date'] = df['Date']  # on laisse en datetime, clean_data s'occupe du format
        df = df[['date', 'Open', 'High', 'Low', 'Close', 'Volume', 'symbol']]
        df = clean_data(df)
        all_data.append(df)

        df.to_csv(os.path.join(DATA_FOLDER, f"{s}.csv"), index=False)

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
        df = df.reset_index()  # 'date' devient une colonne datetime
        df['symbol'] = s
        df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'symbol']]
        df.columns = ['date', 'Open', 'High', 'Low', 'Close', 'Volume', 'symbol']
        df = clean_data(df)
        all_data.append(df)

    pd.concat(all_data).to_csv(os.path.join(TIINGO_FOLDER, "ALL_TIINGO.csv"), index=False)
    print("Tiingo OK")

# ====================== MAIN ======================

def main():
    print("DÉBUT PIPELINE -", datetime.now().strftime("%Y-%m-%d %H:%M"))
    collect_yfinance()
    collect_tiingo()
    print("TERMINÉ – PRIX DANS data/")

if __name__ == "__main__":
    main()
