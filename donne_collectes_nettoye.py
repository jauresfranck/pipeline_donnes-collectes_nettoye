# -*- coding: utf-8 -*-
"""
PIPELINE COMPLET : Collecte → Nettoyage → Signaux → BD SQLAlchemy → Push auto
Intégration BD pour stocker HistoricalData, Features, Predictions
Décembre 2025 - ENSIM 4A Prédicteur Boursier
"""
import os
import pandas as pd
import numpy as np
from datetime import datetime, date
import yfinance as yf
from tiingo import TiingoClient
import subprocess
from sqlalchemy import text
# SQLAlchemy imports
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.exc import IntegrityError
# ====================== CONFIG ======================
TIINGO_API_KEY = os.getenv('TIINGO_API_KEY')
DATA_FOLDER = "data"
TIINGO_FOLDER = os.path.join(DATA_FOLDER, "tiingo")
os.makedirs(DATA_FOLDER, exist_ok=True)
os.makedirs(TIINGO_FOLDER, exist_ok=True)
# BD Config (SQLite)
engine = create_engine('sqlite:///stock_predictor.db', echo=False) # echo=True pour debug logs
Base = declarative_base()
# ====================== BD Modèles ======================
class Stock(Base):
    __tablename__ = 'stocks'
    symbol = Column(String, primary_key=True)
    name = Column(String)
    sector = Column(String)
class HistoricalData(Base):
    __tablename__ = 'historical_data'
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    symbol = Column(String, ForeignKey('stocks.symbol'), nullable=False)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Integer)
    stock = relationship("Stock")
class Feature(Base):
    __tablename__ = 'features'
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    symbol = Column(String, ForeignKey('stocks.symbol'), nullable=False)
    close_lag1 = Column(Float)
    ma_5 = Column(Float)
    volatility = Column(Float)
    rsi = Column(Float)
    macd = Column(Float)
    bb_position = Column(String)
    stock = relationship("Stock")
class Prediction(Base):
    __tablename__ = 'predictions'
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    symbol = Column(String, ForeignKey('stocks.symbol'), nullable=False)
    predicted_close = Column(Float)
    actual_close = Column(Float)
    model = Column(String)
    signal = Column(String)
    stock = relationship("Stock")
# Créer tables
Base.metadata.create_all(engine)
# Session BD
Session = sessionmaker(bind=engine)
# ====================== CLEAN DATA ======================
def clean_data(df):
    df = df.copy()
    df['date'] = pd.to_datetime(df['date']).dt.normalize()
    df = df.dropna()
    df = df[df['High'] >= df['Low']]
    df = df[df['Volume'] >= 0]
    df = df.sort_values('date').drop_duplicates('date')
    Q1, Q3 = df['Close'].quantile([0.25, 0.75])
    IQR = Q3 - Q1
    df['Close'] = df['Close'].clip(Q1 - 3*IQR, Q3 + 3*IQR)
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    return df
# ====================== COLLECTE & INSERT BD ======================
def collect_yfinance():
    session = Session()
    symbols = [{"symbol": "AAPL", "name": "Apple Inc.", "sector": "Tech"},
               {"symbol": "TSLA", "name": "Tesla Inc.", "sector": "Auto"},
               {"symbol": "MSFT", "name": "Microsoft Corp.", "sector": "Tech"},
               {"symbol": "BTC", "name": "Bitcoin USD", "sector": "Crypto"},
               {"symbol": "GOOGL", "name": "Alphabet Inc.", "sector": "Tech"}]
   
    # Init Stocks
    for s in symbols:
        stock = Stock(symbol=s['symbol'], name=s['name'], sector=s['sector'])
        session.merge(stock) # Merge pour update si existe
   
    all_data = []
    print("Collecte yfinance...")
    for s in [sym['symbol'] for sym in symbols]:
        ticker = s if s != "BTC" else "BTC-USD"
        df = yf.Ticker(ticker).history(period="1y").reset_index()
        df['symbol'] = s
        df['date'] = df['Date']
        df = df[['date', 'Open', 'High', 'Low', 'Close', 'Volume', 'symbol']]
        df = clean_data(df)
        all_data.append(df)
        df.to_csv(os.path.join(DATA_FOLDER, f"{s}.csv"), index=False)
       
        # Insert HistoricalData
        for _, row in df.iterrows():
            hist = HistoricalData(
                date=datetime.strptime(row['date'], '%Y-%m-%d').date(),
                symbol=row['symbol'],
                open=row['Open'], high=row['High'], low=row['Low'],
                close=row['Close'], volume=row['Volume']
            )
            session.merge(hist) # Merge pour éviter doublons
   
    pd.concat(all_data).to_csv(os.path.join(DATA_FOLDER, "ALL_YFINANCE.csv"), index=False)
    session.commit()
    session.close()
    print("yfinance & BD insert OK")
def collect_tiingo():
    if not TIINGO_API_KEY:
        print("TIINGO_API_KEY absente → Tiingo ignoré")
        return
    session = Session()
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
       
        # Insert HistoricalData
        for _, row in df.iterrows():
            hist = HistoricalData(
                date=datetime.strptime(row['date'], '%Y-%m-%d').date(),
                symbol=row['symbol'],
                open=row['Open'], high=row['High'], low=row['Low'],
                close=row['Close'], volume=row['Volume']
            )
            session.merge(hist)
   
    pd.concat(all_data).to_csv(os.path.join(TIINGO_FOLDER, "ALL_TIINGO.csv"), index=False)
    session.commit()
    session.close()
    print("Tiingo & BD insert OK")
# ====================== SIGNAUX, FEATURES & PREDICTIONS BD ======================
def generate_signals():
    session = Session()
    df = pd.read_csv(os.path.join(DATA_FOLDER, "ALL_YFINANCE.csv"))
    signals = []
    for symbol in df['symbol'].unique():
        data = df[df['symbol'] == symbol].copy()
        if len(data) < 60: continue
        data['date'] = pd.to_datetime(data['date'])
        data = data.sort_values('date')
       
        # Calcul features (comme avant)
        data['ma20'] = data['Close'].rolling(20).mean()
        data['std20'] = data['Close'].rolling(20).std()
        data['upper_bb'] = data['ma20'] + 2 * data['std20']
        data['lower_bb'] = data['ma20'] - 2 * data['std20']
        delta = data['Close'].diff()
        up = delta.clip(lower=0)
        down = -delta.clip(upper=0)
        roll_up = up.ewm(alpha=1/14, adjust=False).mean()
        roll_down = down.ewm(alpha=1/14, adjust=False).mean()
        rs = roll_up / roll_down
        data['rsi'] = 100 - (100 / (1 + rs))
        ema12 = data['Close'].ewm(span=12, adjust=False).mean()
        ema26 = data['Close'].ewm(span=26, adjust=False).mean()
        data['macd'] = ema12 - ema26
        data['signal_line'] = data['macd'].ewm(span=9, adjust=False).mean()
        data['close_lag1'] = data['Close'].shift(1)
        data['ma_5'] = data['Close'].rolling(5).mean()
        data['volatility'] = data['Close'].rolling(5).std()
       
        last = data.iloc[-1]
        prev = data.iloc[-2]
       
        bb_pos = 'BELOW' if last['Close'] < last['lower_bb'] else 'ABOVE' if last['Close'] > last['upper_bb'] else 'INSIDE'
        buy_count = sum([last['rsi'] < 30, last['Close'] < last['lower_bb'], last['macd'] > last['signal_line']])
        sell_count = sum([last['rsi'] > 70, last['Close'] > last['upper_bb'], last['macd'] < last['signal_line'] and prev['macd'] >= prev['signal_line']])
        signal = "BUY" if buy_count >= 2 else "SELL" if sell_count >= 2 else "HOLD"
       
        # Insert Feature
        feat = Feature(
            date=last['date'].date(),
            symbol=symbol,
            close_lag1=last['close_lag1'],
            ma_5=last['ma_5'],
            volatility=last['volatility'],
            rsi=last['rsi'],
            macd=last['macd'],
            bb_position=bb_pos
        )
        session.merge(feat)
       
        # Insert Prediction (baseline rule-based)
        pred = Prediction(
            date=last['date'].date(),
            symbol=symbol,
            predicted_close=last['Close'], # Placeholder ; remplace par ML pred
            actual_close=last['Close'],
            model='Baseline',
            signal=signal
        )
        session.merge(pred)
       
        signals.append({'symbol': symbol, 'date': last['date'].strftime('%Y-%m-%d'), 'close': round(last['Close'], 2),
                        'rsi': round(last['rsi'], 2), 'bb_position': bb_pos, 'macd_hist': round(last['macd'] - last['signal_line'], 4),
                        'recommendation': signal})
   
    signals_df = pd.DataFrame(signals)
    signals_df.to_csv(os.path.join(DATA_FOLDER, "latest_signals.csv"), index=False)
   
    hist_path = os.path.join(DATA_FOLDER, "signals_history.csv")
    if os.path.exists(hist_path):
        hist_df = pd.read_csv(hist_path)
        signals_df = pd.concat([hist_df, signals_df], ignore_index=True)
    signals_df.to_csv(hist_path, index=False)
   
    session.commit()
    session.close()
    print("\n=== SIGNAUX DU JOUR ===")
    print(signals_df.to_markdown(index=False))
# ====================== COMMIT & PUSH ======================
def commit_and_push():
    subprocess.run(["git", "config", "user.name", "GitHub Actions Bot"])
    subprocess.run(["git", "config", "user.email", "github-actions@github.com"])
    subprocess.run(["git", "add", "data/", "stock_predictor.db"])
    result = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
    if result.stdout.strip():
        subprocess.run(["git", "commit", "-m", f"Update données & BD {datetime.now().strftime('%Y-%m-%d')}"])
        subprocess.run(["git", "push"])
        print("PUSH effectué avec BD !")
    else:
        print("Aucun changement → pas de commit")
# ====================== MAIN ======================
def main():
    print("DÉBUT PIPELINE -", datetime.now().strftime("%Y-%m-%d %H:%M"))
    collect_yfinance()
    collect_tiingo()
    generate_signals()
    commit_and_push()
    print("TERMINÉ – Données stockées en BD & dans data/")
if __name__ == "__main__":
    main()
