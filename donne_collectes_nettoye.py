# -*- coding: utf-8 -*-
"""
PIPELINE COMPLET : Collecte → Nettoyage → Signaux → BD PostgreSQL Cloud → Update auto
CORRECTIF : Commit par symbole pour éviter que AAPL bloque les autres.
"""
import os
import time
import pandas as pd
import numpy as np
from datetime import datetime, date
import yfinance as yf
from tiingo import TiingoClient
import subprocess

# SQLAlchemy imports
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, ForeignKey, BigInteger, text
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

# ====================== CONFIG ======================
TIINGO_API_KEY = os.getenv('TIINGO_API_KEY')
DB_URL = os.getenv('DB_URL')
if not DB_URL:
    raise ValueError("DB_URL manquante – configure-la dans secrets GitHub !")

DATA_FOLDER = "data"
TIINGO_FOLDER = os.path.join(DATA_FOLDER, "tiingo")
os.makedirs(DATA_FOLDER, exist_ok=True)
os.makedirs(TIINGO_FOLDER, exist_ok=True)

# BD Config
engine = create_engine(DB_URL, echo=False)
Base = declarative_base()

# ====================== BD Modèles ======================
class Stock(Base):
    __tablename__ = 'stocks'
    symbol = Column(String, primary_key=True)
    name = Column(String)
    sector = Column(String)

class HistoricalData(Base):
    __tablename__ = 'historical_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    symbol = Column(String, ForeignKey('stocks.symbol'), nullable=False)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(BigInteger)
    stock = relationship("Stock")

class Feature(Base):
    __tablename__ = 'features'
    id = Column(Integer, primary_key=True, autoincrement=True)
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
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    symbol = Column(String, ForeignKey('stocks.symbol'), nullable=False)
    predicted_close = Column(Float)
    actual_close = Column(Float)
    model = Column(String)
    signal = Column(String)
    stock = relationship("Stock")

# Création tables
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# ====================== CLEAN DATA ======================
def clean_data(df):
    df = df.copy()
    df['date'] = pd.to_datetime(df['date']).dt.normalize()
    df = df.dropna()
    
    # Conversion types
    cols_float = ['Open', 'High', 'Low', 'Close']
    for col in cols_float:
        df[col] = df[col].astype(float)
    
    if 'Volume' in df.columns:
        df['Volume'] = df['Volume'].astype('int64')

    df = df[df['High'] >= df['Low']]
    df = df[df['Volume'] >= 0]
    df = df.sort_values('date').drop_duplicates('date')
    
    # Clip Outliers
    Q1, Q3 = df['Close'].quantile([0.25, 0.75])
    IQR = Q3 - Q1
    df['Close'] = df['Close'].clip(Q1 - 3*IQR, Q3 + 3*IQR)
    
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    return df

# ====================== COLLECTE YFINANCE ======================
def collect_yfinance():
    session = Session()
    print("\n--- DÉBUT COLLECTE YFINANCE ---")
    
    symbols = [{"symbol": "AAPL", "name": "Apple Inc.", "sector": "Tech"},
               {"symbol": "TSLA", "name": "Tesla Inc.", "sector": "Auto"},
               {"symbol": "MSFT", "name": "Microsoft Corp.", "sector": "Tech"},
               {"symbol": "BTC", "name": "Bitcoin USD", "sector": "Crypto"},
               {"symbol": "GOOGL", "name": "Alphabet Inc.", "sector": "Tech"}]
    
    # 1. Init Stocks (Table de référence)
    try:
        for s in symbols:
            stock = Stock(symbol=s['symbol'], name=s['name'], sector=s['sector'])
            session.merge(stock)
        session.commit()
    except Exception as e:
        print(f"⚠️ Erreur Init Stocks: {e}")
        session.rollback()

    all_data = []

    # 2. Boucle Tickers
    for s in symbols:
        sym_str = s['symbol']
        print(f"📡 Récupération {sym_str}...")
        
        try:
            # Pause anti-ban (important pour yfinance en boucle)
            time.sleep(1) 
            
            ticker = sym_str if sym_str != "BTC" else "BTC-USD"
            df = yf.Ticker(ticker).history(period="1y").reset_index()
            
            if df.empty:
                print(f"❌ Aucune donnée reçue pour {sym_str}")
                continue
                
            df['symbol'] = sym_str
            df['date'] = df['Date']
            df = df[['date', 'Open', 'High', 'Low', 'Close', 'Volume', 'symbol']]
            df = clean_data(df)
            
            all_data.append(df)
            df.to_csv(os.path.join(DATA_FOLDER, f"{sym_str}.csv"), index=False)
            
            # Insertion BD
            for _, row in df.iterrows():
                hist = HistoricalData(
                    date=datetime.strptime(row['date'], '%Y-%m-%d').date(),
                    symbol=str(row['symbol']),
                    open=float(row['Open']),
                    high=float(row['High']),
                    low=float(row['Low']),
                    close=float(row['Close']),
                    volume=int(row['Volume'])
                )
                session.merge(hist)
            
            # 🔥 COMMIT À CHAQUE SYMBOLE 🔥
            session.commit()
            print(f"✅ {sym_str} : Sauvegardé en BD.")

        except Exception as e:
            print(f"❌ CRASH sur {sym_str}: {e}")
            session.rollback() # Annule l'erreur pour passer au suivant propre

    # Sauvegarde CSV global
    if all_data:
        pd.concat(all_data).to_csv(os.path.join(DATA_FOLDER, "ALL_YFINANCE.csv"), index=False)
    
    session.close()
    print("--- FIN YFINANCE ---\n")

# ====================== COLLECTE TIINGO ======================
def collect_tiingo():
    if not TIINGO_API_KEY:
        print("Info: TIINGO_API_KEY absente.")
        return
        
    session = Session()
    print("--- DÉBUT COLLECTE TIINGO ---")
    
    client = TiingoClient({'api_key': TIINGO_API_KEY, 'session': True})
    symbols = ["AAPL", "TSLA", "MSFT", "GOOGL"]
    start_date = datetime.now().replace(year=datetime.now().year - 1)
    
    for s in symbols:
        try:
            print(f"📡 Tiingo: {s}...")
            df = client.get_dataframe(s, frequency='daily', startDate=start_date)
            if df.empty: continue
            
            df = df.reset_index()
            df['symbol'] = s
            df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'symbol']]
            df.columns = ['date', 'Open', 'High', 'Low', 'Close', 'Volume', 'symbol']
            df = clean_data(df)
            
            # Insert BD
            for _, row in df.iterrows():
                hist = HistoricalData(
                    date=datetime.strptime(row['date'], '%Y-%m-%d').date(),
                    symbol=str(row['symbol']),
                    open=float(row['Open']),
                    high=float(row['High']),
                    low=float(row['Low']),
                    close=float(row['Close']),
                    volume=int(row['Volume'])
                )
                session.merge(hist)
            
            session.commit() # Commit par symbole
            print(f"✅ {s} (Tiingo) : Sauvegardé.")
            
        except Exception as e:
            print(f"⚠️ Erreur Tiingo {s}: {e}")
            session.rollback()

    session.close()
    print("--- FIN TIINGO ---\n")

# ====================== SIGNAUX ======================
def generate_signals():
    session = Session()
    print("--- GÉNÉRATION DES SIGNAUX ---")
    
    try:
        path = os.path.join(DATA_FOLDER, "ALL_YFINANCE.csv")
        if not os.path.exists(path):
            print("❌ Fichier ALL_YFINANCE.csv manquant.")
            return

        df = pd.read_csv(path)
        signals = []
        
        # Helper pour float safe
        def safe_float(val):
            return float(val) if pd.notna(val) else 0.0
        
        for symbol in df['symbol'].unique():
            try:
                data = df[df['symbol'] == symbol].copy()
                if len(data) < 60: 
                    print(f"Info: Pas assez de données pour {symbol}")
                    continue
                    
                data['date'] = pd.to_datetime(data['date'])
                data = data.sort_values('date')
                
                # Calculs Techniques
                data['ma20'] = data['Close'].rolling(20).mean()
                data['std20'] = data['Close'].rolling(20).std()
                data['upper_bb'] = data['ma20'] + 2 * data['std20']
                data['lower_bb'] = data['ma20'] - 2 * data['std20']
                
                delta = data['Close'].diff()
                up = delta.clip(lower=0)
                down = -delta.clip(upper=0)
                rs = up.ewm(alpha=1/14).mean() / down.ewm(alpha=1/14).mean()
                data['rsi'] = 100 - (100 / (1 + rs))
                
                data['macd'] = data['Close'].ewm(span=12).mean() - data['Close'].ewm(span=26).mean()
                data['signal_line'] = data['macd'].ewm(span=9).mean()
                
                data['close_lag1'] = data['Close'].shift(1)
                data['ma_5'] = data['Close'].rolling(5).mean()
                data['volatility'] = data['Close'].rolling(5).std()
                
                last = data.iloc[-1]
                prev = data.iloc[-2]
                
                # Signal
                bb_pos = 'BELOW' if last['Close'] < last['lower_bb'] else 'ABOVE' if last['Close'] > last['upper_bb'] else 'INSIDE'
                
                buy_cond = (last['rsi'] < 30) or (last['Close'] < last['lower_bb']) or (last['macd'] > last['signal_line'])
                sell_cond = (last['rsi'] > 70) or (last['Close'] > last['upper_bb']) or (last['macd'] < last['signal_line'])
                
                signal = "BUY" if buy_cond else "SELL" if sell_cond else "HOLD"
                
                # Insertion Feature
                feat = Feature(
                    date=last['date'].date(),
                    symbol=str(symbol),
                    close_lag1=safe_float(last['close_lag1']),
                    ma_5=safe_float(last['ma_5']),
                    volatility=safe_float(last['volatility']),
                    rsi=safe_float(last['rsi']),
                    macd=safe_float(last['macd']),
                    bb_position=str(bb_pos)
                )
                session.merge(feat)
                
                # Insertion Prediction
                pred = Prediction(
                    date=last['date'].date(),
                    symbol=str(symbol),
                    predicted_close=safe_float(last['Close']),
                    actual_close=safe_float(last['Close']),
                    model='Baseline',
                    signal=str(signal)
                )
                session.merge(pred)
                
                # Commit par signal
                session.commit()
                
                signals.append({
                    'symbol': symbol, 
                    'date': last['date'].strftime('%Y-%m-%d'), 
                    'close': round(safe_float(last['Close']), 2),
                    'recommendation': signal
                })
                
            except Exception as e:
                print(f"❌ Erreur calcul signal {symbol}: {e}")
                session.rollback()

        # CSV Export
        if signals:
            signals_df = pd.DataFrame(signals)
            hist_path = os.path.join(DATA_FOLDER, "signals_history.csv")
            
            if os.path.exists(hist_path):
                hist_df = pd.read_csv(hist_path)
                signals_df = pd.concat([hist_df, signals_df], ignore_index=True)
                signals_df = signals_df.drop_duplicates(subset=['date', 'symbol'], keep='last')
            
            signals_df.to_csv(hist_path, index=False)
            print("\n=== SIGNAUX DU JOUR ===")
            print(signals_df.to_markdown(index=False))

    except Exception as e:
        print(f"Erreur globale Signals: {e}")
    finally:
        session.close()

# ====================== GIT PUSH ======================
def commit_and_push():
    subprocess.run(["git", "config", "user.name", "GitHub Actions Bot"])
    subprocess.run(["git", "config", "user.email", "github-actions@github.com"])
    subprocess.run(["git", "add", "data/"])
    res = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
    if res.stdout.strip():
        subprocess.run(["git", "commit", "-m", f"Auto-update {datetime.now().strftime('%Y-%m-%d')}"])
        subprocess.run(["git", "push"])
        print("✅ Données CSV pushées sur GitHub.")
    else:
        print("Info: Pas de changements CSV à pusher.")

# ====================== MAIN ======================
def main():
    print(f"🚀 DÉBUT PIPELINE - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    collect_yfinance()
    collect_tiingo()
    generate_signals()
    commit_and_push()
    print("🏁 PIPELINE TERMINÉ")

if __name__ == "__main__":
    main()
