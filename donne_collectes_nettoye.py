# -*- coding: utf-8 -*-
"""
PIPELINE COMPLET : Collecte → Nettoyage → Signaux → BD PostgreSQL Cloud → Update auto
Modifié pour SUPABASE
"""
import os
import pandas as pd
import numpy as np
from datetime import datetime, date
import yfinance as yf
from tiingo import TiingoClient
import subprocess

# SQLAlchemy imports
# AJOUT DE BigInteger car les volumes boursiers sont énormes
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, ForeignKey, BigInteger
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

# ====================== CONFIG ======================
TIINGO_API_KEY = os.getenv('TIINGO_API_KEY')

# RECUPERATION DE L'URL SUPABASE
DB_URL = os.getenv('DB_URL')
if not DB_URL:
    raise ValueError("DB_URL manquante – configure-la dans secrets GitHub !")

DATA_FOLDER = "data"
TIINGO_FOLDER = os.path.join(DATA_FOLDER, "tiingo")
os.makedirs(DATA_FOLDER, exist_ok=True)
os.makedirs(TIINGO_FOLDER, exist_ok=True)

# BD Config (PostgreSQL via Supabase)
# On utilise DB_URL au lieu de sqlite:///...
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
    # CHANGEMENT ICI : BigInteger pour éviter les erreurs de dépassement
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

# Créer tables (Supabase va créer les tables si elles n'existent pas)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# ====================== CLEAN DATA ======================
def clean_data(df):
    df = df.copy()
    df['date'] = pd.to_datetime(df['date']).dt.normalize()
    df = df.dropna()
    
    # Conversion explicite des types pour éviter les erreurs SQL
    cols_float = ['Open', 'High', 'Low', 'Close']
    for col in cols_float:
        df[col] = df[col].astype(float)
    
    if 'Volume' in df.columns:
        df['Volume'] = df['Volume'].astype('int64') # int64 pour BigInteger

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
    try:
        for s in symbols:
            stock = Stock(symbol=s['symbol'], name=s['name'], sector=s['sector'])
            session.merge(stock)
        session.commit()
    except Exception as e:
        print(f"Erreur init stocks: {e}")
        session.rollback()
   
    all_data = []
    print("Collecte yfinance...")
    
    for s in [sym['symbol'] for sym in symbols]:
        ticker = s if s != "BTC" else "BTC-USD"
        
        try:
            df = yf.Ticker(ticker).history(period="1y").reset_index()
            if df.empty:
                print(f"Pas de données pour {s}")
                continue

            df['symbol'] = s
            df['date'] = df['Date']
            df = df[['date', 'Open', 'High', 'Low', 'Close', 'Volume', 'symbol']]
            df = clean_data(df)
            all_data.append(df)
            
            # Sauvegarde CSV locale (optionnel maintenant)
            df.to_csv(os.path.join(DATA_FOLDER, f"{s}.csv"), index=False)
        
            # Insert HistoricalData
            # Note: session.merge avec un ID autoincrement risque de créer des doublons 
            # si on relance le script plusieurs fois. 
            # Pour un projet simple, on vide ou on gère ça plus tard.
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
            session.commit()
            print(f"{s} inséré en base.")
            
        except Exception as e:
            print(f"Erreur sur {s}: {e}")
            session.rollback()
   
    if all_data:
        pd.concat(all_data).to_csv(os.path.join(DATA_FOLDER, "ALL_YFINANCE.csv"), index=False)
    
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
        try:
            df = client.get_dataframe(s, frequency='daily', startDate=start_date)
            if df.empty: continue
            
            df = df.reset_index()
            df['symbol'] = s
            df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'symbol']]
            df.columns = ['date', 'Open', 'High', 'Low', 'Close', 'Volume', 'symbol']
            df = clean_data(df)
            all_data.append(df)
        
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
            session.commit()
            
        except Exception as e:
            print(f"Erreur Tiingo {s}: {e}")
            session.rollback()
   
    if all_data:
        pd.concat(all_data).to_csv(os.path.join(TIINGO_FOLDER, "ALL_TIINGO.csv"), index=False)
    
    session.close()
    print("Tiingo & BD insert OK")

# ====================== SIGNAUX, FEATURES & PREDICTIONS BD ======================
def generate_signals():
    session = Session()
    
    # On lit le CSV généré juste avant pour calculer les signaux
    csv_path = os.path.join(DATA_FOLDER, "ALL_YFINANCE.csv")
    if not os.path.exists(csv_path):
        print("Fichier ALL_YFINANCE.csv manquant")
        return

    df = pd.read_csv(csv_path)
    signals = []
    
    for symbol in df['symbol'].unique():
        try:
            data = df[df['symbol'] == symbol].copy()
            if len(data) < 60: continue
            
            data['date'] = pd.to_datetime(data['date'])
            data = data.sort_values('date')
        
            # Calcul features
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
            
            buy_cond = (last['rsi'] < 30) + (last['Close'] < last['lower_bb']) + (last['macd'] > last['signal_line'])
            sell_cond = (last['rsi'] > 70) + (last['Close'] > last['upper_bb']) + (last['macd'] < last['signal_line'] and prev['macd'] >= prev['signal_line'])
            
            signal = "BUY" if buy_cond >= 2 else "SELL" if sell_cond >= 2 else "HOLD"
        
            # Helpers pour gérer les NaN avant insertion
            def safe_float(val):
                return float(val) if pd.notna(val) else 0.0

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
        
            pred = Prediction(
                date=last['date'].date(),
                symbol=str(symbol),
                predicted_close=safe_float(last['Close']),
                actual_close=safe_float(last['Close']),
                model='Baseline',
                signal=str(signal)
            )
            session.merge(pred)
        
            signals.append({
                'symbol': symbol, 
                'date': last['date'].strftime('%Y-%m-%d'), 
                'close': round(last['Close'], 2),
                'rsi': round(last['rsi'], 2), 
                'recommendation': signal
            })
            session.commit()
            
        except Exception as e:
            print(f"Erreur signaux {symbol}: {e}")
            session.rollback()
   
    if signals:
        signals_df = pd.DataFrame(signals)
        signals_df.to_csv(os.path.join(DATA_FOLDER, "latest_signals.csv"), index=False)
        print("\n=== SIGNAUX DU JOUR ===")
        print(signals_df.to_markdown(index=False))
        
    session.close()

# ====================== COMMIT & PUSH ======================
def commit_and_push():
    # On ne push plus la DB locale car on utilise Supabase
    # On push uniquement les CSV (data/) pour garder une trace, mais c'est optionnel
    subprocess.run(["git", "config", "user.name", "GitHub Actions Bot"])
    subprocess.run(["git", "config", "user.email", "github-actions@github.com"])
    
    # On ajoute uniquement le dossier data
    subprocess.run(["git", "add", "data/"])
    
    result = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
    if result.stdout.strip():
        subprocess.run(["git", "commit", "-m", f"Auto-update data {datetime.now().strftime('%Y-%m-%d')}"])
        subprocess.run(["git", "push"])
        print("✅ Données CSV mises à jour sur GitHub (DB sur Supabase).")
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
