# -*- coding: utf-8 -*-
"""
PIPELINE COMPLET : Collecte -> Nettoyage -> Features historiques -> BD PostgreSQL Cloud/Supabase -> Update auto
Version modifiée : collecte 5 ans + features générées pour toutes les dates + suppression des doublons avant insertion.
"""

import os
import time
import subprocess
from datetime import datetime

import numpy as np
import pandas as pd
import yfinance as yf
from tiingo import TiingoClient

from sqlalchemy import create_engine, Column, Integer, String, Float, Date, ForeignKey, BigInteger
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

# ====================== CONFIG ======================
TIINGO_API_KEY = os.getenv("TIINGO_API_KEY")
DB_URL = os.getenv("DB_URL")

if not DB_URL:
    print("⚠️ Attention : DB_URL non trouvée. Mets-la dans les variables d'environnement.")

DATA_FOLDER = "data"
TIINGO_FOLDER = os.path.join(DATA_FOLDER, "tiingo")
os.makedirs(DATA_FOLDER, exist_ok=True)
os.makedirs(TIINGO_FOLDER, exist_ok=True)

YEARS_BACK = 5
YF_PERIOD = f"{YEARS_BACK}y"

try:
    engine = create_engine(DB_URL, echo=False) if DB_URL else None
    Base = declarative_base()
except Exception as e:
    print(f"Erreur Config DB: {e}")
    engine = None
    Base = declarative_base()

# ====================== BD Modèles ======================
class Stock(Base):
    __tablename__ = "stocks"
    symbol = Column(String, primary_key=True)
    name = Column(String)
    sector = Column(String)

class HistoricalData(Base):
    __tablename__ = "historical_data"
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    symbol = Column(String, ForeignKey("stocks.symbol"), nullable=False)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(BigInteger)
    stock = relationship("Stock")

class Feature(Base):
    __tablename__ = "features"
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    symbol = Column(String, ForeignKey("stocks.symbol"), nullable=False)
    close_lag1 = Column(Float)
    ma_5 = Column(Float)
    volatility = Column(Float)
    rsi = Column(Float)
    macd = Column(Float)
    bb_position = Column(String)
    stock = relationship("Stock")

class Prediction(Base):
    __tablename__ = "predictions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    symbol = Column(String, ForeignKey("stocks.symbol"), nullable=False)
    predicted_close = Column(Float)
    actual_close = Column(Float)
    model = Column(String)
    signal = Column(String)
    stock = relationship("Stock")

if engine:
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
else:
    Session = None

# ====================== ACTIFS ======================
SYMBOLS = [
    {"symbol": "AAPL", "name": "Apple Inc.", "sector": "Technology"},
    {"symbol": "MSFT", "name": "Microsoft Corp.", "sector": "Technology"},
    {"symbol": "GOOGL", "name": "Alphabet Inc.", "sector": "Communication Services"},
    {"symbol": "AMZN", "name": "Amazon.com Inc.", "sector": "Consumer Cyclical"},
    {"symbol": "META", "name": "Meta Platforms", "sector": "Communication Services"},
    {"symbol": "NVDA", "name": "NVIDIA Corp.", "sector": "Technology"},
    {"symbol": "TSLA", "name": "Tesla Inc.", "sector": "Consumer Cyclical"},
    {"symbol": "INTC", "name": "Intel Corp.", "sector": "Technology"},
    {"symbol": "AMD", "name": "Advanced Micro Devices", "sector": "Technology"},
    {"symbol": "IBM", "name": "IBM", "sector": "Technology"},
    {"symbol": "ORCL", "name": "Oracle Corp.", "sector": "Technology"},
    {"symbol": "NFLX", "name": "Netflix Inc.", "sector": "Communication Services"},
    {"symbol": "PLTR", "name": "Palantir Technologies", "sector": "Technology"},
    {"symbol": "SNOW", "name": "Snowflake Inc.", "sector": "Technology"},
    {"symbol": "SHOP", "name": "Shopify Inc.", "sector": "Technology"},
    {"symbol": "COIN", "name": "Coinbase Global", "sector": "Financial Services"},
    {"symbol": "ROKU", "name": "Roku Inc.", "sector": "Communication Services"},
    {"symbol": "U", "name": "Unity Software", "sector": "Technology"},
    {"symbol": "CRWD", "name": "CrowdStrike Holdings", "sector": "Technology"},
    {"symbol": "ZS", "name": "Zscaler Inc.", "sector": "Technology"},
    {"symbol": "RIVN", "name": "Rivian Automotive", "sector": "Consumer Cyclical"},
    {"symbol": "LCID", "name": "Lucid Group", "sector": "Consumer Cyclical"},
    {"symbol": "BTC-USD", "name": "Bitcoin USD", "sector": "Crypto"},
]

# ====================== CLEAN DATA ======================
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None).dt.normalize()

    required = ["date", "Open", "High", "Low", "Close", "Volume", "symbol"]
    df = df[[c for c in required if c in df.columns]].copy()
    df = df.dropna(subset=["date", "Open", "High", "Low", "Close", "Volume", "symbol"])

    for col in ["Open", "High", "Low", "Close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0).astype("int64")

    df = df.dropna(subset=["Open", "High", "Low", "Close"])
    df = df[df["High"] >= df["Low"]]
    df = df[df["Volume"] >= 0]
    df = df.sort_values("date").drop_duplicates("date")

    # Winsorisation légère du close, pour éviter quelques outliers extrêmes
    if len(df) > 20:
        q1, q3 = df["Close"].quantile([0.25, 0.75])
        iqr = q3 - q1
        if pd.notna(iqr) and iqr > 0:
            df["Close"] = df["Close"].clip(q1 - 3 * iqr, q3 + 3 * iqr)

    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    return df.reset_index(drop=True)


def safe_float(val, default=0.0):
    return float(val) if pd.notna(val) and np.isfinite(val) else default


def init_stocks(session):
    for s in SYMBOLS:
        session.merge(Stock(symbol=s["symbol"], name=s["name"], sector=s["sector"]))
    session.commit()

# ====================== COLLECTE YFINANCE ======================
def collect_yfinance():
    if not engine:
        return

    session = Session()
    print(f"\n--- DÉBUT COLLECTE YFINANCE ({YEARS_BACK} ANS, {len(SYMBOLS)} ACTIFS) ---")

    try:
        init_stocks(session)
    except Exception as e:
        print(f"⚠️ Erreur init stocks: {e}")
        session.rollback()

    all_data = []

    for s in SYMBOLS:
        sym_str = s["symbol"]
        print(f"📡 Récupération {sym_str} sur {YEARS_BACK} ans...")

        try:
            time.sleep(1)
            raw = yf.Ticker(sym_str).history(period=YF_PERIOD, interval="1d", auto_adjust=False).reset_index()

            if raw.empty:
                print(f"⚠️ Aucun résultat pour {sym_str}")
                continue

            raw["symbol"] = sym_str
            raw["date"] = raw["Date"]

            if not all(col in raw.columns for col in ["Open", "High", "Low", "Close", "Volume"]):
                print(f"⚠️ Colonnes OHLCV manquantes pour {sym_str}")
                continue

            df = raw[["date", "Open", "High", "Low", "Close", "Volume", "symbol"]]
            df = clean_data(df)
            all_data.append(df)
            df.to_csv(os.path.join(DATA_FOLDER, f"{sym_str}.csv"), index=False)

            # Supprime les anciennes lignes du symbole pour éviter les doublons.
            session.query(HistoricalData).filter(HistoricalData.symbol == sym_str).delete(synchronize_session=False)
            session.commit()

            objects = []
            for _, row in df.iterrows():
                objects.append(HistoricalData(
                    date=datetime.strptime(row["date"], "%Y-%m-%d").date(),
                    symbol=str(row["symbol"]),
                    open=safe_float(row["Open"]),
                    high=safe_float(row["High"]),
                    low=safe_float(row["Low"]),
                    close=safe_float(row["Close"]),
                    volume=int(row["Volume"]),
                ))
            session.bulk_save_objects(objects)
            session.commit()
            print(f"✅ {sym_str} : {len(df)} jours sauvegardés.")

        except Exception as e:
            print(f"❌ Erreur sur {sym_str}: {e}")
            session.rollback()

    if all_data:
        all_df = pd.concat(all_data, ignore_index=True)
        all_df.to_csv(os.path.join(DATA_FOLDER, "ALL_YFINANCE.csv"), index=False)
        print(f"✅ ALL_YFINANCE.csv sauvegardé : {len(all_df)} lignes")

    session.close()
    print("--- FIN YFINANCE ---\n")

# ====================== COLLECTE TIINGO ======================
def collect_tiingo():
    if not TIINGO_API_KEY:
        print("Info: TIINGO_API_KEY absente.")
        return
    if not engine:
        return

    session = Session()
    client = TiingoClient({"api_key": TIINGO_API_KEY, "session": True})
    symbols = ["AAPL", "TSLA", "MSFT", "GOOGL"]
    all_data = []
    print(f"--- DÉBUT COLLECTE TIINGO ({YEARS_BACK} ANS) ---")
    start_date = datetime.now().replace(year=datetime.now().year - YEARS_BACK)

    for sym in symbols:
        try:
            df = client.get_dataframe(sym, frequency="daily", startDate=start_date)
            if df.empty:
                continue

            df = df.reset_index()
            df["symbol"] = sym
            df = df[["date", "open", "high", "low", "close", "volume", "symbol"]]
            df.columns = ["date", "Open", "High", "Low", "Close", "Volume", "symbol"]
            df = clean_data(df)
            all_data.append(df)
            df.to_csv(os.path.join(TIINGO_FOLDER, f"{sym}.csv"), index=False)

            # Tiingo écrase aussi l'historique du symbole si activé.
            session.query(HistoricalData).filter(HistoricalData.symbol == sym).delete(synchronize_session=False)
            session.commit()

            objects = []
            for _, row in df.iterrows():
                objects.append(HistoricalData(
                    date=datetime.strptime(row["date"], "%Y-%m-%d").date(),
                    symbol=str(row["symbol"]),
                    open=safe_float(row["Open"]),
                    high=safe_float(row["High"]),
                    low=safe_float(row["Low"]),
                    close=safe_float(row["Close"]),
                    volume=int(row["Volume"]),
                ))
            session.bulk_save_objects(objects)
            session.commit()
            print(f"✅ {sym} (Tiingo) : {len(df)} jours sauvegardés.")

        except Exception as e:
            print(f"⚠️ Erreur Tiingo {sym}: {e}")
            session.rollback()

    if all_data:
        pd.concat(all_data, ignore_index=True).to_csv(os.path.join(TIINGO_FOLDER, "ALL_TIINGO.csv"), index=False)

    session.close()
    print("--- FIN TIINGO ---\n")

# ====================== FEATURES + SIGNAUX ======================
def compute_features_for_symbol(data: pd.DataFrame) -> pd.DataFrame:
    data = data.copy()
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data = data.sort_values("date").drop_duplicates("date")

    data["ma20"] = data["Close"].rolling(20).mean()
    data["std20"] = data["Close"].rolling(20).std()
    data["upper_bb"] = data["ma20"] + 2 * data["std20"]
    data["lower_bb"] = data["ma20"] - 2 * data["std20"]

    delta = data["Close"].diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    rs = up.ewm(alpha=1 / 14, adjust=False).mean() / down.ewm(alpha=1 / 14, adjust=False).mean()
    data["rsi"] = 100 - (100 / (1 + rs))

    data["macd"] = data["Close"].ewm(span=12, adjust=False).mean() - data["Close"].ewm(span=26, adjust=False).mean()
    data["signal_line"] = data["macd"].ewm(span=9, adjust=False).mean()

    data["close_lag1"] = data["Close"].shift(1)
    data["ma_5"] = data["Close"].rolling(5).mean()
    data["volatility"] = data["Close"].rolling(5).std()

    data["bb_position"] = "INSIDE"
    data.loc[data["Close"] < data["lower_bb"], "bb_position"] = "BELOW"
    data.loc[data["Close"] > data["upper_bb"], "bb_position"] = "ABOVE"

    data["buy_cond"] = (data["rsi"] < 30) | (data["Close"] < data["lower_bb"]) | (data["macd"] > data["signal_line"])
    data["sell_cond"] = (data["rsi"] > 70) | (data["Close"] > data["upper_bb"]) | (data["macd"] < data["signal_line"])
    data["recommendation"] = np.where(data["buy_cond"], "BUY", np.where(data["sell_cond"], "SELL", "HOLD"))

    return data


def generate_signals():
    if not engine:
        return

    session = Session()
    print("--- GÉNÉRATION DES FEATURES HISTORIQUES + SIGNAUX ---")

    csv_path = os.path.join(DATA_FOLDER, "ALL_YFINANCE.csv")
    if not os.path.exists(csv_path):
        print("❌ Fichier ALL_YFINANCE.csv manquant.")
        return

    df = pd.read_csv(csv_path)
    latest_signals = []
    all_features_csv = []

    for symbol in df["symbol"].unique():
        try:
            data = df[df["symbol"] == symbol].copy()
            if len(data) < 60:
                print(f"⚠️ Pas assez de données pour {symbol}: {len(data)}")
                continue

            data = compute_features_for_symbol(data)
            feature_rows = data.dropna(subset=["close_lag1", "ma_5", "volatility", "rsi", "macd"]).copy()
            all_features_csv.append(feature_rows)

            # Supprime les anciennes features du symbole pour éviter les doublons.
            session.query(Feature).filter(Feature.symbol == str(symbol)).delete(synchronize_session=False)
            session.commit()

            objects = []
            for _, row in feature_rows.iterrows():
                objects.append(Feature(
                    date=row["date"].date(),
                    symbol=str(symbol),
                    close_lag1=safe_float(row["close_lag1"]),
                    ma_5=safe_float(row["ma_5"]),
                    volatility=safe_float(row["volatility"]),
                    rsi=safe_float(row["rsi"]),
                    macd=safe_float(row["macd"]),
                    bb_position=str(row["bb_position"]),
                ))
            session.bulk_save_objects(objects)
            session.commit()

            last = feature_rows.iloc[-1]
            # Ne garde qu'une prédiction baseline récente par symbole.
            session.query(Prediction).filter(Prediction.symbol == str(symbol)).delete(synchronize_session=False)
            session.add(Prediction(
                date=last["date"].date(),
                symbol=str(symbol),
                predicted_close=safe_float(last["Close"]),
                actual_close=safe_float(last["Close"]),
                model="Baseline",
                signal=str(last["recommendation"]),
            ))
            session.commit()

            latest_signals.append({
                "symbol": symbol,
                "date": last["date"].strftime("%Y-%m-%d"),
                "close": round(safe_float(last["Close"]), 2),
                "rsi": round(safe_float(last["rsi"]), 2),
                "recommendation": str(last["recommendation"]),
            })

            print(f"✅ {symbol}: {len(feature_rows)} lignes features enregistrées.")

        except Exception as e:
            print(f"❌ Erreur features/signal {symbol}: {e}")
            session.rollback()

    if all_features_csv:
        feat_df = pd.concat(all_features_csv, ignore_index=True)
        feat_df.to_csv(os.path.join(DATA_FOLDER, "ALL_FEATURES.csv"), index=False)
        print(f"✅ ALL_FEATURES.csv sauvegardé : {len(feat_df)} lignes")

    if latest_signals:
        signals_df = pd.DataFrame(latest_signals)
        signals_df.to_csv(os.path.join(DATA_FOLDER, "latest_signals.csv"), index=False)
        try:
            print("\n=== SIGNAUX DU JOUR ===")
            print(signals_df.to_markdown(index=False))
        except ImportError:
            print(signals_df)

    session.close()
    print("--- FIN FEATURES + SIGNAUX ---\n")

# ====================== GIT PUSH ======================
def commit_and_push():
    subprocess.run(["git", "config", "user.name", "GitHub Actions Bot"])
    subprocess.run(["git", "config", "user.email", "github-actions@github.com"])
    subprocess.run(["git", "add", "data/"])
    res = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
    if res.stdout.strip():
        subprocess.run(["git", "commit", "-m", f"Auto-update {datetime.now().strftime('%Y-%m-%d')}"], check=False)
        subprocess.run(["git", "push"], check=False)
        print("✅ Données CSV pushées.")
    else:
        print("Info: Pas de changements CSV.")

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
