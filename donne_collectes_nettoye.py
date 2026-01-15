# ====================== COLLECTE YFINANCE ======================
def collect_yfinance():
    session = Session()
    print("\n--- DÉBUT COLLECTE YFINANCE ---")
    
    # --- LISTE COMPLÈTE DES ACTIFS ---
    symbols = [
        # === Big Tech / Large Caps ===
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

        # === Startups / Growth Stocks ===
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

        # === Crypto ===
        {"symbol": "BTC-USD", "name": "Bitcoin USD", "sector": "Crypto"},
    ]
    
    # 1. Init Stocks (Table de référence)
    # On met à jour la table 'stocks' avec les nouveaux noms/secteurs
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
            # Pause légèrement augmentée pour éviter le blocage Yahoo (23 actifs)
            time.sleep(1.5) 
            
            # Note: On utilise directement le symbole (BTC-USD est déjà correct dans la liste)
            df = yf.Ticker(sym_str).history(period="1y").reset_index()
            
            if df.empty:
                print(f" Aucune donnée reçue pour {sym_str}")
                continue
                
            df['symbol'] = sym_str
            df['date'] = df['Date']
            
            # Vérification basique des colonnes
            if 'Close' not in df.columns:
                print(f"⚠️ Colonnes manquantes pour {sym_str}")
                continue

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
            
            # 🔥 COMMIT À CHAQUE SYMBOLE (Sécurité) 🔥
            session.commit()
            print(f"✅ {sym_str} : Sauvegardé en BD.")

        except Exception as e:
            print(f" CRASH sur {sym_str}: {e}")
            session.rollback() # Annule l'erreur pour passer au suivant

    # Sauvegarde CSV global
    if all_data:
        pd.concat(all_data).to_csv(os.path.join(DATA_FOLDER, "ALL_YFINANCE.csv"), index=False)
    
    session.close()
    print("--- FIN YFINANCE ---\n")
