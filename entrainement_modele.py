import os
import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error

# 1. CONNEXION À LA BASE DE DONNÉES
# Assurez-vous d'avoir votre DB_URL (la même que pour le script de collecte)
# Si vous testez en local, remplacez os.getenv par votre vrai lien 'postgresql://...'
DB_URL = os.getenv('DB_URL') 

if not DB_URL:
    raise ValueError("DB_URL manquante. Vérifiez vos variables d'environnement.")

print("🔌 Connexion à Supabase...")
engine = create_engine(DB_URL)

# 2. RÉCUPÉRATION DES DONNÉES (Le "Select")
# On récupère l'historique et les features (indicateurs techniques)
# Exemple pour Apple (AAPL)
print("📥 Téléchargement des données pour AAPL...")

query = """
SELECT 
    h.date,
    h.close,
    f.rsi,
    f.macd,
    f.ma_5,
    f.volatility
FROM historical_data h
JOIN features f ON h.date = f.date AND h.symbol = f.symbol
WHERE h.symbol = 'AAPL'
ORDER BY h.date ASC
"""

# Pandas exécute la requête SQL et transforme le résultat en Tableau (DataFrame)
df = pd.read_sql(query, engine)

# 3. PRÉPARATION POUR L'IA
print(f"📊 Données récupérées : {len(df)} lignes")

# On enlève les lignes vides s'il y en a
df = df.dropna()

# Définition des X (ce que l'IA regarde) et y (ce que l'IA doit deviner)
# X = RSI, MACD, Moyenne Mobile, Volatilité
X = df[['rsi', 'macd', 'ma_5', 'volatility']]

# y = Le prix de clôture (Close) qu'on veut prédire
y = df['close']

# Séparation : 80% pour apprendre, 20% pour tester
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

# 4. ENTRAÎNEMENT DU MODÈLE
print("🧠 Entraînement du modèle en cours...")
model = RandomForestRegressor(n_estimators=100) # On utilise une Forêt Aléatoire simple pour l'exemple
model.fit(X_train, y_train)

# 5. ÉVALUATION
predictions = model.predict(X_test)
mae = mean_absolute_error(y_test, predictions)

print("--- RÉSULTATS ---")
print(f"Erreur moyenne (MAE) : {mae:.2f} $")
print(f"Dernier prix réel : {y_test.iloc[-1]} $")
print(f"Dernier prix prédit : {predictions[-1]:.2f} $")
