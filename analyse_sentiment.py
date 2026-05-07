# -*- coding: utf-8 -*-
"""
Analyse de sentiment FinBERT
Stockage cumulatif par action + fichier master combiné + envoi vers Supabase.
Version modifiée : tentative de collecte sur 5 ans + plus d'articles/source + requêtes enrichies.

Note : NewsAPI/GNews peuvent limiter fortement l'historique selon l'abonnement.
Le script demande 5 ans, mais l'API peut renvoyer uniquement ce qu'elle autorise.
"""

import os
import time
import random
import hashlib
import warnings
from datetime import datetime, timedelta

import requests
import pandas as pd
from transformers import pipeline
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.orm import declarative_base, sessionmaker

warnings.filterwarnings("ignore")

# ==================== CONFIGURATION ====================
class Config:
    USE_NEWSAPI = True
    USE_GNEWS = True

    NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
    GNEWS_API_KEY = os.getenv("GNEWS_API_KEY")
    DB_URL = os.getenv("DB_URL")

    YEARS_BACK = 5
    DAYS_BACK = 365 * YEARS_BACK
    MAX_ARTICLES_PER_SOURCE = 50
    FINBERT_MODEL = "ProsusAI/finbert"

    ARTICLES_FOLDER = "data/articles_sentiment"
    os.makedirs(ARTICLES_FOLDER, exist_ok=True)
    MASTER_FILE = f"{ARTICLES_FOLDER}/ALL_ARTICLES_MASTER.csv"

# ==================== BASE DE DONNÉES ====================
print("🔌 Initialisation de la connexion à Supabase...")
try:
    if Config.DB_URL:
        engine = create_engine(Config.DB_URL, echo=False)
        Base = declarative_base()
        Session = sessionmaker(bind=engine)
    else:
        print("⚠️ Variable d'environnement DB_URL non trouvée.")
        engine = None
        Base = declarative_base()
        Session = None
except Exception as e:
    print(f"❌ Erreur Config DB: {e}")
    engine = None
    Base = declarative_base()
    Session = None

if engine:
    class DailySentiment(Base):
        __tablename__ = "daily_sentiment"
        id = Column(Integer, primary_key=True, autoincrement=True)
        date = Column(Date, nullable=False)
        symbol = Column(String, nullable=False)
        sentiment_score = Column(Float, default=0.0)
        articles_count = Column(Integer, default=0)

    Base.metadata.create_all(engine)
else:
    DailySentiment = None

# ==================== ENTREPRISES ====================
ENTREPRISES = [
    {"ticker": "AAPL", "name": "Apple", "queries": ["Apple", "Apple Inc", "AAPL", "Apple stock", "Apple earnings", "iPhone Apple", "Tim Cook Apple"]},
    {"ticker": "MSFT", "name": "Microsoft", "queries": ["Microsoft", "MSFT", "Microsoft stock", "Microsoft earnings", "Azure Microsoft", "Satya Nadella"]},
    {"ticker": "TSLA", "name": "Tesla", "queries": ["Tesla", "TSLA", "Tesla stock", "Tesla earnings", "Elon Musk Tesla"]},
    {"ticker": "NVDA", "name": "Nvidia", "queries": ["Nvidia", "NVDA", "Nvidia stock", "Nvidia earnings", "Nvidia AI chips"]},
    {"ticker": "GOOGL", "name": "Google", "queries": ["Google", "Alphabet", "GOOGL", "Google stock", "Alphabet earnings"]},
    {"ticker": "AMZN", "name": "Amazon", "queries": ["Amazon", "AMZN", "Amazon stock", "Amazon earnings", "AWS Amazon"]},
    {"ticker": "META", "name": "Meta", "queries": ["Meta", "Facebook Meta", "META stock", "Meta earnings", "Mark Zuckerberg Meta"]},
]

# ==================== UTILITAIRES ====================
def generate_article_id(title, source, date):
    unique_string = f"{title}_{source}_{date}"
    return hashlib.md5(unique_string.encode()).hexdigest()[:16]


def parse_publication_date(value):
    if not value:
        return None
    try:
        dt = pd.to_datetime(value, errors="coerce", utc=True)
        if pd.isna(dt):
            return None
        return dt.tz_convert(None).to_pydatetime()
    except Exception:
        return None


def load_company_articles(ticker):
    company_file = f"{Config.ARTICLES_FOLDER}/{ticker}.csv"
    try:
        if os.path.exists(company_file):
            df = pd.read_csv(company_file)
            print(f"Chargement de {ticker}.csv avec {len(df)} articles existants")
            return df
        print(f"Création du fichier {ticker}.csv")
        return pd.DataFrame()
    except Exception as e:
        print(f"Erreur chargement {ticker}.csv: {e}")
        return pd.DataFrame()


def save_company_articles(ticker, new_articles_df):
    company_file = f"{Config.ARTICLES_FOLDER}/{ticker}.csv"
    try:
        old_df = load_company_articles(ticker)
        if old_df.empty:
            final_df = new_articles_df.copy()
            print(f"Création de {ticker}.csv avec {len(new_articles_df)} articles")
        else:
            existing_ids = set(old_df["article_id"].astype(str).values) if "article_id" in old_df.columns else set()
            to_add = new_articles_df[~new_articles_df["article_id"].astype(str).isin(existing_ids)].copy()
            if not to_add.empty:
                final_df = pd.concat([old_df, to_add], ignore_index=True)
                print(f"Ajout de {len(to_add)} nouveaux articles à {ticker}.csv")
            else:
                final_df = old_df.copy()
                print(f"Aucun nouvel article pour {ticker}")

        final_df = final_df.drop_duplicates(subset=["article_id"]).reset_index(drop=True)
        final_df.to_csv(company_file, index=False)
        print(f"{ticker}.csv sauvegardé: {len(final_df)} articles au total")
        return final_df
    except Exception as e:
        print(f"Erreur sauvegarde {ticker}.csv: {e}")
        return pd.DataFrame()


def update_master_file(all_new_articles):
    try:
        if os.path.exists(Config.MASTER_FILE):
            master_df = pd.read_csv(Config.MASTER_FILE)
            print(f"Chargement du master avec {len(master_df)} articles")
        else:
            master_df = pd.DataFrame()
            print("Création d'un nouveau master")

        if master_df.empty:
            final_df = all_new_articles.copy()
        else:
            existing_ids = set(master_df["article_id"].astype(str).values) if "article_id" in master_df.columns else set()
            to_add = all_new_articles[~all_new_articles["article_id"].astype(str).isin(existing_ids)].copy()
            if not to_add.empty:
                final_df = pd.concat([master_df, to_add], ignore_index=True)
                print(f"Ajout de {len(to_add)} nouveaux articles au master")
            else:
                final_df = master_df.copy()
                print("Aucun nouvel article à ajouter au master")

        final_df = final_df.drop_duplicates(subset=["article_id"]).reset_index(drop=True)
        final_df.to_csv(Config.MASTER_FILE, index=False)
        print(f"Master sauvegardé: {Config.MASTER_FILE}")
        print(f"Total articles dans master: {len(final_df)}")
        return final_df
    except Exception as e:
        print(f"Erreur mise à jour master: {e}")
        return pd.DataFrame()

# ==================== SYNC SUPABASE ====================
def sync_to_supabase(master_df):
    if not engine or master_df.empty:
        print("⚠️ Impossible de synchroniser avec Supabase.")
        return

    print("\n" + "=" * 60)
    print("☁️ SYNCHRONISATION VERS SUPABASE (daily_sentiment)")
    print("=" * 60)

    session = Session()
    try:
        df = master_df.copy()
        df["date_clean"] = pd.to_datetime(df["date_publication"], errors="coerce").dt.date
        df = df.dropna(subset=["date_clean", "symbol", "score_pondere"])

        daily_stats = df.groupby(["symbol", "date_clean"]).agg(
            sentiment_score=("score_pondere", "mean"),
            articles_count=("article_id", "count"),
        ).reset_index()

        lignes_ajoutees = 0
        lignes_mises_a_jour = 0

        for _, row in daily_stats.iterrows():
            symbol = str(row["symbol"])
            day = row["date_clean"]

            existing_rows = session.query(DailySentiment).filter_by(date=day, symbol=symbol).all()
            if existing_rows:
                # Garde une seule ligne propre et supprime les doublons éventuels.
                first = existing_rows[0]
                first.sentiment_score = float(row["sentiment_score"])
                first.articles_count = int(row["articles_count"])
                for duplicate in existing_rows[1:]:
                    session.delete(duplicate)
                lignes_mises_a_jour += 1
            else:
                session.add(DailySentiment(
                    date=day,
                    symbol=symbol,
                    sentiment_score=float(row["sentiment_score"]),
                    articles_count=int(row["articles_count"]),
                ))
                lignes_ajoutees += 1

        session.commit()
        print(f"✅ Supabase à jour : {lignes_ajoutees} lignes créées, {lignes_mises_a_jour} lignes mises à jour.")
    except Exception as e:
        print(f"❌ Erreur lors de l'envoi vers Supabase : {e}")
        session.rollback()
    finally:
        session.close()

# ==================== SOURCES D'ARTICLES ====================
class NewsAPIClient:
    @staticmethod
    def get_articles(query, max_results=10):
        if not Config.USE_NEWSAPI or not Config.NEWSAPI_KEY:
            print("NewsAPI désactivée ou clé manquante")
            return []

        from_date = (datetime.now() - timedelta(days=Config.DAYS_BACK)).strftime("%Y-%m-%d")
        url = "https://newsapi.org/v2/everything"
        params = {
            "q": f"({query}) AND (stock OR shares OR earnings OR revenue OR market OR investors)",
            "apiKey": Config.NEWSAPI_KEY,
            "pageSize": min(int(max_results), 100),
            "language": "en",
            "sortBy": "publishedAt",
            "from": from_date,
        }

        try:
            print(f"Recherche NewsAPI: {query}")
            response = requests.get(url, params=params, timeout=25)
            if response.status_code == 200:
                articles = response.json().get("articles", [])
                print(f"NewsAPI: {len(articles)} articles trouvés")
                for article in articles:
                    article["api_source"] = "newsapi"
                return articles
            print(f"NewsAPI Erreur {response.status_code}: {response.text[:200]}")
            return []
        except Exception as e:
            print(f"Erreur connexion NewsAPI: {e}")
            return []

class GNewsClient:
    @staticmethod
    def get_articles(query, max_results=10):
        if not Config.USE_GNEWS or not Config.GNEWS_API_KEY:
            print("GNews désactivée ou clé manquante")
            return []

        url = "https://gnews.io/api/v4/search"
        params = {
            "q": f"{query} stock market",
            "token": Config.GNEWS_API_KEY,
            "lang": "en",
            "max": min(int(max_results), 100),
            "from": (datetime.now() - timedelta(days=Config.DAYS_BACK)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sortby": "publishedAt",
        }

        try:
            print(f"Recherche GNews: {query}")
            response = requests.get(url, params=params, timeout=25)
            if response.status_code == 200:
                data = response.json()
                articles = data.get("articles", [])
                print(f"GNews: {len(articles)} articles trouvés")
                formatted_articles = []
                for article in articles:
                    formatted_articles.append({
                        "title": article.get("title", ""),
                        "description": article.get("description", ""),
                        "content": article.get("content", ""),
                        "publishedAt": article.get("publishedAt", ""),
                        "source": {"name": article.get("source", {}).get("name", "GNews")},
                        "url": article.get("url", ""),
                        "api_source": "gnews",
                    })
                return formatted_articles
            print(f"GNews Erreur {response.status_code}: {response.text[:200]}")
            return []
        except Exception as e:
            print(f"Erreur connexion GNews: {e}")
            return []

class ArticleFetcher:
    @staticmethod
    def get_all_articles(query, max_results_per_source=50):
        all_articles = []
        if Config.USE_NEWSAPI:
            all_articles.extend(NewsAPIClient.get_articles(query, max_results_per_source))
            time.sleep(1)
        if Config.USE_GNEWS:
            all_articles.extend(GNewsClient.get_articles(query, max_results_per_source))
            time.sleep(1)

        random.shuffle(all_articles)
        max_total = int(max_results_per_source) * 2
        if len(all_articles) > max_total:
            all_articles = all_articles[:max_total]

        print(f"Total articles: {len(all_articles)}")
        return all_articles

# ==================== FINBERT ====================
class SentimentAnalyzer:
    def __init__(self):
        print("CHARGEMENT DU MODÈLE FinBERT")
        try:
            self.sentiment_pipeline = pipeline(
                "sentiment-analysis",
                model=Config.FINBERT_MODEL,
                tokenizer=Config.FINBERT_MODEL,
                device=-1,
            )
            print("Modèle FinBERT chargé avec succès!")
        except Exception as e:
            print(f"Erreur chargement FinBERT: {e}")
            self.sentiment_pipeline = None

    def build_query(self, ticker, company_name):
        return f'"{company_name}" OR {ticker}'

    def analyze_text(self, text):
        try:
            text = str(text).strip()
            if len(text) < 20:
                return {"score": 0, "label": "neutral", "confidence": 0.1}

            if len(text) > 500:
                text = text[:400] + " ... " + text[-100:]

            if self.sentiment_pipeline:
                result = self.sentiment_pipeline(text)[0]
                label = str(result["label"]).lower()
                confidence = float(result["score"])
            else:
                label, confidence = "neutral", 0.1

            if label == "positive":
                score = confidence
            elif label == "negative":
                score = -confidence
            else:
                score = 0.0

            return {"score": max(-1.0, min(1.0, score)), "label": label, "confidence": confidence}
        except Exception as e:
            print(f"Erreur analyse: {e}")
            return {"score": 0, "label": "neutral", "confidence": 0.1}

    def calculate_weight(self, article):
        weight = 1.0
        api_source = article.get("api_source", "unknown")
        if api_source == "newsapi":
            weight *= 1.2

        pub_date = parse_publication_date(article.get("publishedAt", ""))
        if pub_date:
            hours_old = (datetime.now() - pub_date).total_seconds() / 3600
            if hours_old <= 24:
                weight *= 1.3
            elif hours_old <= 48:
                weight *= 1.1
            else:
                weight *= 0.9

        source_name = article.get("source", {}).get("name", "").lower()
        important_sources = ["reuters", "bloomberg", "cnbc", "financial times", "wall street journal", "yahoo finance"]
        if any(source in source_name for source in important_sources):
            weight *= 1.5
        elif any(blog in source_name for blog in ["blog", "personal", "medium"]):
            weight *= 0.6

        content_length = len(str(article.get("title", "")) + str(article.get("description", "")) + str(article.get("content", "")))
        if content_length > 300:
            weight *= 1.2
        elif content_length < 100:
            weight *= 0.8

        return max(0.2, min(weight, 3.0))

    def analyze_company_query(self, ticker, company_name, query):
        print(f"ANALYSE POUR {company_name} ({ticker}) | Query: {query}")
        articles = ArticleFetcher.get_all_articles(query, Config.MAX_ARTICLES_PER_SOURCE)
        if not articles:
            print("Aucun article trouvé")
            return pd.DataFrame()

        analyzed_articles = []
        for i, article in enumerate(articles, 1):
            title = str(article.get("title", "Sans titre")).strip()
            description = str(article.get("description", "")).strip()
            content = str(article.get("content", "")).strip()
            full_text = f"{title}. {content if content else description}".strip()

            source_name = article.get("source", {}).get("name", "Inconnue")
            published_at = article.get("publishedAt", "")
            article_id = generate_article_id(title, source_name, published_at)
            sentiment = self.analyze_text(full_text)
            weight = self.calculate_weight(article)
            weighted_score = sentiment["score"] * weight

            pub_dt = parse_publication_date(published_at)
            pub_date_str = pub_dt.strftime("%Y-%m-%d %H:%M") if pub_dt else str(published_at)[:16]

            analyzed_articles.append({
                "article_id": article_id,
                "symbol": ticker,
                "company": company_name,
                "query_used": query,
                "date_publication": pub_date_str,
                "source": str(source_name),
                "api_source": article.get("api_source", "unknown"),
                "titre": title[:200],
                "contenu": full_text[:1000],
                "score_sentiment": round(sentiment["score"], 4),
                "sentiment": sentiment["label"],
                "confiance": round(sentiment["confidence"], 4),
                "poids": round(weight, 4),
                "score_pondere": round(weighted_score, 4),
                "url": article.get("url", ""),
                "date_analyse": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "ajout_date": datetime.now().strftime("%Y-%m-%d"),
            })
            print(f"Article {i}: {title[:70]}... | score={sentiment['score']:.3f} | source={source_name}")

        return pd.DataFrame(analyzed_articles)

    def analyze_company(self, ticker, company_name, queries=None):
        queries = queries or [self.build_query(ticker, company_name)]
        batches = []
        for query in queries:
            df_q = self.analyze_company_query(ticker, company_name, query)
            if not df_q.empty:
                batches.append(df_q)
            time.sleep(1)

        if not batches:
            return pd.DataFrame()

        df = pd.concat(batches, ignore_index=True).drop_duplicates(subset=["article_id"]).reset_index(drop=True)
        save_company_articles(ticker, df)
        print(f"STATISTIQUES {ticker}: {len(df)} articles nouveaux/bruts dans cette exécution")
        print(df["sentiment"].value_counts(dropna=False).to_string())
        print(f"Score moyen: {df['score_sentiment'].mean():.3f}")
        return df

# ==================== TEST APIS ====================
def test_apis():
    print("TEST DES APIS...")
    if Config.USE_NEWSAPI and Config.NEWSAPI_KEY:
        print(f"Test NewsAPI (clé: {Config.NEWSAPI_KEY[:10]}...)")
        try:
            response = requests.get(
                "https://newsapi.org/v2/everything",
                params={"q": "apple", "apiKey": Config.NEWSAPI_KEY, "pageSize": 1},
                timeout=10,
            )
            print("NewsAPI:", "Connexion OK" if response.status_code == 200 else f"Erreur {response.status_code}")
        except Exception as e:
            print(f"NewsAPI: {e}")
    else:
        print("NewsAPI désactivée ou clé absente")

    if Config.USE_GNEWS and Config.GNEWS_API_KEY:
        print(f"Test GNews (clé: {Config.GNEWS_API_KEY[:10]}...)")
        try:
            response = requests.get(
                "https://gnews.io/api/v4/top-headlines",
                params={"token": Config.GNEWS_API_KEY, "lang": "en", "max": 1},
                timeout=10,
            )
            print("GNews:", "Connexion OK" if response.status_code == 200 else f"Erreur {response.status_code}")
        except Exception as e:
            print(f"GNews: {e}")
    else:
        print("GNews désactivée ou clé absente")

# ==================== MAIN ====================
def main():
    print("DÉBUT DE L'ANALYSE DE SENTIMENT")
    print("=" * 60)
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Période demandée: {Config.DAYS_BACK} jours (~{Config.YEARS_BACK} ans)")
    print(f"Sources: NewsAPI={Config.USE_NEWSAPI}, GNews={Config.USE_GNEWS}")
    print(f"Articles max/source/query: {Config.MAX_ARTICLES_PER_SOURCE}")
    print(f"Dossier: {Config.ARTICLES_FOLDER}")
    print("=" * 60)

    test_apis()
    analyzer = SentimentAnalyzer()

    all_new_articles = []
    for entreprise in ENTREPRISES:
        print("\n" + "=" * 60)
        df = analyzer.analyze_company(entreprise["ticker"], entreprise["name"], entreprise.get("queries"))
        if not df.empty:
            all_new_articles.append(df)

    if all_new_articles:
        combined_new = pd.concat(all_new_articles, ignore_index=True).drop_duplicates(subset=["article_id"])
        print("\n" + "=" * 60)
        print("RÉSUMÉ DE LA COLLECTE")
        print("=" * 60)
        print(f"Total articles collectés dans cette exécution: {len(combined_new)}")

        master_df = update_master_file(combined_new)
        if not master_df.empty:
            sync_to_supabase(master_df)
            print("\nSTATISTIQUES MASTER:")
            print(f"  Total articles: {len(master_df)}")
            print(f"  Entreprises uniques: {master_df['symbol'].nunique()}")
            print("\nARTICLES PAR ENTREPRISE:")
            print(master_df["symbol"].value_counts().to_string())
    else:
        print("⚠️ Aucun article collecté. Vérifie les clés API et les limites d'abonnement.")

    print("\n" + "=" * 60)
    print("ANALYSE TERMINÉE")
    print("=" * 60)

if __name__ == "__main__":
    print("CONFIGURATION DU SCRIPT")
    print("=" * 60)
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"NewsAPI: {'Active' if Config.USE_NEWSAPI else 'Désactivée'}")
    print(f"GNews: {'Active' if Config.USE_GNEWS else 'Désactivée'}")
    print(f"Articles/source/query: {Config.MAX_ARTICLES_PER_SOURCE}")
    print(f"Période demandée: {Config.DAYS_BACK} jours")
    print(f"Modèle: {Config.FINBERT_MODEL}")
    print("=" * 60)
    main()
