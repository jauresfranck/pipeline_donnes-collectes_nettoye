# -*- coding: utf-8 -*-
"""
Analyse de sentiment simplifiée
Stockage cumulatif par action + fichier master combiné + ENVOI VERS SUPABASE
"""

import requests
from transformers import pipeline
import pandas as pd
from datetime import datetime, timedelta
import warnings
import os
import time
import random
import hashlib
warnings.filterwarnings('ignore')

# === NOUVEAUX IMPORTS POUR LA BASE DE DONNÉES ===
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.orm import declarative_base, sessionmaker

# ==================== CONFIGURATION ====================
class Config:
    """Configuration centrale du projet"""
    USE_NEWSAPI = True
    USE_GNEWS = True
    
    NEWSAPI_KEY = os.getenv('NEWSAPI_KEY')
    GNEWS_API_KEY = os.getenv('GNEWS_API_KEY')
    DB_URL = os.getenv('DB_URL') # <--- URL Supabase
    
    MAX_ARTICLES_PER_SOURCE = 8
    DAYS_BACK = 3
    FINBERT_MODEL = "ProsusAI/finbert"
    
    ARTICLES_FOLDER = "data/articles_sentiment"
    os.makedirs(ARTICLES_FOLDER, exist_ok=True)
    MASTER_FILE = f"{ARTICLES_FOLDER}/ALL_ARTICLES_MASTER.csv"

# ==================== CONFIGURATION BASE DE DONNÉES ====================
print("🔌 Initialisation de la connexion à Supabase...")
try:
    if Config.DB_URL:
        engine = create_engine(Config.DB_URL, echo=False)
        Base = declarative_base()
        Session = sessionmaker(bind=engine)
    else:
        print("⚠️ Attention : Variable d'environnement DB_URL non trouvée.")
        engine = None
except Exception as e:
    print(f"❌ Erreur Config DB: {e}")
    engine = None

# Définition de la table dans Supabase
if engine:
    class DailySentiment(Base):
        __tablename__ = 'daily_sentiment'
        id = Column(Integer, primary_key=True, autoincrement=True)
        date = Column(Date, nullable=False)
        symbol = Column(String, nullable=False)
        sentiment_score = Column(Float, default=0.0)
        articles_count = Column(Integer, default=0)
    
    # S'assure que la table existe
    Base.metadata.create_all(engine)

# ==================== FONCTIONS UTILITAIRES ====================
def generate_article_id(title, source, date):
    unique_string = f"{title}_{source}_{date}"
    return hashlib.md5(unique_string.encode()).hexdigest()[:16]

def load_company_articles(ticker):
    company_file = f"{Config.ARTICLES_FOLDER}/{ticker}.csv"
    try:
        if os.path.exists(company_file):
            df = pd.read_csv(company_file)
            print(f"Chargement de {ticker}.csv avec {len(df)} articles existants")
            return df
        else:
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
            old_df = new_articles_df
            print(f"Création de {ticker}.csv avec {len(new_articles_df)} articles")
        else:
            existing_ids = set(old_df['article_id'].values) if 'article_id' in old_df.columns else set()
            new_articles_df = new_articles_df[~new_articles_df['article_id'].isin(existing_ids)]
            if not new_articles_df.empty:
                old_df = pd.concat([old_df, new_articles_df], ignore_index=True)
                print(f"Ajout de {len(new_articles_df)} nouveaux articles a {ticker}.csv")
            else:
                print(f"Aucun nouvel article pour {ticker}")
        
        old_df.to_csv(company_file, index=False)
        print(f"{ticker}.csv sauvegardé: {len(old_df)} articles au total")
        return old_df
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
            master_df = all_new_articles
        else:
            existing_ids = set(master_df['article_id'].values) if 'article_id' in master_df.columns else set()
            new_articles = all_new_articles[~all_new_articles['article_id'].isin(existing_ids)]
            if not new_articles.empty:
                master_df = pd.concat([master_df, new_articles], ignore_index=True)
                print(f"Ajout de {len(new_articles)} nouveaux articles au master")
            else:
                print("Aucun nouvel article à ajouter au master")
        
        master_df.to_csv(Config.MASTER_FILE, index=False)
        print(f"Master sauvegardé: {Config.MASTER_FILE}")
        print(f"Total articles dans master: {len(master_df)}")
        return master_df
    except Exception as e:
        print(f"Erreur mise a jour master: {e}")
        return pd.DataFrame()

# ==================== ENVOI VERS SUPABASE ====================
def sync_to_supabase(master_df):
    """Calcule les moyennes par jour et envoie à Supabase"""
    if not engine or master_df.empty:
        print("⚠️ Impossible de synchroniser avec Supabase (Pas de connexion ou pas de données).")
        return

    print("\n" + "="*60)
    print("☁️ SYNCHRONISATION VERS SUPABASE (Table daily_sentiment)")
    print("="*60)
    
    session = Session()
    try:
        # Nettoyage de la date
        master_df['date_clean'] = pd.to_datetime(master_df['date_publication']).dt.date
        
        # Calcul de la moyenne des sentiments par jour et par entreprise
        daily_stats = master_df.groupby(['symbol', 'date_clean']).agg(
            sentiment_score=('score_pondere', 'mean'),
            articles_count=('article_id', 'count')
        ).reset_index()

        lignes_ajoutees = 0
        lignes_mises_a_jour = 0

        for _, row in daily_stats.iterrows():
            # Cherche si ce jour existe déjà dans la base
            existing = session.query(DailySentiment).filter_by(
                date=row['date_clean'], 
                symbol=str(row['symbol'])
            ).first()
            
            if existing:
                existing.sentiment_score = float(row['sentiment_score'])
                existing.articles_count = int(row['articles_count'])
                lignes_mises_a_jour += 1
            else:
                new_sentiment = DailySentiment(
                    date=row['date_clean'],
                    symbol=str(row['symbol']),
                    sentiment_score=float(row['sentiment_score']),
                    articles_count=int(row['articles_count'])
                )
                session.add(new_sentiment)
                lignes_ajoutees += 1
                
        session.commit()
        print(f"✅ Supabase à jour : {lignes_ajoutees} nouvelles lignes créées, {lignes_mises_a_jour} mises à jour.")

    except Exception as e:
        print(f"❌ Erreur lors de l'envoi vers Supabase : {e}")
        session.rollback()
    finally:
        session.close()

# ==================== SOURCES D'ARTICLES ====================
class NewsAPIClient:
    """Client pour recuperer les articles via NewsAPI"""
    @staticmethod
    def get_articles(query, max_results=10):
        if not Config.USE_NEWSAPI or not Config.NEWSAPI_KEY:
            print("NewsAPI desactive ou cle manquante")
            return []
        
        from_date = (datetime.now() - timedelta(days=Config.DAYS_BACK)).strftime('%Y-%m-%d')
        url = "https://newsapi.org/v2/everything"
        params = {
            'q': f'{query} AND (stock OR shares OR earnings OR revenue OR market)',
            'apiKey': Config.NEWSAPI_KEY,
            'pageSize': max_results,
            'language': 'en',
            'sortBy': 'relevancy',
            'from': from_date,
        }
        try:
            print(f"Recherche NewsAPI: {query}")
            response = requests.get(url, params=params, timeout=20)
            if response.status_code == 200:
                articles = response.json().get('articles', [])
                print(f"NewsAPI: {len(articles)} articles trouves")
                for article in articles:
                    article['api_source'] = 'newsapi'
                return articles
            else:
                print(f"NewsAPI Erreur {response.status_code}")
                return []
        except Exception as e:
            print(f"Erreur connexion NewsAPI: {e}")
            return []

class GNewsClient:
    """Client pour recuperer les articles via GNews"""
    @staticmethod
    def get_articles(query, max_results=10):
        if not Config.USE_GNEWS or not Config.GNEWS_API_KEY:
            print("GNews desactive ou cle manquante")
            return []
        
        url = "https://gnews.io/api/v4/search"
        params = {
            'q': f'{query} stock market',
            'token': Config.GNEWS_API_KEY,
            'lang': 'en',
            'max': max_results,
            'from': (datetime.now() - timedelta(days=Config.DAYS_BACK)).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'to': datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'sortby': 'relevance'
        }
        try:
            print(f"Recherche GNews: {query}")
            response = requests.get(url, params=params, timeout=20)
            if response.status_code == 200:
                data = response.json()
                articles = data.get('articles', [])
                print(f"GNews: {len(articles)} articles trouves")
                formatted_articles = []
                for article in articles:
                    formatted_article = {
                        'title': article.get('title', ''),
                        'description': article.get('description', ''),
                        'content': article.get('content', ''),
                        'publishedAt': article.get('publishedAt', ''),
                        'source': {'name': article.get('source', {}).get('name', 'GNews')},
                        'url': article.get('url', ''),
                        'api_source': 'gnews'
                    }
                    formatted_articles.append(formatted_article)
                return formatted_articles
            else:
                print(f"GNews Erreur {response.status_code}")
                return []
        except Exception as e:
            print(f"Erreur connexion GNews: {e}")
            return []

class ArticleFetcher:
    """Combine les articles de toutes les sources"""
    @staticmethod
    def get_all_articles(query, max_results_per_source=8):
        all_articles = []
        if Config.USE_NEWSAPI:
            newsapi_articles = NewsAPIClient.get_articles(query, max_results_per_source)
            all_articles.extend(newsapi_articles)
            time.sleep(1)
        if Config.USE_GNEWS:
            gnews_articles = GNewsClient.get_articles(query, max_results_per_source)
            all_articles.extend(gnews_articles)
            time.sleep(1)
        
        random.shuffle(all_articles)
        max_total = max_results_per_source * 2
        if len(all_articles) > max_total:
            all_articles = all_articles[:max_total]
        
        print(f"Total articles: {len(all_articles)} (NewsAPI: {Config.USE_NEWSAPI}, GNews: {Config.USE_GNEWS})")
        return all_articles

# ==================== ANALYSE SENTIMENT ====================
class SentimentAnalyzer:
    """Analyseur de sentiment avec FinBERT"""
    def __init__(self):
        print("CHARGEMENT DU MODELE FinBERT")
        try:
            self.sentiment_pipeline = pipeline(
                "sentiment-analysis",
                model=Config.FINBERT_MODEL,
                tokenizer=Config.FINBERT_MODEL,
                device=-1
            )
            print("Modele FinBERT charge avec succes!")
        except Exception as e:
            print(f"Erreur chargement FinBERT: {e}")
            self.sentiment_pipeline = None
    
    def build_query(self, ticker, company_name):
        return f'"{company_name}" OR {ticker}'
    
    def analyze_text(self, text):
        try:
            text = str(text).strip()
            if len(text) < 20:
                return {'score': 0, 'label': 'neutral', 'confidence': 0.1}
            
            if len(text) > 500:
                text = text[:400] + " ... " + text[-100:]
            
            if self.sentiment_pipeline:
                result = self.sentiment_pipeline(text)[0]
                label = result['label']
                confidence = result['score']
            else:
                positive_words = ['profit', 'gain', 'growth', 'increase', 'rise', 'positive', 'strong', 'bullish']
                negative_words = ['loss', 'decline', 'decrease', 'fall', 'negative', 'weak', 'bearish']
                
                text_lower = text.lower()
                positive_count = sum(1 for word in positive_words if word in text_lower)
                negative_count = sum(1 for word in negative_words if word in text_lower)
                
                total = positive_count + negative_count
                if total > 0:
                    score = (positive_count - negative_count) / total
                    if score > 0.1:
                        label = 'positive'
                    elif score < -0.1:
                        label = 'negative'
                    else:
                        label = 'neutral'
                    confidence = min(0.9, total / 10)
                else:
                    label = 'neutral'
                    confidence = 0.1
                    score = 0
            
            if label == 'positive':
                score = confidence if 'confidence' in locals() else 0.7
            elif label == 'negative':
                score = -confidence if 'confidence' in locals() else -0.7
            else:
                score = 0
            
            score = max(-1.0, min(1.0, score))
            return {
                'score': score,
                'label': label,
                'confidence': confidence if 'confidence' in locals() else 0.5
            }
        except Exception as e:
            print(f"Erreur analyse: {e}")
            return {'score': 0, 'label': 'neutral', 'confidence': 0.1}
    
    def calculate_weight(self, article):
        weight = 1.0
        api_source = article.get('api_source', 'unknown')
        if api_source == 'newsapi':
            weight *= 1.2
        elif api_source == 'gnews':
            weight *= 1.0
        
        if 'publishedAt' in article and article['publishedAt']:
            try:
                pub_str = str(article['publishedAt'])
                if 'T' in pub_str:
                    pub_date = datetime.strptime(pub_str, '%Y-%m-%dT%H:%M:%SZ')
                else:
                    pub_date = datetime.strptime(pub_str, '%Y-%m-%d %H:%M:%S')
                
                hours_old = (datetime.now() - pub_date).total_seconds() / 3600
                if hours_old <= 24:
                    weight *= 1.3
                elif hours_old <= 48:
                    weight *= 1.1
                else:
                    weight *= 0.9
            except:
                pass
        
        source_name = article.get('source', {}).get('name', '').lower()
        important_sources = ['reuters', 'bloomberg', 'cnbc', 'financial times', 'wall street journal', 'yahoo finance']
        if any(source in source_name for source in important_sources):
            weight *= 1.5
        elif any(blog in source_name for blog in ['blog', 'personal', 'medium']):
            weight *= 0.6
        
        title = str(article.get('title', ''))
        description = str(article.get('description', ''))
        content = str(article.get('content', ''))
        content_length = len(title + description + content)
        
        if content_length > 300:
            weight *= 1.2
        elif content_length < 100:
            weight *= 0.8
        
        return max(0.2, min(weight, 3.0))
    
    def analyze_company(self, ticker, company_name):
        print(f"ANALYSE POUR {company_name} ({ticker})")
        query = self.build_query(ticker, company_name)
        articles = ArticleFetcher.get_all_articles(query, Config.MAX_ARTICLES_PER_SOURCE)
        
        if not articles:
            print("Aucun article trouve")
            return pd.DataFrame()
        
        analyzed_articles = []
        print(f"{len(articles)} articles trouves:")
        
        for i, article in enumerate(articles, 1):
            title = str(article.get('title', 'Sans titre')).strip()
            description = str(article.get('description', '')).strip()
            content = str(article.get('content', '')).strip()
            
            if content:
                full_text = f"{title}. {content}"
            else:
                full_text = f"{title}. {description}"
            
            source_name = article.get('source', {}).get('name', 'Inconnue')
            article_id = generate_article_id(title, source_name, article.get('publishedAt', ''))
            sentiment = self.analyze_text(full_text)
            weight = self.calculate_weight(article)
            weighted_score = sentiment['score'] * weight
            
            pub_date = "Inconnue"
            if 'publishedAt' in article and article['publishedAt']:
                try:
                    date_str = str(article['publishedAt'])
                    if 'T' in date_str:
                        pub_date = datetime.strptime(date_str, '%Y-%m-dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M')
                    else:
                        pub_date = date_str[:16]
                except:
                    pub_date = date_str[:10] if len(date_str) >= 10 else "Inconnue"
            
            api_source = article.get('api_source', 'unknown')
            analyzed_article = {
                'article_id': article_id,
                'symbol': ticker,
                'company': company_name,
                'date_publication': pub_date,
                'source': str(source_name),
                'api_source': api_source,
                'titre': title[:150] + '...' if len(title) > 150 else title,
                'contenu': full_text[:300] + '...' if len(full_text) > 300 else full_text,
                'score_sentiment': round(sentiment['score'], 3),
                'sentiment': sentiment['label'],
                'confiance': round(sentiment['confidence'], 3),
                'poids': round(weight, 2),
                'score_pondere': round(weighted_score, 3),
                'url': article.get('url', ''),
                'date_analyse': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'ajout_date': datetime.now().strftime('%Y-%m-%d')
            }
            analyzed_articles.append(analyzed_article)
            print(f"Article {i}: {title[:60]}...")
            print(f"  Score: {sentiment['score']:.3f} | Sentiment: {sentiment['label']} | Source: {source_name}")
        
        df = pd.DataFrame(analyzed_articles)
        if not df.empty:
            save_company_articles(ticker, df)
            print(f"STATISTIQUES {ticker}:")
            print(f"  Articles trouves: {len(df)}")
            positif = len(df[df['sentiment'] == 'positive'])
            negatif = len(df[df['sentiment'] == 'negative'])
            neutre = len(df) - positif - negatif
            print(f"  Sentiment: {positif} positif, {negatif} negatif, {neutre} neutre")
            print(f"  Score moyen: {df['score_sentiment'].mean():.3f}")
        return df

def test_apis():
    print("TEST DES APIS...")
    if Config.USE_NEWSAPI and Config.NEWSAPI_KEY:
        print(f"Test NewsAPI (cle: {Config.NEWSAPI_KEY[:10]}...)")
        test_url = f"https://newsapi.org/v2/everything?q=apple&apiKey={Config.NEWSAPI_KEY}&pageSize=1"
        try:
            response = requests.get(test_url, timeout=10)
            if response.status_code == 200:
                print("NewsAPI: Connexion OK")
            else:
                print(f"NewsAPI: Erreur {response.status_code}")
        except Exception as e:
            print(f"NewsAPI: {e}")
    else:
        print("NewsAPI desactive")
    
    if Config.USE_GNEWS and Config.GNEWS_API_KEY:
        print(f"Test GNews (cle: {Config.GNEWS_API_KEY[:10]}...)")
        test_url = f"https://gnews.io/api/v4/top-headlines?token={Config.GNEWS_API_KEY}&lang=en&max=1"
        try:
            response = requests.get(test_url, timeout=10)
            if response.status_code == 200:
                print("GNews: Connexion OK")
            else:
                print(f"GNews: Erreur {response.status_code}")
        except Exception as e:
            print(f"GNews: {e}")
    else:
        print("GNews desactive")

# ==================== EXECUTION PRINCIPALE ====================
def main():
    print("DEBUT DE L'ANALYSE DE SENTIMENT")
    print("="*60)
    
    today = datetime.now().strftime('%Y-%m-%d')
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Periode analyse: {Config.DAYS_BACK} derniers jours")
    print(f"Sources: NewsAPI={Config.USE_NEWSAPI}, GNews={Config.USE_GNEWS}")
    print(f"Articles max/source: {Config.MAX_ARTICLES_PER_SOURCE}")
    print(f"Dossier: {Config.ARTICLES_FOLDER}")
    print("="*60)
    
    test_apis()
    analyzer = SentimentAnalyzer()
    
    entreprises = [
        {'ticker': 'AAPL', 'name': 'Apple'},
        {'ticker': 'MSFT', 'name': 'Microsoft'},
        {'ticker': 'TSLA', 'name': 'Tesla'},
        {'ticker': 'NVDA', 'name': 'Nvidia'},
        {'ticker': 'GOOGL', 'name': 'Google'},
        {'ticker': 'AMZN', 'name': 'Amazon'},
        {'ticker': 'META', 'name': 'Meta'},
    ]
    
    all_new_articles = []
    
    for entreprise in entreprises:
        print(f"\n" + "="*60)
        df = analyzer.analyze_company(entreprise['ticker'], entreprise['name'])
        if not df.empty:
            all_new_articles.append(df)
    
    if all_new_articles:
        combined_new = pd.concat(all_new_articles, ignore_index=True)
        
        print(f"\n" + "="*60)
        print(f"RESUME DE LA COLLECTE")
        print("="*60)
        print(f"Total articles collectes aujourd'hui: {len(combined_new)}")
        
        master_df = update_master_file(combined_new)
        
        # ---> APPEL À LA SYNCHRONISATION SUPABASE <---
        if not master_df.empty:
            sync_to_supabase(master_df)

            print(f"\nSTATISTIQUES MASTER:")
            print(f"  Total articles: {len(master_df)}")
            print(f"  Entreprises uniques: {master_df['symbol'].nunique()}")
            
            print(f"\nARTICLES PAR ENTREPRISE:")
            company_stats = master_df['symbol'].value_counts()
            for company, count in company_stats.items():
                company_name = next((e['name'] for e in entreprises if e['ticker'] == company), company)
                print(f"  {company} ({company_name}): {count} articles")

    print("\n" + "="*60)
    print("ANALYSE TERMINEE")
    print("="*60)

if __name__ == "__main__":
    print("CONFIGURATION DU SCRIPT")
    print("="*60)
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"NewsAPI: {'Active' if Config.USE_NEWSAPI else 'Desactive'}")
    print(f"GNews: {'Active' if Config.USE_GNEWS else 'Desactive'}")
    print(f"Articles/source: {Config.MAX_ARTICLES_PER_SOURCE}")
    print(f"Periode: {Config.DAYS_BACK} jours")
    print(f"Modele: {Config.FINBERT_MODEL}")
    print("="*60)
    
    main()
