"""
generate_streaming_data.py
Script de génération de données réalistes pour StreamVision
Auteur : Équipe Data Engineering
Date : 2025-11-27
"""

import os
import sys
import random
import json
import logging
import io
from datetime import datetime, timedelta, date, time
from decimal import Decimal
from typing import List, Dict, Tuple, Any

import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor
import pandas as pd
import numpy as np
from faker import Faker
from tqdm import tqdm

# ============================================================================
# LOGGING (UTF-8 safe pour la console Windows)
# ============================================================================
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# File handler écrit en UTF-8 (pour stocker les emojis dans le fichier de log sans erreur)
file_handler = logging.FileHandler('data_generation.log', encoding='utf-8')
file_handler.setFormatter(formatter)

# Stream handler: sur Windows la console utilise souvent cp1252 — on la wrap en UTF-8
# pour éviter UnicodeEncodeError lors des messages contenant des emojis.
try:
    stream = sys.stdout
    if hasattr(sys.stdout, "buffer"):
        # TextIOWrapper autour de stdout.buffer permet de forcer l'encodage en UTF-8
        stream = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
except Exception:
    stream = sys.stdout

stream_handler = logging.StreamHandler(stream)
stream_handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Remove any existing handlers to avoid duplication in interactive re-runs
if logger.hasHandlers():
    logger.handlers.clear()
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Configuration PostgreSQL - À MODIFIER SELON VOTRE INSTALLATION
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'streamvision',
    'user': 'postgres',
    'password': '1234'  # ⚠️ METTEZ VOTRE MOT DE PASSE ICI
}

# Constantes métier
COUNTRIES = ['FRA', 'USA', 'DEU', 'ESP', 'GBR', 'ITA', 'CAN', 'AUS', 'JPN', 'KOR', 'BRA', 'MEX', 'IND', 'CHN']
AGE_GROUPS = ['13-17', '18-24', '25-34', '35-44', '45-54', '55+']
SUBSCRIPTION_PLANS = ['free_trial', 'basic', 'standard', 'premium', 'family']
CONTENT_TYPES = ['movie', 'tv_show', 'documentary', 'short_film', 'original']
GENRES = [
    'Action', 'Adventure', 'Animation', 'Comedy', 'Crime', 'Documentary',
    'Drama', 'Family', 'Fantasy', 'History', 'Horror', 'Music', 'Mystery',
    'Romance', 'Science Fiction', 'Thriller', 'War', 'Western'
]
PLATFORMS = ['web', 'mobile_ios', 'mobile_android', 'smart_tv', 'game_console', 'tablet']
DEVICE_TYPES = ['desktop', 'laptop', 'phone', 'tablet', 'tv', 'console']
QUALITIES = ['SD', 'HD', 'Full HD', '4K', 'HDR']
SUBSCRIPTION_EVENTS = ['subscription_start', 'upgrade', 'downgrade', 'cancellation', 'renewal', 'payment_failed']

# Initialisation de Faker avec plusieurs langues
fake = Faker(['fr_FR', 'en_US', 'de_DE', 'es_ES', 'it_IT', 'pt_BR', 'ja_JP', 'ko_KR'])


# ============================================================================
# UTILITAIRES
# ============================================================================

def to_datetime(d):
    """Convertit date -> datetime (00:00:00). Si déjà datetime, renvoie tel quel."""
    if isinstance(d, datetime):
        return d
    if isinstance(d, date):
        return datetime.combine(d, time.min)
    return d


def get_db_connection():
    """Établit une connexion à PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        logger.info("Connexion PostgreSQL établie avec succès")
        return conn
    except Exception as e:
        logger.error(f"Erreur de connexion PostgreSQL: {e}")
        sys.exit(1)

# ============================================================================
# 🚨 FLUSH DATABASE (SUPPRESSION TOTALE DES DONNÉES)
# ============================================================================

def flush_database(conn):
    """
    Supprime TOUTES les données de la base StreamVision.
    Action IRRÉVERSIBLE.
    """
    print("\n" + "⚠️" * 30)
    print("⚠️  DANGER – FLUSH DE LA BASE DE DONNÉES")
    print("⚠️  Cette action SUPPRIME TOUTES LES DONNÉES.")
    print("⚠️  Elle est IRRÉVERSIBLE.")
    print("⚠️" * 30)

    confirm = input("\nTapez exactement 'YES' pour confirmer : ").strip()

    if confirm != "YES":
        print("❌ Opération annulée.")
        return

    tables = [
        "episode_viewing",
        "episodes",
        "search_queries",
        "subscription_events",
        "watchlist",
        "ratings",
        "viewing_sessions",
        "content",
        "users"
    ]

    cursor = conn.cursor()

    try:
        logger.warning("FLUSH DATABASE INITIÉ")

        cursor.execute(
            f"TRUNCATE TABLE {', '.join(tables)} RESTART IDENTITY CASCADE;"
        )

        conn.commit()

        logger.warning("🔥 BASE DE DONNÉES COMPLÈTEMENT VIDÉE")
        print("\n🧹 BASE DE DONNÉES FLUSHÉE AVEC SUCCÈS")

    except Exception as e:
        conn.rollback()
        logger.error("Erreur lors du flush de la base", exc_info=True)
        print(f"\n❌ ERREUR LORS DU FLUSH : {e}")

    finally:
        cursor.close()



def generate_realistic_movie_titles(count: int = 100) -> List[str]:
    """Génère des titres de films réalistes"""
    titles = []
    prefixes = ['The', 'A', 'My', 'Our', 'Your', 'His', 'Her', 'Their']
    adjectives = ['Last', 'First', 'Great', 'Big', 'Small', 'Lost', 'Found', 'Hidden', 'Secret']
    nouns = ['Journey', 'Adventure', 'Dream', 'Night', 'Day', 'Love', 'War', 'Peace', 'Hope']
    suffixes = ['Returns', 'Begins', 'Ends', 'Rises', 'Falls', 'Lives', 'Dies']

    for _ in range(count):
        r = random.random()
        if r < 0.3:
            title = f"{random.choice(adjectives)} {random.choice(nouns)}"
        elif r < 0.6:
            title = f"{random.choice(prefixes)} {random.choice(adjectives)} {random.choice(nouns)}"
        else:
            title = f"{random.choice(nouns)} of the {random.choice(nouns)}"

        if random.random() < 0.2:
            title = f"{title}: {random.choice(suffixes)}"

        titles.append(title)

    return titles


def calculate_age_group(birth_year: int) -> str:
    """Calcule le groupe d'âge basé sur l'année de naissance"""
    current_year = datetime.now().year
    age = current_year - birth_year

    if age < 13:
        return '13-17'  # Pour la démo, on considère que tous ont au moins 13 ans
    elif age <= 17:
        return '13-17'
    elif age <= 24:
        return '18-24'
    elif age <= 34:
        return '25-34'
    elif age <= 44:
        return '35-44'
    elif age <= 54:
        return '45-54'
    else:
        return '55+'


# ============================================================================
# GÉNÉRATION DES DONNÉES
# ============================================================================

def generate_users(conn, n_users: int = 10000):
    """Génère des utilisateurs réalistes pour une plateforme de streaming"""
    logger.info(f"Début de la génération de {n_users} utilisateurs...")

    cursor = conn.cursor()
    users_data = []

    # Titres de films réalistes pour les usernames (non utilisés directement mais prêts)
    movie_titles = generate_realistic_movie_titles(500)

    # Progress bar
    pbar = tqdm(total=n_users, desc="Génération des utilisateurs", unit="user")

    for i in range(n_users):
        # Informations de base
        email = fake.unique.email()
        username = f"{fake.user_name()}_{random.randint(100, 999)}"
        first_name = fake.first_name()
        last_name = fake.last_name()
        country = random.choice(COUNTRIES)

        # Calcul de l'âge
        birth_year = random.randint(1950, 2010)
        age_group = calculate_age_group(birth_year)

        # Abonnement
        subscription_plan = random.choice(SUBSCRIPTION_PLANS)

        # Dates d'abonnement (éviter les chaînes de parse de faker en passant des objets date)
        subscription_start = fake.date_between(
            start_date=date.today() - timedelta(days=365),
            end_date=date.today()
        )

        if subscription_plan == 'free_trial':
            # Essai gratuit de 7 à 30 jours
            subscription_end = subscription_start + timedelta(days=random.randint(7, 30))
        else:
            # Abonnement payant de 1 mois à 2 ans
            subscription_end = subscription_start + timedelta(days=random.randint(30, 730))

        # Dates de création et dernière connexion
        created_at = fake.date_time_between(
            start_date=to_datetime(subscription_start - timedelta(days=7)),
            end_date=to_datetime(subscription_start)
        )

        # Dernière connexion (plus récente que la création)
        if random.random() < 0.8:  # 80% des utilisateurs se sont connectés récemment
            last_login = fake.date_time_between(
                start_date=datetime.now() - timedelta(days=30),
                end_date=datetime.now()
            )
        else:
            last_login = None  # Utilisateurs inactifs

        # Statut actif
        is_active = random.random() < 0.85  # 85% d'utilisateurs actifs

        # Méthode de paiement
        payment_methods = ['credit_card', 'paypal', 'apple_pay', 'google_pay', 'bank_transfer']
        payment_method = random.choice(payment_methods) if subscription_plan != 'free_trial' else None

        # Préférence d'appareil
        device_preference = random.choice(DEVICE_TYPES)

        user_record = (
            email, username, first_name, last_name, country, age_group,
            subscription_plan, subscription_start, subscription_end,
            created_at, last_login, is_active, payment_method, device_preference
        )

        users_data.append(user_record)

        # Insertion par lots de 1000
        if len(users_data) >= 1000:
            query = """
                INSERT INTO users (
                    email, username, first_name, last_name, country, age_group,
                    subscription_plan, subscription_start, subscription_end,
                    created_at, last_login, is_active, payment_method, device_preference
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            execute_batch(cursor, query, users_data)
            users_data = []

        pbar.update(1)

    # Insertion des données restantes
    if users_data:
        query = """
            INSERT INTO users (
                email, username, first_name, last_name, country, age_group,
                subscription_plan, subscription_start, subscription_end,
                created_at, last_login, is_active, payment_method, device_preference
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        execute_batch(cursor, query, users_data)

    pbar.close()
    conn.commit()
    cursor.close()
    logger.info(f"✅ {n_users} utilisateurs générés avec succès")


def generate_content(conn, n_content: int = 5000):
    """Génère du contenu vidéo (films, séries, documentaires)"""
    logger.info(f"Début de la génération de {n_content} contenus...")

    cursor = conn.cursor()
    content_data = []

    # Titres de films réalistes
    movie_titles = generate_realistic_movie_titles(n_content + 1000)

    # Noms de réalisateurs célèbres (fictifs pour la démo)
    directors = [
        'Christopher Nolan', 'Steven Spielberg', 'Martin Scorsese', 'Quentin Tarantino',
        'James Cameron', 'David Fincher', 'Ridley Scott', 'Tim Burton', 'Wes Anderson',
        'Alfred Hitchcock', 'Stanley Kubrick', 'Francis Ford Coppola', 'George Lucas',
        'Peter Jackson', 'Guillermo del Toro', 'Hayao Miyazaki', 'Bong Joon-ho',
        'Denis Villeneuve', 'Ava DuVernay', 'Greta Gerwig', 'Jordan Peele'
    ]

    # Acteurs principaux
    actors = [
        'Leonardo DiCaprio', 'Meryl Streep', 'Tom Hanks', 'Denzel Washington',
        'Jennifer Lawrence', 'Robert Downey Jr.', 'Scarlett Johansson', 'Brad Pitt',
        'Angelina Jolie', 'Johnny Depp', 'Emma Stone', 'Ryan Gosling', 'Margot Robbie',
        'Will Smith', 'Natalie Portman', 'Christian Bale', 'Anne Hathaway', 'Matt Damon',
        'Cate Blanchett', 'Joaquin Phoenix', 'Viola Davis', 'Samuel L. Jackson',
        'Morgan Freeman', 'Keanu Reeves', 'Charlize Theron'
    ]

    # Progress bar
    pbar = tqdm(total=n_content, desc="Génération des contenus", unit="content")

    for i in range(n_content):
        # Titre
        title = movie_titles[i]

        # Type de contenu
        content_type = random.choice(CONTENT_TYPES)

        # Genres (1 à 3 genres par contenu)
        if content_type == 'documentary':
            main_genre = 'Documentary'
            subgenres = random.sample([g for g in GENRES if g != 'Documentary'], random.randint(0, 2))
        else:
            main_genre = random.choice([g for g in GENRES if g != 'Documentary'])
            subgenres = random.sample([g for g in GENRES if g not in [main_genre, 'Documentary']], random.randint(0, 2))

        genre = main_genre
        subgenre = ', '.join(subgenres) if subgenres else None

        # Année de sortie
        if content_type == 'tv_show':
            release_year = random.randint(1990, 2024)
        else:
            release_year = random.randint(1970, 2024)

        # Durée selon le type
        if content_type == 'movie':
            duration = random.randint(80, 180)  # 1h20 à 3h
        elif content_type == 'tv_show':
            duration = random.randint(40, 60)  # Durée par épisode
        elif content_type == 'documentary':
            duration = random.randint(50, 120)  # 50min à 2h
        else:  # short_film ou original
            duration = random.randint(10, 50)

        # Réalisateur et acteur
        director = random.choice(directors)
        main_actor = random.choice(actors)

        # Note IMDB réaliste (distribution normale)
        imdb_mean = 7.0 if content_type == 'original' else 6.5
        imdb_std = 1.5
        imdb_rating = np.random.normal(imdb_mean, imdb_std)
        imdb_rating = max(1.0, min(10.0, round(imdb_rating, 1)))  # Borné entre 1 et 10

        # Classification
        content_ratings = ['G', 'PG', 'PG-13', 'R', 'NC-17']
        weights = [0.1, 0.2, 0.4, 0.25, 0.05]
        content_rating = random.choices(content_ratings, weights=weights)[0]

        # Contenu original (20% de chance)
        is_original = random.random() < 0.2

        # Date d'ajout à la plateforme (passer des objets date)
        added_date = fake.date_between(
            start_date=date(release_year, 1, 1),
            end_date=date.today()
        )

        # Pays disponibles (3 à 10 pays aléatoires)
        available_countries = random.sample(COUNTRIES, random.randint(3, 10))

        # Tags (mots-clés)
        tags = []
        if main_genre:
            tags.append(main_genre.lower())
        if subgenres:
            tags.extend([g.lower() for g in subgenres])
        tags.extend(random.sample(['popular', 'new', 'award', 'oscar', 'bestseller', 'trending'], 2))

        # Description
        description = fake.text(max_nb_chars=200)

        content_record = (
            title, content_type, genre, subgenre, release_year, duration,
            director, main_actor, float(imdb_rating), content_rating,
            is_original, added_date, available_countries, tags, description
        )

        content_data.append(content_record)

        # Insertion par lots de 500
        if len(content_data) >= 500:
            query = """
                INSERT INTO content (
                    title, content_type, genre, subgenre, release_year, duration_minutes,
                    director, main_actor, imdb_rating, content_rating, is_original,
                    added_date, available_countries, tags, description
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            execute_batch(cursor, query, content_data)
            content_data = []

        pbar.update(1)

    # Insertion des données restantes
    if content_data:
        query = """
            INSERT INTO content (
                title, content_type, genre, subgenre, release_year, duration_minutes,
                director, main_actor, imdb_rating, content_rating, is_original,
                added_date, available_countries, tags, description
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        execute_batch(cursor, query, content_data)

    pbar.close()
    conn.commit()
    cursor.close()
    logger.info(f"✅ {n_content} contenus générés avec succès")


def generate_viewing_sessions(conn, n_sessions: int = 100000):
    """Génère des sessions de visionnage réalistes"""
    logger.info(f"Début de la génération de {n_sessions} sessions de visionnage...")

    cursor = conn.cursor()

    # Récupération des IDs utilisateurs (actifs seulement)
    cursor.execute("SELECT id FROM users WHERE is_active = TRUE LIMIT 5000")
    user_ids = [row[0] for row in cursor.fetchall()]

    # Récupération des IDs de contenu
    cursor.execute("SELECT id, duration_minutes FROM content")
    content_data = cursor.fetchall()

    if not user_ids or not content_data:
        logger.error("Pas d'utilisateurs ou de contenus. Générez-les d'abord.")
        return

    sessions_data = []

    # Progress bar
    pbar = tqdm(total=n_sessions, desc="Génération des sessions", unit="session")

    for i in range(n_sessions):
        # Utilisateur aléatoire
        user_id = random.choice(user_ids)

        # Contenu aléatoire
        content_id, content_duration = random.choice(content_data)

        # Date de la session (derniers 6 mois) - utiliser des datetimes explicites
        session_start = fake.date_time_between(
            start_date=datetime.now() - timedelta(days=180),
            end_date=datetime.now()
        )

        # Durée de visionnage (entre 5 minutes et la durée totale du contenu)
        max_watch_seconds = min(content_duration * 60, 7200)  # Max 2 heures
        min_watch_seconds = 300  # Min 5 minutes

        duration_seconds = random.randint(min_watch_seconds, max_watch_seconds)

        # Heure de fin
        session_end = session_start + timedelta(seconds=duration_seconds)

        # Taux de complétion
        completion_rate = min(duration_seconds / (content_duration * 60), 1.0) * 100

        # Plateforme et appareil
        platform = random.choice(PLATFORMS)

        # Mapping plateforme -> appareil
        platform_device_map = {
            'web': ['desktop', 'laptop'],
            'mobile_ios': ['phone', 'tablet'],
            'mobile_android': ['phone', 'tablet'],
            'smart_tv': ['tv'],
            'game_console': ['console'],
            'tablet': ['tablet']
        }
        device_type = random.choice(platform_device_map[platform])

        # Qualité (plus probable pour les appareils récents)
        if device_type in ['tv', 'desktop', 'laptop']:
            quality_weights = [0.1, 0.3, 0.4, 0.15, 0.05]  # Plus de Full HD
        else:
            quality_weights = [0.2, 0.5, 0.2, 0.05, 0.05]  # Plus de HD sur mobile

        quality = random.choices(QUALITIES, weights=quality_weights)[0]

        # Nombre de buffering (0-5)
        buffering_count = random.randint(0, 5)

        # Bitrate moyen (en kbps)
        quality_bitrate = {
            'SD': 1000,
            'HD': 3000,
            'Full HD': 6000,
            '4K': 15000,
            'HDR': 20000
        }
        avg_bitrate = quality_bitrate[quality] + random.randint(-500, 500)

        # Ville (optionnelle, 70% de chances)
        if random.random() < 0.7:
            city = fake.city()
        else:
            city = None

        # Adresse IP (optionnelle)
        ip_address = fake.ipv4() if random.random() < 0.5 else None

        session_record = (
            user_id, content_id, session_start, session_end, duration_seconds,
            platform, device_type, quality, round(completion_rate, 2),
            buffering_count, avg_bitrate, city, ip_address
        )

        sessions_data.append(session_record)

        # Insertion par lots de 2000
        if len(sessions_data) >= 2000:
            query = """
                INSERT INTO viewing_sessions (
                    user_id, content_id, session_start, session_end, duration_seconds,
                    platform, device_type, quality, completion_rate, buffering_count,
                    avg_bitrate, city, ip_address
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            execute_batch(cursor, query, sessions_data)
            sessions_data = []

        pbar.update(1)

    # Insertion des données restantes
    if sessions_data:
        query = """
            INSERT INTO viewing_sessions (
                user_id, content_id, session_start, session_end, duration_seconds,
                platform, device_type, quality, completion_rate, buffering_count,
                avg_bitrate, city, ip_address
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        execute_batch(cursor, query, sessions_data)

    pbar.close()
    conn.commit()
    cursor.close()
    logger.info(f"✅ {n_sessions} sessions de visionnage générées avec succès")


def generate_ratings(conn, n_ratings: int = 30000):
    """Génère des évaluations réalistes"""
    logger.info(f"Début de la génération de {n_ratings} évaluations...")

    cursor = conn.cursor()

    # Récupération des IDs utilisateurs (qui ont regardé du contenu)
    cursor.execute("""
        SELECT DISTINCT user_id 
        FROM viewing_sessions 
        WHERE user_id IS NOT NULL 
        LIMIT 3000
    """)
    user_ids = [row[0] for row in cursor.fetchall()]

    # Récupération des IDs de contenu
    cursor.execute("SELECT id FROM content LIMIT 1000")
    content_ids = [row[0] for row in cursor.fetchall()]

    ratings_data = []

    # Progress bar
    pbar = tqdm(total=n_ratings, desc="Génération des évaluations", unit="rating")

    # Garder une trace des paires (user_id, content_id) pour éviter les doublons
    user_content_pairs = set()

    for i in range(n_ratings):
        # Sélectionner un utilisateur et un contenu
        user_id = random.choice(user_ids)
        content_id = random.choice(content_ids)

        # Vérifier si cette paire existe déjà
        pair = (user_id, content_id)
        if pair in user_content_pairs:
            continue  # Passer à l'itération suivante

        user_content_pairs.add(pair)

        # Note (distribution normale centrée sur 3.5/5)
        rating_raw = np.random.normal(3.5, 1.0)
        rating = max(1, min(5, round(rating_raw)))

        # Date d'évaluation (après la date d'ajout du contenu) - utiliser des datetimes explicites
        rating_date = fake.date_time_between(
            start_date=datetime.now() - timedelta(days=180),
            end_date=datetime.now()
        )

        # Texte de critique (30% de chances)
        if random.random() < 0.3:
            review_text = fake.text(max_nb_chars=200)
        else:
            review_text = None

        # Nombre de "utile" (0-50)
        helpful_count = random.randint(0, 50)

        rating_record = (
            user_id, content_id, rating, rating_date, review_text, helpful_count
        )

        ratings_data.append(rating_record)

        # Insertion par lots de 1000
        if len(ratings_data) >= 1000:
            query = """
                INSERT INTO ratings (
                    user_id, content_id, rating, rating_date, review_text, helpful_count
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """
            execute_batch(cursor, query, ratings_data)
            ratings_data = []

        pbar.update(1)

    # Insertion des données restantes
    if ratings_data:
        query = """
            INSERT INTO ratings (
                user_id, content_id, rating, rating_date, review_text, helpful_count
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """
        execute_batch(cursor, query, ratings_data)

    pbar.close()
    conn.commit()
    cursor.close()
    logger.info(f"✅ {n_ratings} évaluations générées avec succès")


def generate_watchlist(conn, n_items: int = 20000):
    """Génère des entrées de liste de visionnage"""
    logger.info(f"Début de la génération de {n_items} entrées de liste de visionnage...")

    cursor = conn.cursor()

    # Récupération des IDs utilisateurs
    cursor.execute("SELECT id FROM users WHERE is_active = TRUE LIMIT 3000")
    user_ids = [row[0] for row in cursor.fetchall()]

    # Récupération des IDs de contenu
    cursor.execute("SELECT id FROM content LIMIT 1500")
    content_ids = [row[0] for row in cursor.fetchall()]

    watchlist_data = []

    # Progress bar
    pbar = tqdm(total=n_items, desc="Génération de la watchlist", unit="item")

    # Garder une trace des paires (user_id, content_id)
    user_content_pairs = set()

    for i in range(n_items):
        user_id = random.choice(user_ids)
        content_id = random.choice(content_ids)

        # Vérifier si cette paire existe déjà
        pair = (user_id, content_id)
        if pair in user_content_pairs:
            continue

        user_content_pairs.add(pair)

        # Date d'ajout
        added_date = fake.date_time_between(
            start_date=datetime.now() - timedelta(days=90),
            end_date=datetime.now()
        )

        # Statut "regardé" (40% de chances)
        watched = random.random() < 0.4

        if watched:
            watched_date = added_date + timedelta(days=random.randint(1, 30))
        else:
            watched_date = None

        watchlist_record = (
            user_id, content_id, added_date, watched, watched_date
        )

        watchlist_data.append(watchlist_record)

        # Insertion par lots de 1000
        if len(watchlist_data) >= 1000:
            query = """
                INSERT INTO watchlist (
                    user_id, content_id, added_date, watched, watched_date
                ) VALUES (%s, %s, %s, %s, %s)
            """
            execute_batch(cursor, query, watchlist_data)
            watchlist_data = []

        pbar.update(1)

    # Insertion des données restantes
    if watchlist_data:
        query = """
            INSERT INTO watchlist (
                user_id, content_id, added_date, watched, watched_date
            ) VALUES (%s, %s, %s, %s, %s)
        """
        execute_batch(cursor, query, watchlist_data)

    pbar.close()
    conn.commit()
    cursor.close()
    logger.info(f"✅ {n_items} entrées de watchlist générées avec succès")


def generate_subscription_events(conn, n_events: int = 15000):
    """Génère des événements d'abonnement"""
    logger.info(f"Début de la génération de {n_events} événements d'abonnement...")

    cursor = conn.cursor()

    # Récupération des IDs utilisateurs
    cursor.execute("SELECT id, subscription_plan FROM users LIMIT 4000")
    users = cursor.fetchall()

    events_data = []

    # Progress bar
    pbar = tqdm(total=n_events, desc="Génération des événements d'abonnement", unit="event")

    # Garde-fou pour éviter trop d'événements par utilisateur
    user_event_count = {}

    for i in range(n_events):
        user_id, current_plan = random.choice(users)

        # Limiter à 5 événements par utilisateur
        if user_id not in user_event_count:
            user_event_count[user_id] = 0

        if user_event_count[user_id] >= 5:
            continue

        user_event_count[user_id] += 1

        # Type d'événement
        if current_plan == 'free_trial':
            event_types = ['subscription_start', 'upgrade', 'cancellation']
            weights = [0.6, 0.3, 0.1]
        else:
            event_types = ['renewal', 'upgrade', 'downgrade', 'cancellation', 'payment_failed']
            weights = [0.5, 0.2, 0.1, 0.1, 0.1]

        event_type = random.choices(event_types, weights=weights)[0]

        # Date de l'événement (dans les 2 dernières années)
        event_date = fake.date_time_between(
            start_date=datetime.now() - timedelta(days=730),
            end_date=datetime.now()
        )

        # Plans précédents et nouveaux
        if event_type == 'subscription_start':
            previous_plan = None
            new_plan = 'free_trial'
        elif event_type in ['upgrade', 'downgrade']:
            previous_plan = current_plan
            possible_plans = [p for p in SUBSCRIPTION_PLANS if p != current_plan]
            new_plan = random.choice(possible_plans)
        elif event_type == 'cancellation':
            previous_plan = current_plan
            new_plan = None
        else:  # renewal ou payment_failed
            previous_plan = current_plan
            new_plan = current_plan

        # Montant (selon le plan)
        plan_prices = {
            'free_trial': 0,
            'basic': 7.99,
            'standard': 10.99,
            'premium': 15.99,
            'family': 19.99
        }

        amount = None
        if new_plan and event_type != 'payment_failed':
            amount = plan_prices.get(new_plan, 0)
            amount = round(amount * random.uniform(0.9, 1.1), 2)

        # Devise
        currency = 'USD'

        # Passerelle de paiement
        payment_gateways = ['stripe', 'paypal', 'apple_pay', 'google_pay']
        payment_gateway = random.choice(payment_gateways) if amount and amount > 0 else None

        # ID de transaction (pour les paiements)
        transaction_id = None
        if amount and amount > 0:
            transaction_id = f"txn_{random.randint(100000000, 999999999)}"

        event_record = (
            user_id, event_type, event_date, previous_plan, new_plan,
            amount, currency, payment_gateway, transaction_id
        )

        events_data.append(event_record)

        # Insertion par lots de 1000
        if len(events_data) >= 1000:
            query = """
                INSERT INTO subscription_events (
                    user_id, event_type, event_date, previous_plan, new_plan,
                    amount, currency, payment_gateway, transaction_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            execute_batch(cursor, query, events_data)
            events_data = []

        pbar.update(1)

    # Insertion des données restantes
    if events_data:
        query = """
            INSERT INTO subscription_events (
                user_id, event_type, event_date, previous_plan, new_plan,
                amount, currency, payment_gateway, transaction_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        execute_batch(cursor, query, events_data)

    pbar.close()
    conn.commit()
    cursor.close()
    logger.info(f"✅ {n_events} événements d'abonnement générés avec succès")


def generate_search_queries(conn, n_queries: int = 25000):
    """Génère des requêtes de recherche"""
    logger.info(f"Début de la génération de {n_queries} requêtes de recherche...")

    cursor = conn.cursor()

    # Récupération des IDs utilisateurs
    cursor.execute("SELECT id FROM users WHERE is_active = TRUE LIMIT 3000")
    user_ids = [row[0] for row in cursor.fetchall()]

    # Récupération des IDs de contenu
    cursor.execute("SELECT id, title, genre FROM content LIMIT 1000")
    content_data = cursor.fetchall()

    # Mots-clés de recherche populaires
    search_keywords = [
        'action', 'comedy', 'drama', 'horror', 'romance', 'sci-fi', 'thriller',
        'documentary', 'animation', 'fantasy', 'adventure', 'crime', 'mystery',
        'new', 'popular', 'trending', 'oscar', 'award', 'best', 'top',
        'movie', 'film', 'series', 'show', 'tv', 'netflix', 'amazon',
        '2023', '2024', '2022', '2021', '2020',
        'christmas', 'halloween', 'summer', 'winter',
        'kid', 'family', 'adult', 'teen'
    ]

    queries_data = []

    # Progress bar
    pbar = tqdm(total=n_queries, desc="Génération des recherches", unit="query")

    for i in range(n_queries):
        # Utilisateur (70% de chances d'être connecté)
        if random.random() < 0.7 and user_ids:
            user_id = random.choice(user_ids)
        else:
            user_id = None

        # Texte de recherche
        r = random.random()
        if r < 0.3 and content_data:
            # Recherche par titre
            _, title, _ = random.choice(content_data)
            query_text = title
        elif r < 0.5:
            # Recherche par genre/mot-clé
            query_text = random.choice(search_keywords)
        else:
            # Recherche combinée
            words = random.sample(search_keywords, random.randint(1, 3))
            query_text = ' '.join(words)

        # Date de recherche
        search_date = fake.date_time_between(
            start_date=datetime.now() - timedelta(days=90),
            end_date=datetime.now()
        )

        # Nombre de résultats
        results_count = random.randint(0, 200)

        # Contenu cliqué (30% de chances)
        clicked_content_id = None
        if random.random() < 0.3 and content_data:
            clicked_content_id = random.choice(content_data)[0]

        # Filtres de recherche (JSON)
        search_filters = None
        if random.random() < 0.4:
            filters = {}
            if random.random() < 0.5:
                filters['genre'] = random.choice(GENRES)
            if random.random() < 0.3:
                filters['year'] = random.randint(2000, 2024)
            if random.random() < 0.2:
                filters['rating'] = random.choice(['G', 'PG', 'PG-13', 'R'])
            if filters:
                search_filters = json.dumps(filters)

        # ID de session
        session_id = f"sess_{random.randint(100000, 999999)}"

        query_record = (
            user_id, query_text, search_date, results_count,
            clicked_content_id, search_filters, session_id
        )

        queries_data.append(query_record)

        # Insertion par lots de 1000
        if len(queries_data) >= 1000:
            query = """
                INSERT INTO search_queries (
                    user_id, query_text, search_date, results_count,
                    clicked_content_id, search_filters, session_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            execute_batch(cursor, query, queries_data)
            queries_data = []

        pbar.update(1)

    # Insertion des données restantes
    if queries_data:
        query = """
            INSERT INTO search_queries (
                user_id, query_text, search_date, results_count,
                clicked_content_id, search_filters, session_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        execute_batch(cursor, query, queries_data)

    pbar.close()
    conn.commit()
    cursor.close()
    logger.info(f"✅ {n_queries} requêtes de recherche générées avec succès")


def generate_episodes_and_viewing(conn, n_episodes: int = 5000, n_episode_views: int = 30000):
    """Génère des épisodes (pour les séries) et leur visionnage"""
    logger.info(f"Début de la génération de {n_episodes} épisodes et {n_episode_views} visionnages d'épisodes...")

    cursor = conn.cursor()

    # Récupération des séries TV seulement
    cursor.execute("SELECT id, title FROM content WHERE content_type = 'tv_show' LIMIT 200")
    tv_shows = cursor.fetchall()

    if not tv_shows:
        logger.warning("Aucune série TV trouvée. Génération de quelques séries...")
        # Créer quelques séries TV manuellement
        cursor.execute("""
            INSERT INTO content (title, content_type, genre, release_year, duration_minutes)
            VALUES 
                ('The Adventure Chronicles', 'tv_show', 'Adventure', 2020, 45),
                ('Medical Dreams', 'tv_show', 'Drama', 2018, 50),
                ('Space Explorers', 'tv_show', 'Science Fiction', 2022, 60)
            RETURNING id, title
        """)
        tv_shows = cursor.fetchall()
        conn.commit()

    # ============================================================================
    # ÉTAPE 1 : Génération des épisodes
    # ============================================================================
    logger.info("Génération des épisodes...")
    episodes_data = []

    for tv_show_id, tv_show_title in tv_shows:
        # Nombre de saisons (1 à 5)
        n_seasons = random.randint(1, 5)

        for season in range(1, n_seasons + 1):
            # Nombre d'épisodes par saison (6 à 13)
            n_episodes_per_season = random.randint(6, 13)

            for episode in range(1, n_episodes_per_season + 1):
                # Titre de l'épisode
                episode_titles = [
                    'Pilot', 'Beginnings', 'Endings', 'The Start', 'The Finish',
                    'Unexpected', 'Revelations', 'Secrets', 'Truth', 'Lies',
                    'Alliances', 'Betrayals', 'Hope', 'Despair', 'Love', 'Hate'
                ]
                episode_title = f"Episode {episode}: {random.choice(episode_titles)}"

                # Durée (40-60 minutes)
                duration_minutes = random.randint(40, 60)

                # Date de sortie (derniers ~5 ans)
                release_date = fake.date_between(
                    start_date=date.today() - timedelta(days=5 * 365),
                    end_date=date.today()
                )

                # Réalisateur
                director = fake.name()

                # Note IMDB
                imdb_rating = round(random.uniform(6.0, 9.5), 1)

                # Description
                description = fake.text(max_nb_chars=150)

                episode_record = (
                    tv_show_id, season, episode, episode_title, duration_minutes,
                    release_date, director, float(imdb_rating), description
                )

                episodes_data.append(episode_record)

                # Limiter le nombre total d'épisodes
                if len(episodes_data) >= n_episodes:
                    break

            if len(episodes_data) >= n_episodes:
                break

        if len(episodes_data) >= n_episodes:
            break

    # Insertion des épisodes
    if episodes_data:
        query = """
            INSERT INTO episodes (
                tv_show_id, season_number, episode_number, title, duration_minutes,
                release_date, director, imdb_rating, description
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        execute_batch(cursor, query, episodes_data)

    conn.commit()
    logger.info(f"✅ {len(episodes_data)} épisodes générés avec succès")

    # ============================================================================
    # ÉTAPE 2 : Génération des visionnages d'épisodes
    # ============================================================================
    logger.info("Génération des visionnages d'épisodes...")

    # Récupération des IDs des épisodes
    cursor.execute("SELECT id, duration_minutes FROM episodes LIMIT 1000")
    episodes = cursor.fetchall()

    # Récupération des IDs utilisateurs
    cursor.execute("SELECT id FROM users WHERE is_active = TRUE LIMIT 2000")
    user_ids = [row[0] for row in cursor.fetchall()]

    # Récupération des IDs de sessions de visionnage pour les séries
    cursor.execute("""
        SELECT vs.id, vs.user_id 
        FROM viewing_sessions vs
        JOIN content c ON vs.content_id = c.id
        WHERE c.content_type = 'tv_show'
        LIMIT 5000
    """)
    viewing_sessions_data = cursor.fetchall()

    episode_viewing_data = []

    # Progress bar
    pbar = tqdm(total=min(n_episode_views, 30000), desc="Génération des visionnages d'épisodes", unit="view")

    for i in range(min(n_episode_views, 30000)):
        # Choisir une session de visionnage ou créer une nouvelle
        if viewing_sessions_data and random.random() < 0.7:
            viewing_session_id, user_id = random.choice(viewing_sessions_data)
        else:
            viewing_session_id = None
            user_id = random.choice(user_ids)

        # Choisir un épisode
        episode_id, episode_duration = random.choice(episodes)

        # Heure de début
        start_time = fake.date_time_between(
            start_date=datetime.now() - timedelta(days=90),
            end_date=datetime.now()
        )

        # Durée regardée (entre 5 minutes et la durée totale)
        max_watch = min(episode_duration * 60, 3600)  # Max 1 heure
        duration_watched = random.randint(300, max_watch)  # Min 5 minutes

        # Taux de complétion
        completion_rate = min(duration_watched / (episode_duration * 60), 1.0) * 100

        viewing_record = (
            viewing_session_id, episode_id, user_id, start_time,
            start_time + timedelta(seconds=duration_watched),
            duration_watched, round(completion_rate, 2)
        )

        episode_viewing_data.append(viewing_record)

        # Insertion par lots de 1000
        if len(episode_viewing_data) >= 1000:
            query = """
                INSERT INTO episode_viewing (
                    viewing_session_id, episode_id, user_id, start_time,
                    end_time, duration_watched, completion_rate
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            execute_batch(cursor, query, episode_viewing_data)
            episode_viewing_data = []

        pbar.update(1)

    # Insertion des données restantes
    if episode_viewing_data:
        query = """
            INSERT INTO episode_viewing (
                viewing_session_id, episode_id, user_id, start_time,
                end_time, duration_watched, completion_rate
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        execute_batch(cursor, query, episode_viewing_data)

    pbar.close()
    conn.commit()
    cursor.close()
    logger.info(f"✅ {min(n_episode_views, 30000)} visionnages d'épisodes générés avec succès")


def verify_data(conn):
    """Vérifie et affiche un récapitulatif des données générées"""
    logger.info("Vérification des données générées...")

    cursor = conn.cursor(cursor_factory=RealDictCursor)

    # Requêtes de vérification
    queries = [
        ("Utilisateurs", "SELECT COUNT(*) as count FROM users"),
        ("Utilisateurs actifs", "SELECT COUNT(*) as count FROM users WHERE is_active = TRUE"),
        ("Contenus", "SELECT COUNT(*) as count FROM content"),
        ("Films", "SELECT COUNT(*) as count FROM content WHERE content_type = 'movie'"),
        ("Séries TV", "SELECT COUNT(*) as count FROM content WHERE content_type = 'tv_show'"),
        ("Documentaires", "SELECT COUNT(*) as count FROM content WHERE content_type = 'documentary'"),
        ("Sessions de visionnage", "SELECT COUNT(*) as count FROM viewing_sessions"),
        ("Évaluations", "SELECT COUNT(*) as count FROM ratings"),
        ("Liste de visionnage", "SELECT COUNT(*) as count FROM watchlist"),
        ("Événements d'abonnement", "SELECT COUNT(*) as count FROM subscription_events"),
        ("Requêtes de recherche", "SELECT COUNT(*) as count FROM search_queries"),
        ("Épisodes", "SELECT COUNT(*) as count FROM episodes"),
        ("Visionnages d'épisodes", "SELECT COUNT(*) as count FROM episode_viewing"),
    ]

    print("\n" + "=" * 60)
    print("RÉCAPITULATIF DES DONNÉES GÉNÉRÉES")
    print("=" * 60)

    total_records = 0
    for label, query in queries:
        cursor.execute(query)
        result = cursor.fetchone()
        count = result['count'] if result else 0
        total_records += count
        print(f"{label:30} : {count:>10,}")

    print("-" * 60)
    print(f"{'TOTAL':30} : {total_records:>10,}")
    print("=" * 60)

    # Quelques statistiques supplémentaires
    print("\n📊 Statistiques supplémentaires:")

    # Temps de visionnage total
    cursor.execute("SELECT SUM(duration_seconds) as total_watch_seconds FROM viewing_sessions")
    result = cursor.fetchone()
    total_seconds = result['total_watch_seconds'] or 0
    total_hours = total_seconds / 3600
    print(f"• Temps total de visionnage : {total_hours:,.0f} heures")

    # Note moyenne
    cursor.execute("SELECT AVG(rating) as avg_rating FROM ratings")
    result = cursor.fetchone()
    avg_rating = result['avg_rating'] or 0
    print(f"• Note moyenne : {avg_rating:.2f}/5")

    # Taux de complétion moyen
    cursor.execute("SELECT AVG(completion_rate) as avg_completion FROM viewing_sessions")
    result = cursor.fetchone()
    avg_completion = result['avg_completion'] or 0
    print(f"• Taux de complétion moyen : {avg_completion:.1f}%")

    # Répartition des abonnements
    cursor.execute("""
        SELECT subscription_plan, COUNT(*) as count,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
        FROM users
        GROUP BY subscription_plan
        ORDER BY count DESC
    """)
    print("\n📈 Répartition des plans d'abonnement:")
    for row in cursor.fetchall():
        print(f"  • {row['subscription_plan']:15} : {row['count']:>6,} ({row['percentage']}%)")

    cursor.close()


def main():
    """Point d'entrée principal"""
    print("\n" + "=" * 70)
    print("GÉNÉRATEUR DE DONNÉES STREAMVISION - PLATEFORME DE STREAMING")
    print("=" * 70)

    # Avertissement
    print("\n⚠️  ATTENTION: Cette opération va générer une grande quantité de données.")
    print("   Temps estimé: 5-15 minutes selon votre machine.")

    try:
        # Connexion à la base de données
        conn = get_db_connection()

        print("\n" + "-" * 70)
        print("QUELLES DONNÉES VOULEZ-VOUS GÉNÉRER ?")
        print("-" * 70)
        print("1. ✅ Toutes les données (recommandé)")
        print("2. ⚙️  Personnaliser la génération")
        print("3. 🔍 Vérifier les données existantes")
        print("4. 🧹 FLUSH DATABASE (SUPPRIMER TOUT)")
        print("5. 🚪 Quitter")

        choice = input("\nVotre choix [1-4]: ").strip()

        if choice == "1":
            # Génération complète
            print("\n🎬 Démarrage de la génération complète...")

            generate_users(conn, n_users=10000)
            generate_content(conn, n_content=5000)
            generate_viewing_sessions(conn, n_sessions=100000)
            generate_ratings(conn, n_ratings=30000)
            generate_watchlist(conn, n_items=20000)
            generate_subscription_events(conn, n_events=15000)
            generate_search_queries(conn, n_queries=25000)
            generate_episodes_and_viewing(conn, n_episodes=5000, n_episode_views=30000)

            verify_data(conn)

        elif choice == "2":
            # Génération personnalisée
            print("\n🔧 Génération personnalisée")

            n_users = int(input("Nombre d'utilisateurs [10000]: ") or "10000")
            n_content = int(input("Nombre de contenus [5000]: ") or "5000")
            n_sessions = int(input("Nombre de sessions [100000]: ") or "100000")

            generate_users(conn, n_users)
            generate_content(conn, n_content)
            generate_viewing_sessions(conn, n_sessions)

            # Les autres tables sont optionnelles
            if input("\nGénérer les évaluations? [O/n]: ").strip().lower() != 'n':
                generate_ratings(conn, n_ratings=30000)

            if input("Générer la watchlist? [O/n]: ").strip().lower() != 'n':
                generate_watchlist(conn, n_items=20000)

            verify_data(conn)

        elif choice == "3":
            # Vérification seulement
            verify_data(conn)

        elif choice == "4":
            flush_database(conn)

        elif choice == "5":
            print("\nAu revoir!")
            sys.exit(0)

        else:
            print("\n❌ Choix invalide. Sortie.")
            sys.exit(1)

        # Fermeture de la connexion
        conn.close()
        logger.info("Connexion PostgreSQL fermée")

        print("\n" + "=" * 70)
        print("✅ GÉNÉRATION TERMINÉE AVEC SUCCÈS !")
        print("=" * 70)
        print("\nProchaines étapes:")
        print("1. ✅ PostgreSQL est prêt avec des données réalistes")
        print("2. ➡️  Passez à l'export vers S3")
        print("\n📁 Les logs détaillés sont disponibles dans: data_generation.log")

    except KeyboardInterrupt:
        print("\n\n⏹️  Génération interrompue par l'utilisateur.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Erreur fatale: {e}", exc_info=True)
        print(f"\n❌ ERREUR: {e}")
        print("Consultez le fichier data_generation.log pour plus de détails.")
        sys.exit(1)


if __name__ == "__main__":
    main()