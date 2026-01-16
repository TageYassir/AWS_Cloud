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

    # Enhanced randomization patterns
    patterns = [
        lambda: f"{random.choice(adjectives)} {random.choice(nouns)}",
        lambda: f"{random.choice(prefixes)} {random.choice(adjectives)} {random.choice(nouns)}",
        lambda: f"{random.choice(nouns)} of the {random.choice(nouns)}",
        lambda: f"{random.choice(nouns)} & {random.choice(nouns)}",
        lambda: f"{random.choice(prefixes)} {random.choice(nouns)}",
        lambda: f"{random.choice(adjectives)} {random.choice(nouns)}: {random.choice(suffixes)}",
        lambda: f"{random.choice(nouns)} {random.randint(1, 5)}",
        lambda: f"{random.choice(['Code', 'Project', 'Operation'])} {random.choice(['Alpha', 'Omega', 'Zero', 'X'])}",
    ]

    for _ in range(count):
        # Randomly choose a pattern with varying probabilities
        pattern_weights = [0.15, 0.15, 0.15, 0.1, 0.15, 0.1, 0.1, 0.1]
        title = random.choices(patterns, weights=pattern_weights)[0]()

        # Occasionally add a subtitle (more randomized)
        if random.random() < 0.25:
            subtitle_options = [
                f": {random.choice(suffixes)}",
                f" - {random.choice(['The Beginning', 'The End', 'Redemption', 'Legacy'])}",
                f" ({random.choice(['Part I', 'Part II', 'Final Chapter', 'Reboot'])}"
            ]
            title += random.choice(subtitle_options)

        titles.append(title)

    # Shuffle titles to avoid any sequential patterns
    random.shuffle(titles)
    return titles


def calculate_age_group(birth_year: int) -> str:
    """Calcule le groupe d'âge basé sur l'année de naissance"""
    current_year = datetime.now().year
    age = current_year - birth_year

    if age < 13:
        return random.choice(['13-17', '18-24'])  # More random for demo purposes
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
        # More variation for 55+
        if random.random() < 0.3:
            return random.choice(['45-54', '55+'])
        return '55+'


# ============================================================================
# GÉNÉRATION DES DONNÉES
# ============================================================================

def generate_users(conn, n_users: int = 10000):
    """Génère des utilisateurs réalistes pour une plateforme de streaming"""
    logger.info(f"Début de la génération de {n_users} utilisateurs...")

    cursor = conn.cursor()
    users_data = []

    # Titres de films réalistes pour les usernames
    movie_titles = generate_realistic_movie_titles(500)

    # Progress bar
    pbar = tqdm(total=n_users, desc="Génération des utilisateurs", unit="user")

    for i in range(n_users):
        # More random email patterns
        email_patterns = [
            lambda: fake.unique.email(),
            lambda: f"{fake.user_name()}{random.randint(10, 999)}@{fake.free_email_domain()}",
            lambda: f"{fake.first_name().lower()}.{fake.last_name().lower()}{random.choice(['', str(random.randint(1, 99))])}@{fake.domain_name()}"
        ]
        email = random.choice(email_patterns)()

        # More varied usernames
        username_patterns = [
            f"{fake.user_name()}_{random.randint(100, 9999)}",
            f"{fake.first_name()}{fake.last_name()}{random.randint(1, 99)}",
            f"{random.choice(movie_titles).replace(' ', '_').lower()}{random.randint(1, 999)}",
            f"{fake.word()}{random.choice(['_', '.', ''])}{fake.word()}{random.randint(10, 999)}"
        ]
        username = random.choice(username_patterns)

        first_name = fake.first_name()
        last_name = fake.last_name()

        # Country with more realistic distribution
        country_weights = [0.3, 0.25, 0.1, 0.08, 0.07, 0.06, 0.04, 0.03, 0.02, 0.02, 0.01, 0.01, 0.005, 0.005]
        country = random.choices(COUNTRIES, weights=country_weights)[0]

        # More varied birth years
        year_weights = {
            1950: 0.02, 1960: 0.05, 1970: 0.1, 1980: 0.15,
            1990: 0.25, 2000: 0.3, 2010: 0.13
        }
        birth_year = random.choices(
            list(year_weights.keys()),
            weights=list(year_weights.values())
        )[0] + random.randint(0, 9)

        age_group = calculate_age_group(birth_year)

        # More realistic subscription plan distribution
        plan_weights = [0.15, 0.2, 0.3, 0.25, 0.1]  # free_trial, basic, standard, premium, family
        subscription_plan = random.choices(SUBSCRIPTION_PLANS, weights=plan_weights)[0]

        # More varied subscription dates
        subscription_start = fake.date_between(
            start_date=date.today() - timedelta(days=random.randint(30, 730)),
            end_date=date.today() - timedelta(days=random.randint(0, 30))
        )

        if subscription_plan == 'free_trial':
            subscription_end = subscription_start + timedelta(
                days=random.choices([7, 14, 30], weights=[0.4, 0.4, 0.2])[0]
            )
        else:
            # More varied subscription durations
            duration_options = [
                timedelta(days=30),  # 1 month
                timedelta(days=90),  # 3 months
                timedelta(days=180),  # 6 months
                timedelta(days=365),  # 1 year
                timedelta(days=730),  # 2 years
            ]
            duration_weights = [0.4, 0.25, 0.15, 0.15, 0.05]
            subscription_end = subscription_start + random.choices(
                duration_options, weights=duration_weights
            )[0]

        # More varied creation dates
        created_at = fake.date_time_between(
            start_date=to_datetime(subscription_start) - timedelta(days=random.randint(1, 60)),
            end_date=to_datetime(subscription_start)
        )

        # More realistic last login patterns
        if random.random() < 0.75:  # 75% of users logged in recently
            last_login_days_ago = np.random.exponential(scale=14)  # Exponential distribution
            last_login_days_ago = min(max(1, last_login_days_ago), 90)  # Clamp between 1-90 days
            last_login = datetime.now() - timedelta(days=last_login_days_ago)
        else:
            last_login = None

        # More realistic active status
        if last_login:
            days_since_login = (datetime.now() - last_login).days
            is_active = days_since_login < random.randint(30, 90)
        else:
            is_active = random.random() < 0.3

        # Payment method with country-specific preferences
        payment_methods = ['credit_card', 'paypal', 'apple_pay', 'google_pay', 'bank_transfer']
        payment_weights = {
            'USA': [0.6, 0.2, 0.1, 0.05, 0.05],
            'FRA': [0.5, 0.3, 0.05, 0.05, 0.1],
            'DEU': [0.4, 0.3, 0.05, 0.05, 0.2],
            'GBR': [0.55, 0.25, 0.1, 0.05, 0.05],
            'default': [0.5, 0.3, 0.1, 0.05, 0.05]
        }

        if subscription_plan != 'free_trial':
            weights = payment_weights.get(country, payment_weights['default'])
            payment_method = random.choices(payment_methods, weights=weights)[0]
        else:
            payment_method = None

        # Device preference with more variation
        device_weights = [0.3, 0.25, 0.2, 0.15, 0.05, 0.05]  # desktop, laptop, phone, tablet, tv, console
        device_preference = random.choices(DEVICE_TYPES, weights=device_weights)[0]

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
        # Titre avec more variation
        title = movie_titles[i % len(movie_titles)]

        # Occasionally add year to title
        if random.random() < 0.1:
            title = f"{title} ({random.randint(1990, 2024)})"

        # Content type with more realistic distribution
        content_type_weights = [0.4, 0.3, 0.15, 0.1, 0.05]  # movie, tv_show, documentary, short_film, original
        content_type = random.choices(CONTENT_TYPES, weights=content_type_weights)[0]

        # Genres with more varied combinations
        if content_type == 'documentary':
            main_genre = 'Documentary'
            subgenres = random.sample(
                [g for g in GENRES if g != 'Documentary'],
                random.choices([0, 1, 2], weights=[0.3, 0.5, 0.2])[0]
            )
        else:
            genre_weights = [
                0.12, 0.1, 0.08, 0.15, 0.08,  # Action, Adventure, Animation, Comedy, Crime
                0, 0.12, 0.05, 0.04, 0.03,  # Documentary, Drama, Family, Fantasy, History
                0.07, 0.02, 0.04, 0.06, 0.09,  # Horror, Music, Mystery, Romance, Sci-Fi
                0.03, 0.01, 0.01  # Thriller, War, Western
            ]
            main_genre = random.choices(GENRES, weights=genre_weights)[0]
            subgenres = random.sample(
                [g for g in GENRES if g not in [main_genre, 'Documentary']],
                random.choices([0, 1, 2, 3], weights=[0.2, 0.4, 0.3, 0.1])[0]
            )

        genre = main_genre
        subgenre = ', '.join(subgenres) if subgenres else None

        # Release year with more realistic distribution
        if content_type == 'tv_show':
            release_year = random.choices(
                range(1990, 2025),
                weights=[0.01] * 10 + [0.02] * 10 + [0.03] * 10 + [0.04] * 5  # More recent years more likely
            )[0]
        else:
            release_year = random.choices(
                range(1970, 2025),
                weights=[0.005] * 20 + [0.01] * 10 + [0.02] * 10 + [0.03] * 15  # More recent years more likely
            )[0]

        # Duration with more variation
        if content_type == 'movie':
            duration = random.choices(
                range(80, 181),
                weights=[0.1] * 20 + [0.15] * 30 + [0.1] * 31 + [0.05] * 20  # More common durations
            )[0]
        elif content_type == 'tv_show':
            duration = random.choices([20, 30, 40, 45, 50, 55, 60], weights=[0.05, 0.1, 0.2, 0.3, 0.2, 0.1, 0.05])[0]
        elif content_type == 'documentary':
            duration = random.randint(45, 120)
        else:
            duration = random.randint(5, 50)

        # Director and actor with more variation
        director = random.choice(directors)
        main_actor = random.choice(actors)

        # Sometimes have multiple main actors
        if random.random() < 0.3:
            main_actor = f"{main_actor}, {random.choice([a for a in actors if a != main_actor])}"

        # More realistic IMDB rating distribution
        if content_type == 'original':
            imdb_mean = 7.0
            imdb_std = 1.2
        elif content_type == 'documentary':
            imdb_mean = 7.2
            imdb_std = 0.8
        else:
            imdb_mean = random.uniform(6.0, 7.5)
            imdb_std = random.uniform(1.0, 1.8)

        imdb_rating = np.random.normal(imdb_mean, imdb_std)
        imdb_rating = max(1.0, min(10.0, round(imdb_rating, 1)))

        # Content rating with more realistic distribution
        content_ratings = ['G', 'PG', 'PG-13', 'R', 'NC-17']
        rating_weights = {
            'movie': [0.05, 0.15, 0.4, 0.35, 0.05],
            'tv_show': [0.1, 0.25, 0.45, 0.15, 0.05],
            'documentary': [0.2, 0.3, 0.3, 0.15, 0.05],
            'default': [0.1, 0.2, 0.4, 0.25, 0.05]
        }
        weights = rating_weights.get(content_type, rating_weights['default'])
        content_rating = random.choices(content_ratings, weights=weights)[0]

        # More varied original content probability
        is_original_weights = {
            'movie': 0.15,
            'tv_show': 0.25,
            'documentary': 0.1,
            'short_film': 0.4,
            'original': 1.0
        }
        is_original = random.random() < is_original_weights.get(content_type, 0.2)

        # Added date with more variation
        added_date = fake.date_between(
            start_date=date(release_year, 1, 1),
            end_date=date.today() - timedelta(days=random.randint(0, 365))
        )

        # Available countries with more variation
        country_count = random.choices([3, 5, 8, 10, 15], weights=[0.1, 0.3, 0.4, 0.15, 0.05])[0]
        available_countries = random.sample(COUNTRIES, min(country_count, len(COUNTRIES)))

        # Tags with more variety
        tags = []
        if main_genre:
            tags.append(main_genre.lower())
        if subgenres:
            tags.extend([g.lower() for g in subgenres])

        tag_categories = {
            'popularity': ['popular', 'trending', 'bestseller', 'viral', 'hit'],
            'quality': ['award', 'oscar', 'emmy', 'critics', 'masterpiece'],
            'time': ['new', 'recent', 'classic', 'old', 'retro'],
            'mood': ['funny', 'sad', 'exciting', 'scary', 'romantic']
        }

        # Add 2-4 random tags from different categories
        for _ in range(random.randint(2, 4)):
            category = random.choice(list(tag_categories.keys()))
            tag = random.choice(tag_categories[category])
            if tag not in tags:
                tags.append(tag)

        # Description with varying length
        description_length = random.choices(['short', 'medium', 'long'], weights=[0.3, 0.5, 0.2])[0]
        if description_length == 'short':
            description = fake.sentence()
        elif description_length == 'medium':
            description = fake.text(max_nb_chars=150)
        else:
            description = fake.text(max_nb_chars=300)

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
        # User selection with weighted probability (some users watch more)
        user_weights = np.random.exponential(scale=1.0, size=len(user_ids))
        user_weights = user_weights / user_weights.sum()
        user_id = random.choices(user_ids, weights=user_weights)[0]

        # Content selection with popularity bias
        content_id, content_duration = random.choice(content_data)

        # Some content is more popular
        if random.random() < 0.3:  # 30% chance to pick from "popular" content
            cursor.execute("SELECT id, duration_minutes FROM content ORDER BY imdb_rating DESC LIMIT 100")
            popular_content = cursor.fetchall()
            if popular_content:
                content_id, content_duration = random.choice(popular_content)

        # Session time with more realistic patterns
        # More sessions in evening and weekend
        hour = random.randint(0, 23)
        day_of_week = random.randint(0, 6)  # 0=Monday, 6=Sunday

        # Time distribution weights
        if day_of_week < 5:  # Weekday
            time_weights = [0.01] * 6 + [0.02] * 4 + [0.03] * 4 + [0.05] * 4 + [0.08] * 4 + [0.04] * 2
        else:  # Weekend
            time_weights = [0.02] * 6 + [0.04] * 4 + [0.06] * 4 + [0.08] * 4 + [0.1] * 4 + [0.06] * 2

        # Create session start with time bias
        days_ago = np.random.exponential(scale=30)  # More recent sessions
        days_ago = min(max(1, days_ago), 180)

        session_start = datetime.now() - timedelta(days=days_ago)

        # Set random hour based on weights
        hour = random.choices(range(24), weights=time_weights)[0]
        session_start = session_start.replace(
            hour=hour,
            minute=random.randint(0, 59),
            second=random.randint(0, 59)
        )

        # Watch duration with more variation
        # Some people watch full content, some drop off
        completion_type = random.choices(['full', 'partial', 'short'], weights=[0.3, 0.5, 0.2])[0]

        if completion_type == 'full':
            duration_seconds = min(content_duration * 60,
                                   random.randint(int(content_duration * 60 * 0.9), content_duration * 60))
        elif completion_type == 'partial':
            duration_seconds = random.randint(int(content_duration * 60 * 0.3), int(content_duration * 60 * 0.8))
        else:  # short
            duration_seconds = random.randint(60, 600)  # 1-10 minutes

        max_watch_seconds = min(content_duration * 60, 7200)
        duration_seconds = min(duration_seconds, max_watch_seconds)

        # Session end
        session_end = session_start + timedelta(seconds=duration_seconds)

        # Completion rate
        completion_rate = min(duration_seconds / (content_duration * 60), 1.0) * 100

        # Platform with device correlation
        platform_weights = [0.3, 0.25, 0.2, 0.15, 0.05,
                            0.05]  # web, mobile_ios, mobile_android, smart_tv, game_console, tablet
        platform = random.choices(PLATFORMS, weights=platform_weights)[0]

        # Device type mapping with more variation
        platform_device_map = {
            'web': ['desktop', 'laptop'],
            'mobile_ios': ['phone', 'tablet'],
            'mobile_android': ['phone', 'tablet'],
            'smart_tv': ['tv'],
            'game_console': ['console'],
            'tablet': ['tablet']
        }
        device_type = random.choice(platform_device_map[platform])

        # Quality based on device and time of day
        if device_type in ['tv', 'desktop', 'laptop']:
            if hour >= 18:  # Evening prime time
                quality_weights = [0.05, 0.2, 0.5, 0.2, 0.05]
            else:
                quality_weights = [0.1, 0.3, 0.4, 0.15, 0.05]
        else:  # Mobile devices
            if random.random() < 0.3:  # On mobile data
                quality_weights = [0.4, 0.5, 0.1, 0.0, 0.0]
            else:  # On WiFi
                quality_weights = [0.2, 0.5, 0.2, 0.05, 0.05]

        quality = random.choices(QUALITIES, weights=quality_weights)[0]

        # Buffering count - more realistic distribution
        buffering_prob = random.random()
        if buffering_prob < 0.6:  # 60% no buffering
            buffering_count = 0
        elif buffering_prob < 0.85:  # 25% minimal buffering
            buffering_count = random.randint(1, 2)
        elif buffering_prob < 0.95:  # 10% moderate buffering
            buffering_count = random.randint(3, 5)
        else:  # 5% heavy buffering
            buffering_count = random.randint(6, 10)

        # Bitrate with variation
        quality_bitrate = {
            'SD': random.randint(800, 1200),
            'HD': random.randint(2500, 3500),
            'Full HD': random.randint(5500, 6500),
            '4K': random.randint(14000, 16000),
            'HDR': random.randint(18000, 22000)
        }
        avg_bitrate = quality_bitrate[quality]

        # Location data
        if random.random() < 0.7:
            city = fake.city()
        else:
            city = None

        # IP address with more variation
        if random.random() < 0.6:
            ip_type = random.choices(['ipv4', 'ipv6'], weights=[0.9, 0.1])[0]
            ip_address = fake.ipv4() if ip_type == 'ipv4' else fake.ipv6()
        else:
            ip_address = None

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
        # Sélectionner un utilisateur et un contenu avec some users rating more
        user_weights = np.random.exponential(scale=1.0, size=len(user_ids))
        user_weights = user_weights / user_weights.sum()
        user_id = random.choices(user_ids, weights=user_weights)[0]

        content_id = random.choice(content_ids)

        # Vérifier si cette paire existe déjà
        pair = (user_id, content_id)
        if pair in user_content_pairs:
            continue  # Passer à l'itération suivante

        user_content_pairs.add(pair)

        # Rating distribution - more realistic (J-shaped distribution common in ratings)
        rating_distribution = random.choices(
            [1, 2, 3, 4, 5],
            weights=[0.1, 0.15, 0.25, 0.3, 0.2]  # More 4s, fewer 1s and 5s
        )[0]

        # Add some random noise
        rating = rating_distribution + random.choice([-1, 0, 1])
        rating = max(1, min(5, rating))

        # Date d'évaluation - some ratings right after viewing, some later
        days_after_viewing = np.random.exponential(scale=7)  # Most within a week
        days_after_viewing = min(days_after_viewing, 90)  # But some up to 3 months

        rating_date = datetime.now() - timedelta(days=random.randint(1, 180)) - timedelta(days=days_after_viewing)

        # Review text - longer reviews for extreme ratings
        if random.random() < 0.3:
            if rating in [1, 5]:
                # Extreme ratings get longer reviews
                review_text = fake.text(max_nb_chars=random.randint(100, 500))
            else:
                review_text = fake.text(max_nb_chars=random.randint(50, 200))
        else:
            review_text = None

        # Helpful count - follows power law distribution
        helpful_count = int(np.random.pareto(1.5))  # Few have many helpful votes
        helpful_count = min(helpful_count, 100)

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
        # Users have different watchlist behavior
        user_id = random.choice(user_ids)

        # Some content is more likely to be watchlisted
        content_weights = np.random.exponential(scale=1.0, size=len(content_ids))
        content_weights = content_weights / content_weights.sum()
        content_id = random.choices(content_ids, weights=content_weights)[0]

        # Vérifier si cette paire existe déjà
        pair = (user_id, content_id)
        if pair in user_content_pairs:
            continue

        user_content_pairs.add(pair)

        # Added date - some recent, some older
        days_ago = np.random.exponential(scale=30)
        days_ago = min(days_ago, 365)
        added_date = datetime.now() - timedelta(days=days_ago)

        # Watched status - depends on how long it's been in watchlist
        days_in_watchlist = (datetime.now() - added_date).days
        watched_prob = min(0.8, days_in_watchlist / 90)  # Increases over time, max 80%

        watched = random.random() < watched_prob

        if watched:
            # Watched after some time in watchlist
            watch_delay = np.random.exponential(scale=14)
            watch_delay = min(watch_delay, 90)
            watched_date = added_date + timedelta(days=watch_delay)
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

    # Different users have different event frequencies
    user_event_weights = {}
    for user_id, _ in users:
        user_event_weights[user_id] = np.random.exponential(scale=1.0)

    for i in range(n_events):
        # Weight users by their event frequency
        user_ids = [u[0] for u in users]
        weights = [user_event_weights.get(uid, 1.0) for uid in user_ids]
        weights = np.array(weights) / sum(weights)

        selected_idx = random.choices(range(len(users)), weights=weights)[0]
        user_id, current_plan = users[selected_idx]

        # Limiter à 5 événements par utilisateur
        if user_id not in user_event_count:
            user_event_count[user_id] = 0

        if user_event_count[user_id] >= random.randint(3, 8):  # Variable limit
            continue

        user_event_count[user_id] += 1

        # Event type with more realistic transition patterns
        if current_plan == 'free_trial':
            event_types = ['subscription_start', 'upgrade', 'cancellation']
            weights = [0.7, 0.2, 0.1]  # Most start with free trial
        elif current_plan == 'basic':
            event_types = ['renewal', 'upgrade', 'downgrade', 'cancellation', 'payment_failed']
            weights = [0.5, 0.3, 0.05, 0.1, 0.05]
        elif current_plan == 'standard':
            event_types = ['renewal', 'upgrade', 'downgrade', 'cancellation', 'payment_failed']
            weights = [0.6, 0.2, 0.1, 0.05, 0.05]
        elif current_plan == 'premium':
            event_types = ['renewal', 'downgrade', 'cancellation', 'payment_failed']
            weights = [0.7, 0.15, 0.1, 0.05]
        else:  # family
            event_types = ['renewal', 'downgrade', 'cancellation', 'payment_failed']
            weights = [0.8, 0.1, 0.05, 0.05]

        event_type = random.choices(event_types, weights=weights)[0]

        # Event date - clustered around certain times (end of month, holidays)
        days_ago = np.random.exponential(scale=180)  # More recent events
        days_ago = min(days_ago, 730)

        event_date = datetime.now() - timedelta(days=days_ago)

        # Adjust for end-of-month clustering
        if random.random() < 0.3 and event_date.day > 25:
            event_date = event_date.replace(day=random.randint(28, 31))

        # Plan transitions
        if event_type == 'subscription_start':
            previous_plan = None
            new_plan = 'free_trial'
        elif event_type in ['upgrade', 'downgrade']:
            previous_plan = current_plan
            possible_plans = [p for p in SUBSCRIPTION_PLANS if p != current_plan]

            if event_type == 'upgrade':
                # Upgrade to higher tier
                plan_levels = {'free_trial': 0, 'basic': 1, 'standard': 2, 'premium': 3, 'family': 4}
                current_level = plan_levels[current_plan]
                higher_plans = [p for p in possible_plans if plan_levels[p] > current_level]
                new_plan = random.choice(higher_plans) if higher_plans else current_plan
            else:  # downgrade
                plan_levels = {'free_trial': 0, 'basic': 1, 'standard': 2, 'premium': 3, 'family': 4}
                current_level = plan_levels[current_plan]
                lower_plans = [p for p in possible_plans if plan_levels[p] < current_level]
                new_plan = random.choice(lower_plans) if lower_plans else current_plan
        elif event_type == 'cancellation':
            previous_plan = current_plan
            new_plan = None
        else:  # renewal ou payment_failed
            previous_plan = current_plan
            new_plan = current_plan

        # Amount with regional pricing and discounts
        plan_prices = {
            'free_trial': 0,
            'basic': random.uniform(6.99, 8.99),
            'standard': random.uniform(9.99, 11.99),
            'premium': random.uniform(14.99, 16.99),
            'family': random.uniform(18.99, 21.99)
        }

        amount = None
        if new_plan and event_type != 'payment_failed':
            base_amount = plan_prices.get(new_plan, 0)

            # Apply occasional discounts
            if random.random() < 0.1:  # 10% chance of discount
                discount = random.choice([0.1, 0.15, 0.2, 0.25])
                base_amount *= (1 - discount)

            amount = round(base_amount, 2)

        # Currency based on country (would need country info, using USD as default)
        currency = 'USD'

        # Payment gateway with regional preferences
        payment_gateways = ['stripe', 'paypal', 'apple_pay', 'google_pay', 'bank_transfer']
        gateway_weights = [0.5, 0.3, 0.1, 0.05, 0.05]
        payment_gateway = random.choices(payment_gateways, weights=gateway_weights)[
            0] if amount and amount > 0 else None

        # Transaction ID
        transaction_id = None
        if amount and amount > 0:
            transaction_id = f"txn_{random.randint(100000000, 999999999)}_{int(event_date.timestamp())}"

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

    # Enhanced search keywords with categories
    search_keywords = {
        'genres': ['action', 'comedy', 'drama', 'horror', 'romance', 'sci-fi', 'thriller',
                   'documentary', 'animation', 'fantasy', 'adventure', 'crime', 'mystery'],
        'actors': ['leonardo dicaprio', 'meryl streep', 'tom hanks', 'jennifer lawrence',
                   'robert downey jr', 'scarlett johansson', 'brad pitt', 'johnny depp'],
        'directors': ['christopher nolan', 'steven spielberg', 'martin scorsese', 'quentin tarantino',
                      'james cameron', 'david fincher', 'ridley scott'],
        'years': ['2024', '2023', '2022', '2021', '2020', '2019', '2010s', '2000s', '90s'],
        'qualities': ['4k', 'hdr', 'dolby atmos', 'imax', 'hd'],
        'moods': ['funny', 'sad', 'exciting', 'scary', 'romantic', 'inspiring', 'thought-provoking'],
        'times': ['christmas', 'halloween', 'summer', 'winter', 'holiday', 'weekend'],
        'popular': ['new', 'popular', 'trending', 'top', 'best', 'award winning', 'oscar']
    }

    queries_data = []

    # Progress bar
    pbar = tqdm(total=n_queries, desc="Génération des recherches", unit="query")

    for i in range(n_queries):
        # User (some searches are by guests)
        if random.random() < 0.7 and user_ids:
            user_id = random.choice(user_ids)
        else:
            user_id = None

        # Query text with more variety
        query_pattern = random.choices(
            ['single', 'multiple', 'actor', 'director', 'year', 'mixed'],
            weights=[0.2, 0.3, 0.15, 0.1, 0.1, 0.15]
        )[0]

        if query_pattern == 'single' and content_data:
            _, title, _ = random.choice(content_data)
            query_text = random.choice(title.split()[:3])  # Just part of title
        elif query_pattern == 'multiple':
            # Multiple keywords
            category = random.choice(list(search_keywords.keys()))
            words = random.sample(search_keywords[category], random.randint(1, 3))
            query_text = ' '.join(words)
        elif query_pattern == 'actor':
            query_text = random.choice(search_keywords['actors'])
        elif query_pattern == 'director':
            query_text = random.choice(search_keywords['directors'])
        elif query_pattern == 'year':
            query_text = random.choice(search_keywords['years'])
        else:  # mixed
            categories = random.sample(list(search_keywords.keys()), random.randint(2, 3))
            words = []
            for category in categories:
                words.append(random.choice(search_keywords[category]))
            query_text = ' '.join(words)

        # Search date - more during peak hours
        hour = random.randint(0, 23)
        if 18 <= hour <= 23:  # Evening peak
            days_ago = np.random.exponential(scale=7)  # More recent
        else:
            days_ago = np.random.exponential(scale=14)

        days_ago = min(days_ago, 90)
        search_date = datetime.now() - timedelta(days=days_ago)
        search_date = search_date.replace(hour=hour, minute=random.randint(0, 59))

        # Results count - follows power law
        results_count = int(np.random.pareto(1.2))
        results_count = min(results_count, 500)

        # Clicked content - more likely for popular content
        clicked_content_id = None
        if random.random() < 0.4 and content_data:
            # Popular content more likely to be clicked
            popular_content = [(cid, title, genre) for cid, title, genre in content_data
                               if random.random() < 0.3]  # 30% are "popular"
            if popular_content:
                clicked_content_id = random.choice(popular_content)[0]
            else:
                clicked_content_id = random.choice(content_data)[0]

        # Search filters - more complex filters
        search_filters = None
        if random.random() < 0.5:
            filters = {}

            # Genre filter
            if random.random() < 0.6:
                filters['genre'] = random.choice(search_keywords['genres'])

            # Year filter
            if random.random() < 0.4:
                filter_type = random.choice(['exact', 'range', 'decade'])
                if filter_type == 'exact':
                    filters['year'] = random.randint(1990, 2024)
                elif filter_type == 'range':
                    start_year = random.randint(1990, 2015)
                    filters['year_range'] = [start_year, start_year + random.randint(5, 15)]
                else:  # decade
                    filters['decade'] = random.choice(['1990s', '2000s', '2010s', '2020s'])

            # Rating filter
            if random.random() < 0.3:
                filters['min_rating'] = round(random.uniform(6.0, 9.0), 1)

            # Content type filter
            if random.random() < 0.2:
                filters['content_type'] = random.choice(CONTENT_TYPES)

            if filters:
                search_filters = json.dumps(filters)

        # Session ID
        session_id = f"sess_{random.randint(100000, 999999)}_{int(search_date.timestamp())}"

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

    # Shuffle TV shows for more randomness
    random.shuffle(tv_shows)

    for tv_show_id, tv_show_title in tv_shows:
        # Variable number of seasons
        n_seasons = random.choices([1, 2, 3, 4, 5, 6], weights=[0.1, 0.2, 0.3, 0.2, 0.15, 0.05])[0]

        for season in range(1, n_seasons + 1):
            # Variable episodes per season
            n_episodes_per_season = random.choices(
                [6, 8, 10, 12, 13, 16, 20, 22],
                weights=[0.05, 0.1, 0.2, 0.3, 0.2, 0.1, 0.03, 0.02]
            )[0]

            for episode in range(1, n_episodes_per_season + 1):
                # Episode title patterns
                episode_title_patterns = [
                    f"Episode {episode}",
                    f"Chapter {episode}",
                    f"Part {episode}",
                    f"S{season:02d}E{episode:02d}"
                ]

                title_base = random.choice(episode_title_patterns)

                # Add descriptive title
                descriptive_titles = [
                    'Pilot', 'Beginnings', 'Endings', 'The Start', 'The Finish',
                    'Unexpected', 'Revelations', 'Secrets', 'Truth', 'Lies',
                    'Alliances', 'Betrayals', 'Hope', 'Despair', 'Love', 'Hate',
                    'Crossroads', 'Turning Point', 'Last Stand', 'New Dawn'
                ]

                if random.random() < 0.8:
                    episode_title = f"{title_base}: {random.choice(descriptive_titles)}"
                else:
                    episode_title = title_base

                # Duration with variation
                duration_minutes = random.choices(
                    [20, 30, 40, 45, 50, 55, 60, 75],
                    weights=[0.05, 0.1, 0.2, 0.25, 0.2, 0.1, 0.08, 0.02]
                )[0]

                # Release date - episodes within a season are close together
                season_start = fake.date_between(
                    start_date=date.today() - timedelta(days=5 * 365),
                    end_date=date.today() - timedelta(days=30)
                )

                # Episodes released weekly
                episode_offset = (episode - 1) * 7
                release_date = season_start + timedelta(days=episode_offset)

                # Director - sometimes same for season, sometimes different
                if episode == 1 or random.random() < 0.3:
                    director = fake.name()
                else:
                    # Keep same director for some episodes
                    director = episodes_data[-1][6] if episodes_data else fake.name()

                # IMDB rating - first and last episodes often rated higher
                if episode == 1 or episode == n_episodes_per_season:
                    imdb_rating = round(random.uniform(7.5, 9.5), 1)
                elif episode < 3 or episode > n_episodes_per_season - 2:
                    imdb_rating = round(random.uniform(7.0, 8.5), 1)
                else:
                    imdb_rating = round(random.uniform(6.5, 8.0), 1)

                # Description length varies
                description = fake.text(max_nb_chars=random.randint(50, 200))

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

    # Shuffle episodes before insertion
    random.shuffle(episodes_data)

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
        # Choose viewing session or create new
        if viewing_sessions_data and random.random() < 0.6:
            viewing_session_id, user_id = random.choice(viewing_sessions_data)
        else:
            viewing_session_id = None
            user_id = random.choice(user_ids)

        # Choose episode - binge watching patterns
        episode_id, episode_duration = random.choice(episodes)

        # For binge watching, multiple episodes in sequence
        if random.random() < 0.3 and i > 0:
            # Continue watching next episode
            last_viewing = episode_viewing_data[-1]
            last_episode_id = last_viewing[1]
            last_user_id = last_viewing[2]
            last_end_time = last_viewing[4]

            # Get next episode (simplified - in real case would query database)
            if user_id == last_user_id and random.random() < 0.7:
                # Same user continues binge
                start_time = last_end_time + timedelta(minutes=random.randint(1, 60))
            else:
                start_time = fake.date_time_between(
                    start_date=datetime.now() - timedelta(days=90),
                    end_date=datetime.now()
                )
        else:
            # New viewing session
            start_time = fake.date_time_between(
                start_date=datetime.now() - timedelta(days=90),
                end_date=datetime.now()
            )

        # Watch duration - binge vs casual watching
        if random.random() < 0.4:  # Binge watching
            duration_watched = episode_duration * 60  # Watch full episode
        else:  # Casual watching
            watch_ratio = random.choices(['short', 'medium', 'full'], weights=[0.3, 0.4, 0.3])[0]
            if watch_ratio == 'short':
                duration_watched = random.randint(300, 900)  # 5-15 minutes
            elif watch_ratio == 'medium':
                duration_watched = random.randint(int(episode_duration * 60 * 0.3), int(episode_duration * 60 * 0.7))
            else:  # full
                duration_watched = episode_duration * 60

        # Ensure duration is valid
        max_watch = min(episode_duration * 60, 3600)
        duration_watched = min(duration_watched, max_watch)
        duration_watched = max(300, duration_watched)  # At least 5 minutes

        # Completion rate
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