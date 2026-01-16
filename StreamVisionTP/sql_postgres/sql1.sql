-- ============================================
-- Script de création du schéma OLTP PostgreSQL
-- Plateforme StreamVision (Streaming Vidéo)
-- ============================================

-- 1. Table USERS (Abonnés)
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    username VARCHAR(100) UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    country VARCHAR(3) NOT NULL,
    age_group VARCHAR(20) CHECK (age_group IN ('13-17', '18-24', '25-34', '35-44', '45-54', '55+')),
    subscription_plan VARCHAR(20) NOT NULL DEFAULT 'free_trial' 
        CHECK (subscription_plan IN ('free_trial', 'basic', 'standard', 'premium', 'family')),
    subscription_start DATE NOT NULL,
    subscription_end DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    last_login TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    payment_method VARCHAR(50),
    device_preference VARCHAR(50)
);

-- 2. Table CONTENT (Contenu vidéo)
CREATE TABLE content (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    content_type VARCHAR(20) NOT NULL 
        CHECK (content_type IN ('movie', 'tv_show', 'documentary', 'short_film', 'original')),
    genre VARCHAR(100) NOT NULL,
    subgenre VARCHAR(100),
    release_year INT CHECK (release_year BETWEEN 1900 AND EXTRACT(YEAR FROM NOW())),
    duration_minutes INT NOT NULL CHECK (duration_minutes > 0),
    director VARCHAR(255),
    main_actor VARCHAR(255),
    imdb_rating DECIMAL(3,1) CHECK (imdb_rating BETWEEN 0 AND 10),
    content_rating VARCHAR(10) CHECK (content_rating IN ('G', 'PG', 'PG-13', 'R', 'NC-17')),
    is_original BOOLEAN DEFAULT FALSE,
    added_date DATE DEFAULT CURRENT_DATE,
    available_countries TEXT[], -- Tableau de pays où le contenu est disponible
    tags TEXT[], -- Mots-clés pour la recherche
    description TEXT
);

-- 3. Table VIEWING_SESSIONS (Sessions de visionnage)
CREATE TABLE viewing_sessions (
    id BIGSERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    content_id INT NOT NULL REFERENCES content(id) ON DELETE CASCADE,
    session_start TIMESTAMP NOT NULL,
    session_end TIMESTAMP,
    duration_seconds INT NOT NULL CHECK (duration_seconds > 0),
    platform VARCHAR(50) NOT NULL 
        CHECK (platform IN ('web', 'mobile_ios', 'mobile_android', 'smart_tv', 'game_console', 'tablet')),
    device_type VARCHAR(50) NOT NULL 
        CHECK (device_type IN ('desktop', 'laptop', 'phone', 'tablet', 'tv', 'console')),
    quality VARCHAR(20) CHECK (quality IN ('SD', 'HD', 'Full HD', '4K', 'HDR')),
    completion_rate DECIMAL(5,2) CHECK (completion_rate BETWEEN 0 AND 100),
    buffering_count INT DEFAULT 0,
    avg_bitrate INT, -- en kbps
    city VARCHAR(100),
    ip_address VARCHAR(45)
);

-- 4. Table RATINGS (Évaluations)
CREATE TABLE ratings (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    content_id INT NOT NULL REFERENCES content(id) ON DELETE CASCADE,
    rating INT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    rating_date TIMESTAMP DEFAULT NOW(),
    review_text TEXT,
    helpful_count INT DEFAULT 0,
    UNIQUE(user_id, content_id) -- Un utilisateur ne peut évaluer qu'une fois un contenu
);

-- 5. Table WATCHLIST (Liste de visionnage)
CREATE TABLE watchlist (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    content_id INT NOT NULL REFERENCES content(id) ON DELETE CASCADE,
    added_date TIMESTAMP DEFAULT NOW(),
    watched BOOLEAN DEFAULT FALSE,
    watched_date TIMESTAMP,
    UNIQUE(user_id, content_id)
);

-- 6. Table SUBSCRIPTION_EVENTS (Événements d'abonnement)
CREATE TABLE subscription_events (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    event_type VARCHAR(50) NOT NULL 
        CHECK (event_type IN ('subscription_start', 'upgrade', 'downgrade', 'cancellation', 'renewal', 'payment_failed')),
    event_date TIMESTAMP DEFAULT NOW(),
    previous_plan VARCHAR(20),
    new_plan VARCHAR(20),
    amount DECIMAL(10,2),
    currency VARCHAR(3) DEFAULT 'USD',
    payment_gateway VARCHAR(50),
    transaction_id VARCHAR(100)
);

-- 7. Table SEARCH_QUERIES (Recherches)
CREATE TABLE search_queries (
    id BIGSERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id) ON DELETE SET NULL,
    query_text VARCHAR(500) NOT NULL,
    search_date TIMESTAMP DEFAULT NOW(),
    results_count INT,
    clicked_content_id INT REFERENCES content(id) ON DELETE SET NULL,
    search_filters JSONB, -- Filtres appliqués (genre, année, etc.)
    session_id VARCHAR(100)
);

-- 8. Table EPISODES (Épisodes pour les séries)
CREATE TABLE episodes (
    id SERIAL PRIMARY KEY,
    tv_show_id INT NOT NULL REFERENCES content(id) ON DELETE CASCADE,
    season_number INT NOT NULL,
    episode_number INT NOT NULL,
    title VARCHAR(255) NOT NULL,
    duration_minutes INT NOT NULL,
    release_date DATE,
    director VARCHAR(255),
    imdb_rating DECIMAL(3,1),
    description TEXT,
    UNIQUE(tv_show_id, season_number, episode_number)
);

-- 9. Table EPISODE_VIEWING (Visionnage d'épisodes)
CREATE TABLE episode_viewing (
    id BIGSERIAL PRIMARY KEY,
    viewing_session_id BIGINT REFERENCES viewing_sessions(id) ON DELETE CASCADE,
    episode_id INT NOT NULL REFERENCES episodes(id) ON DELETE CASCADE,
    user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_watched INT NOT NULL,
    completion_rate DECIMAL(5,2)
);

-- ============================================
-- Création des INDEX pour optimiser les performances
-- ============================================

-- Index pour la table USERS
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_country ON users(country);
CREATE INDEX idx_users_subscription_plan ON users(subscription_plan);
CREATE INDEX idx_users_subscription_end ON users(subscription_end);
CREATE INDEX idx_users_is_active ON users(is_active);

-- Index pour la table CONTENT
CREATE INDEX idx_content_title ON content(title);
CREATE INDEX idx_content_genre ON content(genre);
CREATE INDEX idx_content_type ON content(content_type);
CREATE INDEX idx_content_release_year ON content(release_year);
CREATE INDEX idx_content_is_original ON content(is_original);

-- Index pour la table VIEWING_SESSIONS
CREATE INDEX idx_viewing_sessions_user_id ON viewing_sessions(user_id);
CREATE INDEX idx_viewing_sessions_content_id ON viewing_sessions(content_id);
CREATE INDEX idx_viewing_sessions_session_start ON viewing_sessions(session_start);
CREATE INDEX idx_viewing_sessions_platform ON viewing_sessions(platform);
CREATE INDEX idx_viewing_sessions_completion_rate ON viewing_sessions(completion_rate);

-- Index pour la table RATINGS
CREATE INDEX idx_ratings_user_id ON ratings(user_id);
CREATE INDEX idx_ratings_content_id ON ratings(content_id);
CREATE INDEX idx_ratings_rating ON ratings(rating);
CREATE INDEX idx_ratings_rating_date ON ratings(rating_date);

-- Index pour la table WATCHLIST
CREATE INDEX idx_watchlist_user_id ON watchlist(user_id);
CREATE INDEX idx_watchlist_watched ON watchlist(watched);

-- Index pour la table SUBSCRIPTION_EVENTS
CREATE INDEX idx_subscription_events_user_id ON subscription_events(user_id);
CREATE INDEX idx_subscription_events_event_date ON subscription_events(event_date);
CREATE INDEX idx_subscription_events_event_type ON subscription_events(event_type);

-- Index pour la table SEARCH_QUERIES
CREATE INDEX idx_search_queries_user_id ON search_queries(user_id);
CREATE INDEX idx_search_queries_search_date ON search_queries(search_date);
CREATE INDEX idx_search_queries_clicked_content ON search_queries(clicked_content_id);

-- Index pour la table EPISODES
CREATE INDEX idx_episodes_tv_show_id ON episodes(tv_show_id);
CREATE INDEX idx_episodes_season_episode ON episodes(tv_show_id, season_number, episode_number);

-- Index pour la table EPISODE_VIEWING
CREATE INDEX idx_episode_viewing_user_id ON episode_viewing(user_id);
CREATE INDEX idx_episode_viewing_episode_id ON episode_viewing(episode_id);
CREATE INDEX idx_episode_viewing_start_time ON episode_viewing(start_time);

-- ============================================
-- Commentaires sur les tables
-- ============================================

COMMENT ON TABLE users IS 'Table des abonnés de la plateforme StreamVision';
COMMENT ON TABLE content IS 'Catalogue des contenus vidéo (films, séries, documentaires)';
COMMENT ON TABLE viewing_sessions IS 'Sessions de visionnage avec métriques techniques';
COMMENT ON TABLE ratings IS 'Évaluations et critiques des utilisateurs';
COMMENT ON TABLE watchlist IS 'Liste de visionnage personnelle des utilisateurs';
COMMENT ON TABLE subscription_events IS 'Historique des événements d''abonnement';
COMMENT ON TABLE search_queries IS 'Recherches effectuées par les utilisateurs';
COMMENT ON TABLE episodes IS 'Épisodes pour les séries TV';
COMMENT ON TABLE episode_viewing IS 'Visionnage détaillé par épisode';

-- ============================================
-- Vues utiles pour l''application
-- ============================================

-- Vue des utilisateurs actifs (dernière connexion < 30 jours)
CREATE VIEW active_users AS
SELECT * FROM users 
WHERE last_login >= NOW() - INTERVAL '30 days' 
AND is_active = TRUE;

-- Vue du contenu le plus populaire (plus de 1000 visionnages)
CREATE VIEW popular_content AS
SELECT 
    c.id,
    c.title,
    c.content_type,
    c.genre,
    COUNT(vs.id) as view_count,
    AVG(vs.completion_rate) as avg_completion,
    AVG(r.rating) as avg_rating
FROM content c
LEFT JOIN viewing_sessions vs ON c.id = vs.content_id
LEFT JOIN ratings r ON c.id = r.content_id
GROUP BY c.id, c.title, c.content_type, c.genre
HAVING COUNT(vs.id) >= 1000
ORDER BY view_count DESC;

-- Vue des statistiques d''engagement quotidien
CREATE VIEW daily_engagement AS
SELECT 
    DATE(session_start) as viewing_date,
    COUNT(DISTINCT user_id) as daily_active_users,
    COUNT(*) as total_sessions,
    SUM(duration_seconds) as total_watch_time_seconds,
    AVG(completion_rate) as avg_completion_rate
FROM viewing_sessions
GROUP BY DATE(session_start)
ORDER BY viewing_date DESC;

-- Message de confirmation
SELECT 'Schéma StreamVision créé avec succès!' as message;