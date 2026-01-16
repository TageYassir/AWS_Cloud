-- ============================================
-- Création des tables de STAGING - StreamVision
-- ============================================
Use STREAMVISION_WH;
USE SCHEMA STAGING;

-- Table STG_USERS
CREATE OR REPLACE TABLE stg_users (
    id INT,
    email VARCHAR(255),
    username VARCHAR(100),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    country VARCHAR(3),
    age_group VARCHAR(20),
    subscription_plan VARCHAR(20),
    subscription_start DATE,
    subscription_end DATE,
    created_at TIMESTAMP,
    last_login TIMESTAMP,
    is_active BOOLEAN,
    payment_method VARCHAR(50),
    device_preference VARCHAR(50),
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table STG_CONTENT
CREATE OR REPLACE TABLE stg_content (
    id INT,
    title VARCHAR(255),
    content_type VARCHAR(20),
    genre VARCHAR(100),
    subgenre VARCHAR(100),
    release_year INT,
    duration_minutes INT,
    director VARCHAR(255),
    main_actor VARCHAR(255),
    imdb_rating DECIMAL(3,1),
    content_rating VARCHAR(10),
    is_original BOOLEAN,
    added_date DATE,
    available_countries ARRAY,
    tags ARRAY,
    description TEXT,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table STG_VIEWING_SESSIONS
CREATE OR REPLACE TABLE stg_viewing_sessions (
    id BIGINT,
    user_id INT,
    content_id INT,
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    duration_seconds INT,
    platform VARCHAR(50),
    device_type VARCHAR(50),
    quality VARCHAR(20),
    completion_rate DECIMAL(5,2),
    buffering_count INT,
    avg_bitrate INT,
    city VARCHAR(100),
    ip_address VARCHAR(45),
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table STG_RATINGS
CREATE OR REPLACE TABLE stg_ratings (
    id INT,
    user_id INT,
    content_id INT,
    rating INT,
    rating_date TIMESTAMP,
    review_text TEXT,
    helpful_count INT,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table STG_WATCHLIST
CREATE OR REPLACE TABLE stg_watchlist (
    id INT,
    user_id INT,
    content_id INT,
    added_date TIMESTAMP,
    watched BOOLEAN,
    watched_date TIMESTAMP,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table STG_SUBSCRIPTION_EVENTS
CREATE OR REPLACE TABLE stg_subscription_events (
    id INT,
    user_id INT,
    event_type VARCHAR(50),
    event_date TIMESTAMP,
    previous_plan VARCHAR(20),
    new_plan VARCHAR(20),
    amount DECIMAL(10,2),
    currency VARCHAR(3),
    payment_gateway VARCHAR(50),
    transaction_id VARCHAR(100),
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table STG_SEARCH_QUERIES
CREATE OR REPLACE TABLE stg_search_queries (
    id BIGINT,
    user_id INT,
    query_text VARCHAR(500),
    search_date TIMESTAMP,
    results_count INT,
    clicked_content_id INT,
    search_filters VARIANT, -- JSON
    session_id VARCHAR(100),
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table STG_EPISODES
CREATE OR REPLACE TABLE stg_episodes (
    id INT,
    tv_show_id INT,
    season_number INT,
    episode_number INT,
    title VARCHAR(255),
    duration_minutes INT,
    release_date DATE,
    director VARCHAR(255),
    imdb_rating DECIMAL(3,1),
    description TEXT,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table STG_EPISODE_VIEWING
CREATE OR REPLACE TABLE stg_episode_viewing (
    id BIGINT,
    viewing_session_id BIGINT,
    episode_id INT,
    user_id INT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_watched INT,
    completion_rate DECIMAL(5,2),
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Vérification
SHOW TABLES;

SELECT 'Tables de STAGING StreamVision créées avec succès' AS message;
