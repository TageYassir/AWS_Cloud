SELECT COUNT(*) as total_users FROM users;


SELECT id, email, username, country, subscription_plan 
FROM users 
LIMIT 5;

SELECT 
    c.title,
    c.content_type,
    c.genre,
    COUNT(vs.id) as view_count,
    AVG(vs.completion_rate) as avg_completion
FROM content c
JOIN viewing_sessions vs ON c.id = vs.content_id
GROUP BY c.id, c.title, c.content_type, c.genre
ORDER BY view_count DESC
LIMIT 10;


SELECT 
    DATE(session_start) as viewing_date,
    COUNT(DISTINCT user_id) as daily_active_users,
    COUNT(*) as total_sessions,
    ROUND(SUM(duration_seconds) / 3600.0, 1) as total_hours_watched
FROM viewing_sessions
GROUP BY DATE(session_start)
ORDER BY viewing_date DESC
LIMIT 7;