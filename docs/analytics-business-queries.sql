-- MarTech Data Platform - Sample Queries
--
-- These are simple sample queries for quick analytics checks.
-- Schema used: github_engagement_analytics

-- Sample Query 1: total events by day
SELECT
	event_date,
	SUM(events_count) AS total_events
FROM github_engagement_analytics.fct_user_repo_engagement
GROUP BY event_date
ORDER BY event_date DESC
LIMIT 30;

-- Sample Query 2: top 10 users by engagement score
SELECT
	f.user_id,
	u.user_login,
	SUM(f.engagement_score) AS total_score
FROM github_engagement_analytics.fct_user_repo_engagement f
JOIN github_engagement_analytics.dim_users u
	ON u.user_id = f.user_id
GROUP BY f.user_id, u.user_login
ORDER BY total_score DESC
LIMIT 10;

-- Sample Query 3: top 10 repositories by event volume
SELECT
	r.repo_source,
	SUM(f.events_count) AS total_events
FROM github_engagement_analytics.fct_user_repo_engagement f
JOIN github_engagement_analytics.dim_repos r
	ON r.repo_id = f.repo_id
GROUP BY r.repo_source
ORDER BY total_events DESC
LIMIT 10;
