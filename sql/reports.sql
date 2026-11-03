-- 1. Region üzrə gündəlik xülasə
SELECT * FROM daily_region_stats ORDER BY event_date, region;

-- 2. Ən çox siqnal göndərən 10 gəmi
-- Qeyd: active_minutes data toplama müddəti ilə məhdudlaşır
SELECT ship_name, region, signals, active_minutes, max_speed
FROM ship_daily_activity
WHERE ship_name IS NOT NULL
ORDER BY signals DESC
LIMIT 10;

-- 3. Ən sürətli 10 gəmi (102.3 kimi səhv dəyərlər silver qatında təmizlənib)
SELECT ship_name, region, max_speed
FROM ship_daily_activity
WHERE max_speed IS NOT NULL
ORDER BY max_speed DESC
LIMIT 10;

-- 4. Region üzrə limanda və ya lövbərdə dayanan gəmilərin faizi
SELECT region,
       ROUND(100.0 * ships_anchored_or_moored / ships, 1) AS anchored_pct
FROM daily_region_stats
ORDER BY anchored_pct DESC;
