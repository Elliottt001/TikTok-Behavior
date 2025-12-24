DROP TABLE IF EXISTS ads_hot_videos;
CREATE TABLE ads_hot_videos AS

SELECT 

    dv.video_title,
    -- db.action_type, -- 移除此列，因为我们要按视频聚合
    COUNT(*) FILTER (WHERE db.action_type = 'view') AS views,
    COUNT(*) FILTER (WHERE db.action_type = 'like') AS likes,
    COUNT(*) FILTER (WHERE db.action_type = 'comment') AS comments,
    COUNT(*) FILTER (WHERE db.action_type = 'follow') AS follows,
    SUM(is_finished) AS is_finishes,
    (COUNT(*) FILTER (WHERE db.action_type = 'view') * 0.1 + 
    COUNT(*) FILTER (WHERE db.action_type = 'like') * 0.2 + 
    COUNT(*) FILTER (WHERE db.action_type = 'comment') * 0.2 + 
    COUNT(*) FILTER (WHERE db.action_type = 'share') * 0.3 + -- 修正：推测这里是 share (原代码重复了 view)
    SUM(is_finished) * 0.2
    ) AS hot_rank

FROM read_parquet('./data/dwd_behavior_logs_parquet.parquet') db

INNER JOIN read_parquet('./data/dwd_videos_parquet.parquet') dv ON db.video_id = dv.video_id

GROUP BY dv.video_title

ORDER BY hot_rank DESC

LIMIT 1000;


DROP TABLE IF EXISTS daily_active_users;

CREATE TABLE daily_active_users AS
SELECT 
    CAST(db.event_time AS DATE) AS event_date,
    COUNT(DISTINCT db.user_id) AS users_count
FROM read_parquet('./data/dwd_behavior_logs_parquet.parquet') db
INNER JOIN read_parquet('./data/dwd_users_parquet.parquet') du ON db.user_id = du.user_id
GROUP BY event_date
ORDER BY event_date ASC;



DROP TABLE IF EXISTS retention_rate;

CREATE TABLE retention_rate AS

WITH active_user_date AS (
    SELECT DISTINCT
        user_id,
        CAST(event_time AS DATE) AS event_date
    FROM read_parquet('./data/dwd_behavior_logs_parquet.parquet')
)

SELECT
    t1.event_date,
    COUNT(DISTINCT t1.user_id) AS active_users,
    
    -- 次日留存
    CAST(COUNT(DISTINCT t2.user_id) AS DOUBLE) / COUNT(DISTINCT t1.user_id) AS day1_retention_rate,
    
    -- 7日留存
    CAST(COUNT(DISTINCT t3.user_id) AS DOUBLE) / COUNT(DISTINCT t1.user_id) AS day7_retention_rate,
    
    -- 30日留存
    CAST(COUNT(DISTINCT t4.user_id) AS DOUBLE) / COUNT(DISTINCT t1.user_id) AS day30_retention_rate

FROM active_user_date t1
LEFT JOIN active_user_date t2 
    ON t1.user_id = t2.user_id AND t2.event_date = t1.event_date + INTERVAL 1 DAY
LEFT JOIN active_user_date t3 
    ON t1.user_id = t3.user_id AND t3.event_date = t1.event_date + INTERVAL 7 DAY
LEFT JOIN active_user_date t4 
    ON t1.user_id = t4.user_id AND t4.event_date = t1.event_date + INTERVAL 30 DAY

GROUP BY t1.event_date
ORDER BY t1.event_date;
