DROP TABLE IF EXISTS dwd_users;

CREATE TABLE dwd_users AS

SELECT
    user_id,
    nickname,
    age,
    -- 年龄段分桶
    CASE
        WHEN t.age < 18 THEN 'teenager'
        WHEN t.age BETWEEN 18 AND 35 THEN 'young person'
        WHEN t.age BETWEEN 35 AND 50 THEN 'middle-aged person'
        ELSE 'the elderly'
    END AS age_groups,
    ip,
    fans_num,
    likes_num,
    phone_type,
    CAST(register_date AS TIMESTAMP) AS register_date

FROM (
    SELECT
        user_id,
        nickname,
        age,
        ip,
        fans_num,
        likes_num,
        phone_type,
        register_date,
        ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY register_date DESC) as rn

    FROM ods_user u
    WHERE user_id IS NOT NULL
        AND nickname IS NOT NULL
) t
WHERE t.rn = 1;

DROP TABLE IF EXISTS dwd_video;

CREATE TABLE dwd_video AS

SELECT 
    user_id,
    video_id,
    video_title,
    tags,
    duration,
    -- 视频长度统计
    CASE 
        WHEN t.duration < 60 THEN 'short'
        WHEN t.duration BETWEEN 60 AND 300 THEN 'medium'
        ELSE 'long'
    END AS length_groups,
    upload_time


FROM (
    SELECT 
        user_id,
        video_id,
        video_title,
        tags,
        duration,
        upload_time,
        ROW_NUMBER() OVER(PARTITION BY video_id ORDER BY upload_time DESC) as rn

        FROM ods_video v
        WHERE user_id IS NOT NULL
            AND video_id IS NOT NULL
            AND duration > 0
) t
WHERE t.rn = 1;


DROP TABLE IF EXISTS dwd_behavior_logs;

CREATE TABLE dwd_behavior_logs AS

-- 清洗和标准化
SELECT DISTINCT

    l.user_id,
    l.video_id,
    l.action_type,
    l.duration,

    CAST(l.time_stamp AS TIMESTAMP) AS event_time,
    EXTRACT(HOUR FROM CAST(l.time_stamp AS TIMESTAMP)) AS event_hour,
    l.ip,

-- 维度退化
    v.tags AS video_tags,
    v.duration AS video_total_time,

    u.age_groups AS age_groups,
    u.ip AS user_ip,

-- 业务逻辑计算
-- 完播状态

CASE 
    WHEN (l.duration / v.duration) > 0.9 THEN 1
    ELSE 0
END AS is_finished

FROM ods_behavior_logs l

-- 关联维度表
LEFT JOIN dwd_video v ON l.video_id = v.video_id
LEFT JOIN dwd_users u ON l.user_id = u.user_id

WHERE l.user_id IS NOT NULL
    AND l.video_id IS NOT NULL
    AND l.duration > 0;
