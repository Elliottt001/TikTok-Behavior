CREATE OR REPLACE TABLE ods_video AS
SELECT * FROM read_csv_auto("../data/raw/video.csv");

CREATE OR REPLACE TABLE ods_user AS
SELECT * FROM read_csv_auto("../data/raw/users.csv");

CREATE OR REPLACE TABLE ods_behavior_logs AS
SELECT * FROM read_json_auto("../data/raw/user_behavior_logs.json");