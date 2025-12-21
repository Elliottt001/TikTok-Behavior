import duckdb
from pathlib import Path

file_match = {
    "ods_behavior_logs.db": "user_behavior_logs.json",
    "ods_video.db": "video.csv",
    "ods_users.db": "users.csv"
}


def init_warehouse(db_path, sql_file):

    with duckdb.connect(db_path) as con:
        sql = Path(sql_file).read_text(encoding="UTF-8")

        try:
            con.execute(sql)
            print("success")

        except Exception as e:
            print(f"fail, {e}")


init_warehouse('tiktok_dw.db', 'logs_processing.sql')
