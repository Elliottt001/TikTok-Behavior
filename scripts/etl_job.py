from pathlib import Path
import db_connector


class SQLRunner:
    def __init__(self, db_path):
        self.con = db_connector.get_connection(db_path)

    def execute_sql(self, sql_file):
        sql = Path(sql_file).read_text(encoding="UTF-8")
        self.con.execute(sql)


if __name__ == "__main__":

    db_path = '../data/tiktok_dw.db'
    sql_dir = '../sql/'

    # 实例化 Runner
    runner = SQLRunner(db_path)

    ods_sql_path = sql_dir + 'ods_load.sql'
    runner.execute_sql(ods_sql_path)

    dwd_sql_path = sql_dir + 'dwd_cleansing.sql'
    runner.execute_sql(dwd_sql_path)
