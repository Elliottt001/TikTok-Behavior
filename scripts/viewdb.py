import duckdb

con = duckdb.connect('tiktok_dw.db')

df = con.sql("SHOW TABLES; SELECT * FROM ods_behavior_logs").df()

print(df)

con.close()
