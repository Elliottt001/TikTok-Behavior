import duckdb

con = duckdb.connect('../data/tiktok_dw.db')

df = con.sql("SELECT * FROM information_schema.tables;").df()

df2 = con.sql("SELECT * FROM  dwd_behavior_logs").df()
df3 = con.sql("SELECT * FROM  ods_behavior_logs").df()

print(df)

con.close()
