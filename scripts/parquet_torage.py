import db_connector


class ToParquet:

    def store_to_parquet(self, table_name, output_path, db_path):
        sql = f'''
            COPY (SELCET *, CAST(event_time AS DATE) as event_date FROM {table_name})
            TO '{output_path}'
            (FORMAT PARQUET, PARTITION_BY (event_date), OVERWRITE 1);
        '''
        con = db_connector.get_connection(db_path)
        con.execute(sql)
