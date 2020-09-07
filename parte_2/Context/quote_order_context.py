import sqlite3

from pyspark.sql.functions import from_unixtime

from .base_context import BaseContext

class QuoteOrderContext(BaseContext):
    def __init__(self):
        self.app_name = 'Process Quote Order'
        super().__init__()

    def transformation(self, file_path):
        df_input = self.spark.read.json(file_path)

        df_quote = df_input.selectExpr('payload.order_uuid as id',
                                       'payload.pricing.monthly_cost as pricing',
                                       'raw_timestamp as updated_at')

        df_quote = df_quote.withColumn('updated_at',
                                       from_unixtime(df_quote.updated_at))
        return df_quote

    def save(self, data):
        conn = sqlite3.connect('datalake.db')
        cur = conn.cursor()
        for row in data.collect():
            cur.execute(f"""
            UPDATE orders
            SET updated_at = '{row.updated_at}',
                pricing = {row.pricing}
            WHERE id = '{row.id}'""")
        conn.commit()
