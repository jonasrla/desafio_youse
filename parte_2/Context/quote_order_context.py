import sqlite3

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, \
    DoubleType
from pyspark.sql.functions import from_unixtime

from .base_context import BaseContext

class QuoteOrderContext(BaseContext):
    def __init__(self):
        self.input_schema = StructType([
            StructField('routing_key', StringType(), False),
            StructField('message_id', StringType(), False),
            StructField('raw_timestamp', IntegerType(), False),
            StructField('payload', StructType([
                StructField('order_uuid', StringType(), False),
                StructField('insurance_type', StringType(), False),
                StructField('sales_channel', StringType(), False),
                StructField('lead_person', StructType([
                    StructField('name', StringType(), False),
                    StructField('phone', StringType(), False),
                    StructField('email', StringType(), False),
                ]), False),
                StructField('pricing', StructType([
                    StructField('monthly_cost', DoubleType(), False)
                ]), False),
            ]), False)
        ])

        self.app_name = 'Process Quote Order'
        super().__init__()

    def transformation(self, data):
        df_input = self.spark.createDataFrame(data, self.input_schema)

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
            print(f"""UPDATE orders
            SET updated_at = '{row.updated_at}',
                pricing = {row.pricing}
            WHERE id = '{row.id}'""")
            cur.execute(f"""
            UPDATE orders
            SET updated_at = '{row.updated_at}',
                pricing = {row.pricing}
            WHERE id = '{row.id}'""")
        conn.commit()
