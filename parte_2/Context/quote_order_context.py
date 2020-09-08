from pyspark.sql.functions import from_unixtime

from .base_context import BaseContext

class QuoteOrderContext(BaseContext):
    def __init__(self, file_path):
        self.app_name = 'Process Quote Order'
        super().__init__(file_path)

    def process(self):
        df_quote = self.transformation()
        self.update_table(df_quote, 'orders')

    def transformation(self):
        df_quote = self.input.selectExpr('payload.order_uuid as id',
                                         'payload.pricing.monthly_cost as pricing',
                                         'raw_timestamp as updated_at')

        df_quote = df_quote.withColumn('updated_at',
                                       from_unixtime(df_quote.updated_at))
        return df_quote
