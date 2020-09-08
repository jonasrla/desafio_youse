from pyspark.sql.functions import from_unixtime

from .base_context import BaseContext

class CreateOrderContext(BaseContext):
    def __init__(self, file_path):
        self.app_name = 'Process Create Order'
        super().__init__(file_path)

    def process(self):
        df_client, df_order = self.transformation()
        self.append_table(df_client, 'clients')
        self.append_table(df_order, 'orders')

    def transformation(self):
        self.input.alias('input')
        self.input.registerTempTable('input')
        self.input = self.spark.sql('SELECT *, uuid() AS client_id FROM input')


        df_client = self.input.selectExpr('client_id as id',
                                          'payload.lead_person.*')

        df_order = self.input.selectExpr('raw_timestamp as created_at',
                                         'payload.order_uuid as id',
                                         'payload.insurance_type as insurance_type',
                                         'payload.sales_channel as sales_channel',
                                         'client_id')

        df_order = df_order.withColumn('created_at',
                                       from_unixtime(df_order.created_at))

        return df_client, df_order
