from pyspark.sql.functions import from_unixtime

from .base_context import BaseContext

class CreateOrderContext(BaseContext):
    def __init__(self):

        self.app_name = 'Process Create Order'
        super().__init__()

    def transformation(self, file_path):
        df_input = self.spark.read.json(file_path)

        df_input.alias('input')
        df_input.registerTempTable('input')
        df_input = self.spark.sql('SELECT *, uuid() AS client_id FROM input')


        df_client = df_input.selectExpr('client_id as id',
                                        'payload.lead_person.*')

        df_order = df_input.selectExpr('raw_timestamp as created_at',
                                       'payload.order_uuid as id',
                                       'payload.insurance_type as insurance_type',
                                       'payload.sales_channel as sales_channel',
                                       'client_id')

        df_order = df_order.withColumn('created_at',
                                       from_unixtime(df_order.created_at))

        return df_client, df_order

    def save(self, data):
        data[0].write.jdbc(url='jdbc:sqlite:datalake.db',
                           table='clients',
                           mode='append',
                           properties={"driver":"org.sqlite.JDBC"})

        data[1].write.jdbc(url='jdbc:sqlite:datalake.db',
                           table='orders',
                           mode='append',
                           properties={"driver":"org.sqlite.JDBC"})
        return 'Success'
