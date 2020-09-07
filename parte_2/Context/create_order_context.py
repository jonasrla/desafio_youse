from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from pyspark.sql.functions import from_unixtime

from .base_context import BaseContext

class CreateOrderContext(BaseContext):
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
                ]), False)
            ]), False)
        ])

        self.app_name = 'Process Create Order'
        super().__init__()

    def transformation(self, data):
        df_input = self.spark.createDataFrame(data, self.input_schema)

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
