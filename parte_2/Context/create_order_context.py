from pyspark.sql.types import StructField, StructType, StringType, IntegerType

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
        df_input = df_input.withColumn('client_id', self.create_id())

        df_client = df_input.select('client_id', 'payload.lead_person.*')
        df_client = df_client.withColumnRenamed('client_id', 'id')

        return df_client

    def save(self, data):
        data.write.jdbc(url='jdbc:sqlite:datalake.db',
                        table='clients',
                        mode='append',
                        properties={"driver":"org.sqlite.JDBC"})
        return 'Success'
