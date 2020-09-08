from .base_context import BaseContext

from pyspark.sql import functions as f

class CreatePolicyContext(BaseContext):
    def __init__(self, file_path):
        self.app_name = 'Process Create Policy'
        super().__init__(file_path)

    def process(self):
        df_policy = self.transformation()
        print(df_policy.collect())
        self.append_table(df_policy, 'policies')

    def transformation(self):
        df_result = self.input.selectExpr('payload.policy_number as id',
                                          'payload.order_uuid as order_id',
                                          'payload.insurance_type as insurance_type',
                                          'raw_timestamp as created_at')

        df_result = df_result.withColumn('status', f.lit('created'))

        df_result = df_result.withColumn('created_at',
                                         f.from_unixtime(df_result.created_at))
        return df_result
