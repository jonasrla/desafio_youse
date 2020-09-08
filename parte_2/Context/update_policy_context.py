from pyspark.sql import functions as f

from .base_context import BaseContext

class UpdatePolicyContext(BaseContext):
    def __init__(self, file_path, status):
        self.status = status
        self.app_name = 'Updates policy'
        super().__init__(file_path)

    def process(self):
        data = self.transformation()
        self.update_table(data, 'policies')

    def transformation(self):
        if 'reason' not in self.input.select('payload.*').columns:
            df_result = self.input.selectExpr('payload.policy_number as id',
                                              'raw_timestamp as updated_at')
        else:
            df_result = self.input.selectExpr('payload.policy_number as id',
                                              'raw_timestamp as updated_at',
                                              'payload.reason')

        df_result = df_result.withColumn('status', f.lit(self.status))

        df_result = df_result.withColumn('updated_at',
                                         f.from_unixtime(df_result.updated_at))

        return df_result
