from abc import ABC, abstractmethod
import traceback
import sqlite3

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

class BaseContext(ABC):
    def __init__(self, file_path):
        self.spark = SparkSession.builder \
                                 .appName(self.app_name) \
                                 .master('local') \
                                 .config('spark.jars',
                                         'libs/sqlite-jdbc-3.32.3.2.jar') \
                                 .getOrCreate()

        self.input = self.spark.read.json(file_path)

    @abstractmethod
    def process(self):
        return

    @abstractmethod
    def transformation(self, data):
        return

    def append_table(self, data, table):
        try:
            data.write.jdbc(url='jdbc:sqlite:datalake.db',
                            table=table,
                            mode='append',
                            properties={"driver":"org.sqlite.JDBC"})
        except Exception:
            traceback.print_exc()
            return 'Error'

        return 'Success'

    def update_table(self, data, table):
        columns = [col for col in data.columns if col != 'id']
        set_clause = ', '.join([f'{col} = '+'{'+col+'}' for col in columns])
        try:
            conn = sqlite3.connect('datalake.db')
            cur = conn.cursor()
            for row in data.collect():
                kwarg_set_clause = {col: row[col]
                                        if type(row[col]) is not str
                                        else "'" + row[col] + "'"
                                    for col in columns}
                query = f"""
UPDATE {table}
SET {set_clause.format(**kwarg_set_clause)}
WHERE id = '{row.id}'"""
                print(query)
                cur.execute(query)
            conn.commit()
        except Exception:
            traceback.print_exc()
            return 'Error'
        return 'Success'
