from abc import ABC, abstractmethod
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

class BaseContext(ABC):
    def __init__(self):
        self.spark = SparkSession.builder \
                                 .appName(self.app_name) \
                                 .master('local') \
                                 .config('spark.jars',
                                         '/Users/Jonas/git_storage/desafio_youse/parte_2/libs/sqlite-jdbc-3.32.3.2.jar') \
                                 .getOrCreate()


    def transformation(self, data):
        return data

    def save(self, data):
        return 'success'
