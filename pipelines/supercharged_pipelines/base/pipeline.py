from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from ..config import PipelineConfig


class Pipeline:
    spark: SparkSession
    job_name: str

    def __init__(self, job_name="generic_pipeline"):
        self.job_name = job_name
        self.spark = None

    def __ensure_spark(self):
        if not self.spark:
            self.spark = SparkSession.builder.appName(self.job_name).getOrCreate()

    def extract(self, config: PipelineConfig) -> DataFrame:
        self.__ensure_spark()

    def transform(self, df: DataFrame, config: PipelineConfig) -> DataFrame:
        self.__ensure_spark()

    def load(self, df: DataFrame, config: PipelineConfig):
        self.__ensure_spark()

    def run(self, config: PipelineConfig):
        df = self.extract(config)
        df = self.transform(df, config)
        self.load(df, config)
        print("Completed job", self.job_name)
