from supercharged_pipelines.config import PipelineConfig
from supercharged_pipelines.base.pipeline import Pipeline
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import regexp_replace, split
from pyspark.sql.types import IntegerType


class DRGPipeline(Pipeline):
    def __init__(self):
        super().__init__("drgs_pipeline")

    def extract(self, config: PipelineConfig) -> DataFrame:
        super().extract(config)
        return (
            self.spark.read.format("csv")
            .option("header", True)
            .option("inferSchema", True)
            .option("escape", '"')
            .load(config.input_csv_path)
        )

    def transform(self, df: DataFrame, config: PipelineConfig) -> DataFrame:
        super().transform(df, config)
        df2 = df.withColumn("_drg", split("DRG_DESC", " - "))
        df2 = df2.select(
            df2["_drg"].getItem(0).cast(IntegerType()).alias("id"),
            df2["_drg"].getItem(1).alias("description"),
        )
        df2 = df2.withColumn("description", regexp_replace("description", '"', ""))
        df2 = df2.drop_duplicates(["id"])
        return df2.sort("id")

    def load(self, df: DataFrame, config: PipelineConfig):
        super().load(df, config)
        df = df.coalesce(1)
        db_path = f"jdbc:sqlite:{config.output_db_path}"
        df.write.format("jdbc").mode("append").option("url", db_path).option(
            "dbtable", "drgs"
        ).option("truncate", "true").save()
        # df2.write.format("csv").option("header", True).option("quoteAll", True).mode(
        #     "overwrite"
        # ).save("tmp/drgs")
