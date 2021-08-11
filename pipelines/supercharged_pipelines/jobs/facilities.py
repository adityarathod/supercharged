from supercharged_pipelines.config import PipelineConfig
from supercharged_pipelines.base.pipeline import Pipeline
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import trim


class FacilitiesPipeline(Pipeline):
    def __init__(self):
        super().__init__("facilities_pipeline")

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
        return (
            df.select(
                df["provider_id"].alias("id"),
                trim("FACILITY_NAME").alias("name"),
                trim("FACILITY_STREET_ADDRESS").alias("street"),
                df["FACILITY_CITY"].alias("city"),
                df["STATE_DESC"].alias("state"),
                df["FACILITY_ZIP_CODE"].alias("zip"),
                df["HRR_DESC"].alias("region"),
            )
            .drop_duplicates(["id", "name", "city"])
            .sort(["id"])
        )

    def load(self, df: DataFrame, config: PipelineConfig):
        super().load(df, config)
        df = df.coalesce(1)
        db_path = f"jdbc:sqlite:{config.output_db_path}"
        df.write.format("jdbc").mode("append").option("url", db_path).option(
            "dbtable", "facilities"
        ).option("truncate", "true").save()
        # df.write.format("csv").option("header", True).option("quoteAll", True).mode(
        #     "overwrite"
        # ).save("tmp/facilities")
