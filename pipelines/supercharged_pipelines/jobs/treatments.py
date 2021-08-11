from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import split
from pyspark.sql.types import FloatType, IntegerType
from supercharged_pipelines.config import PipelineConfig
from supercharged_pipelines.base.pipeline import Pipeline


class TreatmentsPipeline(Pipeline):
    def __init__(self):
        super().__init__("treatments_pipeline")

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
        return df.select(
            df["provider_id"].alias("facility_id"),
            split("DRG_DESC", " - ").getItem(0).cast(IntegerType()).alias("drg_id"),
            df["DISCHARGE_COUNT_SUM"].cast(IntegerType()).alias("discharge_count"),
            df["MEAN_COVERED_CHARGES"].cast(FloatType()).alias("mean_covered_charges"),
            df["MEAN_MEDICARE_PAYMENTS"]
            .cast(FloatType())
            .alias("mean_medicare_payments"),
            df["MEAN_MEDICARE_REIMBURSEMENT"].alias("mean_medicare_reimburse"),
        ).sort(["facility_id", "drg_id"])

    def load(self, df: DataFrame, config: PipelineConfig):
        super().load(df, config)
        df = df.coalesce(1)
        db_path = f"jdbc:sqlite:{config.output_db_path}"
        df.write.format("jdbc").mode("append").option("url", db_path).option(
            "dbtable", "treatments"
        ).option("truncate", "true").save()
        # df.write.format("csv").option("header", True).option("quoteAll", True).mode(
        #     "overwrite"
        # ).save("tmp/treatments")
