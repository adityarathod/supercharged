from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import column as trim, split
from pyspark.sql.types import FloatType, IntegerType

spark = SparkSession.builder.getOrCreate()


def extract(csv_path: str) -> DataFrame:
    return (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .option("escape", '"')
        .load(csv_path)
    )


def transform(df: DataFrame) -> DataFrame:
    return df.select(
        df["provider_id"],
        split("DRG_DESC", " - ").getItem(0).cast(IntegerType()).alias("drg_id"),
        df["DISCHARGE_COUNT_SUM"].cast(IntegerType()).alias("discharge_count"),
        df["MEAN_COVERED_CHARGES"].cast(FloatType()).alias("mean_covered_charges"),
        df["MEAN_MEDICARE_PAYMENTS"].case(FloatType()).alias("mean_medicare_payments"),
        df["MEAN_MEDICARE_REIMBURSEMENT"].alias("mean_medicare_reimburse"),
    ).sort(["provider_id", "drg_id"])


def load(df: DataFrame) -> None:
    df = df.coalesce(1)
    df.write.format("csv").option("header", True).option("quoteAll", True).mode(
        "overwrite"
    ).save("tmp/treatments")


def etl(file_name: str) -> None:
    df = extract(file_name)
    df = transform(df)
    load(df)
    print("Completed ETL job for treatments")
