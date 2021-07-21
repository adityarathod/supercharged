from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import column as col, regexp_replace, split, trim
from pyspark.sql.types import IntegerType

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
    df2 = df.withColumn("_drg", split("DRG_DESC", " - "))
    df2 = df2.select(
        df2["_drg"].getItem(0).cast(IntegerType()).alias("drg_id"),
        df2["_drg"].getItem(1).alias("drg_desc"),
    )
    df2 = df2.withColumn("drg_desc", regexp_replace("drg_desc", '"', ""))
    df2 = df2.drop_duplicates(["drg_id"])
    return df2.sort("drg_id")


def load(df: DataFrame) -> None:
    df2 = df.coalesce(1)
    df2.write.format("csv").option("header", True).option("quoteAll", True).mode(
        "overwrite"
    ).save("tmp/drgs")


def etl(file_name: str) -> None:
    df = extract(file_name)
    df = transform(df)
    load(df)
    print("Completed ETL job for DRGs")
