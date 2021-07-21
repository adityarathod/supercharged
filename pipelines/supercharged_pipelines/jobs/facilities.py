from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import column as trim

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
        trim("FACILITY_NAME").alias("facility_name"),
        trim("FACILITY_STREET_ADDRESS").alias("facility_street"),
        df["FACILITY_CITY"].alias("facility_city"),
        df["STATE_DESC"].alias("facility_state"),
        df["FACILITY_ZIP_CODE"].alias("facility_zip"),
        df["HRR_DESC"].alias("facility_region"),
    ).drop_duplicates(["provider_id", "facility_name", "facility_city"])


def load(df: DataFrame) -> None:
    df = df.coalesce(1)
    df.write.format("csv").option("header", True).option("quoteAll", True).mode(
        "overwrite"
    ).save("tmp/facilities")


def etl(file_name: str) -> None:
    df = extract(file_name)
    df = transform(df)
    load(df)
    print("Completed ETL job for facilities")
