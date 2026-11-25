"""r2x spike S3: a plain Python file in the process repository, imported by the totals step.
Calculations are plain functions so they are easy to test (RX-25)."""
from pyspark.sql import DataFrame, functions as F


def totals_by_region(sales: DataFrame, min_amount: float = 0) -> DataFrame:
    return (sales.where(F.col("amount") >= F.lit(min_amount))
                 .groupBy("region")
                 .agg(F.sum("amount").cast("decimal(18,2)").alias("total")))
