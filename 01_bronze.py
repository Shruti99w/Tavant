# =============================================================================
# Notebook 01 - BRONZE LAYER
# Lakehouse: lh_sales_medallion
# Purpose:   Land raw CSV files into a Delta table, append-only, no business
#            logic. Add ingestion metadata so we never lose provenance.
# =============================================================================

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# -----------------------------------------------------------------------------
# 1. Define schema explicitly. The CSVs have NO header row, so we must.
#    Senior-DE note: explicit schema beats inferSchema every time -
#    it is faster, reproducible, and fails loudly on drift.
# -----------------------------------------------------------------------------
sales_schema = StructType([
    StructField("SalesOrderNumber",     StringType(),  True),
    StructField("SalesOrderLineNumber", IntegerType(), True),
    StructField("OrderDate",            StringType(),  True),   # cast to date in Silver
    StructField("CustomerName",         StringType(),  True),
    StructField("EmailAddress",         StringType(),  True),
    StructField("Item",                 StringType(),  True),
    StructField("Quantity",             IntegerType(), True),
    StructField("UnitPrice",            DoubleType(),  True),
    StructField("TaxAmount",            DoubleType(),  True),
])

# -----------------------------------------------------------------------------
# 2. Read all three CSV files from the Files area of the lakehouse.
#    Wildcards work; each row is tagged with its source file later.
# -----------------------------------------------------------------------------
raw_path = "Files/raw/sales/*.csv"

df_raw = (
    spark.read
         .schema(sales_schema)
         .option("header", "false")
         .csv(raw_path)
)

# -----------------------------------------------------------------------------
# 3. Add Bronze metadata. This is the ONE thing we do at Bronze.
#    Everything else stays raw.
# -----------------------------------------------------------------------------
df_bronze = (
    df_raw
    .withColumn("_ingest_ts",   F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

# -----------------------------------------------------------------------------
# 4. Write as a managed Delta table. Overwrite for idempotent lab runs.
#    In production you would use mode("append") with a watermark.
# -----------------------------------------------------------------------------
(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("bronze_sales")
)

# -----------------------------------------------------------------------------
# 5. Validate
# -----------------------------------------------------------------------------
print(f"Bronze row count: {spark.table('bronze_sales').count():,}")
spark.table("bronze_sales").show(5, truncate=False)
spark.sql("""
    SELECT _source_file, COUNT(*) AS row_count
    FROM   bronze_sales
    GROUP  BY _source_file
    ORDER  BY _source_file
""").show(truncate=False)
