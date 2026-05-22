# =============================================================================
# Notebook 02 - SILVER LAYER
# Lakehouse: lh_sales_medallion
# Purpose:   Cleanse, conform, enrich. This is where data engineering earns
#            its keep. Schemas enforced, derivations added, duplicates removed.
# =============================================================================

from pyspark.sql import functions as F

# -----------------------------------------------------------------------------
# 1. Read Bronze
# -----------------------------------------------------------------------------
df_b = spark.read.table("bronze_sales")

# -----------------------------------------------------------------------------
# 2. Conform types + split the Item field into ProductName and Size.
#    The Item field has two shapes:
#       - "Mountain-100 Silver, 44"   -> ProductName + Size
#       - "Water Bottle - 30 oz."     -> ProductName only, no size
#    We handle both with a single split + size check.
# -----------------------------------------------------------------------------
df_silver = (
    df_b
    # Drop bronze-only metadata before promoting to silver
    .drop("_ingest_ts", "_source_file")

    # Type casts
    .withColumn("OrderDate", F.to_date(F.col("OrderDate")))

    # Split Item -> ProductName + Size (Size = 'N/A' when no comma present)
    .withColumn("_parts", F.split(F.col("Item"), ", ", 2))
    .withColumn("ProductName", F.trim(F.col("_parts").getItem(0)))
    .withColumn(
        "Size",
        F.when(F.size("_parts") > 1, F.trim(F.col("_parts").getItem(1)))
         .otherwise(F.lit("N/A"))
    )
    .drop("_parts", "Item")

    # Business derivations
    .withColumn("LineTotal",   F.round(F.col("Quantity") * F.col("UnitPrice"), 2))
    .withColumn("GrossAmount", F.round(F.col("LineTotal") + F.col("TaxAmount"), 2))

    # Calendar derivations (handy for Silver consumers; Gold can re-derive)
    .withColumn("OrderYear",    F.year("OrderDate"))
    .withColumn("OrderMonth",   F.month("OrderDate"))
    .withColumn("OrderQuarter", F.quarter("OrderDate"))

    # Audit
    .withColumn("_silver_load_ts", F.current_timestamp())

    # Deduplicate on the natural primary key
    .dropDuplicates(["SalesOrderNumber", "SalesOrderLineNumber"])
)

# -----------------------------------------------------------------------------
# 3. Data Quality checks. Fail fast - never let bad data into Silver.
# -----------------------------------------------------------------------------
bad_pk_null  = df_silver.filter(
    F.col("SalesOrderNumber").isNull() | F.col("SalesOrderLineNumber").isNull()
).count()
bad_qty      = df_silver.filter(F.col("Quantity") <= 0).count()
bad_price    = df_silver.filter(F.col("UnitPrice") < 0).count()
bad_date     = df_silver.filter(F.col("OrderDate").isNull()).count()

print(f"DQ -> null PK rows : {bad_pk_null}")
print(f"DQ -> bad quantity : {bad_qty}")
print(f"DQ -> negative price: {bad_price}")
print(f"DQ -> bad dates    : {bad_date}")

assert bad_pk_null == 0, "Null PK found - aborting Silver write"
assert bad_qty     == 0, "Non-positive quantity found - aborting Silver write"
assert bad_price   == 0, "Negative price found - aborting Silver write"
assert bad_date    == 0, "Unparseable OrderDate found - aborting Silver write"

# -----------------------------------------------------------------------------
# 4. Write Silver as a managed Delta table
# -----------------------------------------------------------------------------
(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_sales")
)

# -----------------------------------------------------------------------------
# 5. Optimize for downstream Gold reads
# -----------------------------------------------------------------------------
spark.sql("OPTIMIZE silver_sales")

# -----------------------------------------------------------------------------
# 6. Validate
# -----------------------------------------------------------------------------
print(f"\nSilver row count: {spark.table('silver_sales').count():,}")
spark.table("silver_sales").select(
    "SalesOrderNumber","OrderDate","CustomerName","ProductName","Size",
    "Quantity","UnitPrice","LineTotal","GrossAmount"
).show(5, truncate=False)
