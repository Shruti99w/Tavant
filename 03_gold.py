# =============================================================================
# Notebook 03 - GOLD LAYER
# Lakehouse: lh_sales_medallion
# Purpose:   Shape Silver into consumer-ready tables. Star schema for BI
#            (dim_*, fact_*) plus a couple of aggregated marts for fast
#            dashboards. Optimized for Direct Lake in Power BI.
# =============================================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -----------------------------------------------------------------------------
# 1. Read Silver
# -----------------------------------------------------------------------------
df_s = spark.read.table("silver_sales")

# =============================================================================
# DIMENSION TABLES
# =============================================================================

# -----------------------------------------------------------------------------
# 1.1 dim_product - one row per (ProductName, Size). Surrogate key assigned.
# -----------------------------------------------------------------------------
w_prod = Window.orderBy("ProductName", "Size")

dim_product = (
    df_s.select("ProductName", "Size").distinct()
        .withColumn("ProductKey", F.row_number().over(w_prod))
        .select("ProductKey", "ProductName", "Size")
)

(dim_product.write.format("delta").mode("overwrite")
    .option("overwriteSchema","true").saveAsTable("dim_product"))

# -----------------------------------------------------------------------------
# 1.2 dim_customer - one row per customer (by email as natural key)
# -----------------------------------------------------------------------------
w_cust = Window.orderBy("EmailAddress")

dim_customer = (
    df_s.select("CustomerName", "EmailAddress").distinct()
        .withColumn("CustomerKey", F.row_number().over(w_cust))
        .select("CustomerKey", "CustomerName", "EmailAddress")
)

(dim_customer.write.format("delta").mode("overwrite")
    .option("overwriteSchema","true").saveAsTable("dim_customer"))

# -----------------------------------------------------------------------------
# 1.3 dim_date - generated calendar covering the full Silver range
# -----------------------------------------------------------------------------
date_bounds = df_s.agg(
    F.min("OrderDate").alias("min_d"),
    F.max("OrderDate").alias("max_d")
).collect()[0]

dim_date = (
    spark.sql(f"""
        SELECT explode(
            sequence(
                to_date('{date_bounds['min_d']}'),
                to_date('{date_bounds['max_d']}'),
                interval 1 day
            )
        ) AS OrderDate
    """)
    .withColumn("DateKey",   F.date_format("OrderDate", "yyyyMMdd").cast("int"))
    .withColumn("Year",      F.year("OrderDate"))
    .withColumn("Month",     F.month("OrderDate"))
    .withColumn("MonthName", F.date_format("OrderDate", "MMMM"))
    .withColumn("Quarter",   F.quarter("OrderDate"))
    .withColumn("DayName",   F.date_format("OrderDate", "EEEE"))
    .select("DateKey","OrderDate","Year","Quarter","Month","MonthName","DayName")
)

(dim_date.write.format("delta").mode("overwrite")
    .option("overwriteSchema","true").saveAsTable("dim_date"))

# =============================================================================
# FACT TABLE
# =============================================================================

# -----------------------------------------------------------------------------
# 2. fact_sales - one row per sales order line, with surrogate keys joined in.
# -----------------------------------------------------------------------------
fact_sales = (
    df_s
    .join(dim_product,
          on=["ProductName", "Size"], how="left")
    .join(dim_customer,
          on=["CustomerName", "EmailAddress"], how="left")
    .join(dim_date.select("OrderDate", "DateKey"),
          on="OrderDate", how="left")
    .select(
        "SalesOrderNumber",
        "SalesOrderLineNumber",
        "DateKey",
        "CustomerKey",
        "ProductKey",
        "Quantity",
        "UnitPrice",
        "TaxAmount",
        "LineTotal",
        "GrossAmount"
    )
)

(fact_sales.write.format("delta").mode("overwrite")
    .option("overwriteSchema","true").saveAsTable("fact_sales"))

# =============================================================================
# AGGREGATED MARTS (optional, but great for fast dashboards)
# =============================================================================

# -----------------------------------------------------------------------------
# 3.1 Monthly sales mart
# -----------------------------------------------------------------------------
monthly_sales_mart = (
    df_s.groupBy("OrderYear", "OrderMonth")
        .agg(
            F.countDistinct("SalesOrderNumber").alias("OrderCount"),
            F.sum("Quantity").alias("UnitsSold"),
            F.round(F.sum("LineTotal"), 2).alias("Revenue"),
            F.round(F.sum("TaxAmount"), 2).alias("Tax"),
            F.round(F.sum("GrossAmount"), 2).alias("GrossAmount")
        )
        .orderBy("OrderYear", "OrderMonth")
)

(monthly_sales_mart.write.format("delta").mode("overwrite")
    .option("overwriteSchema","true").saveAsTable("mart_monthly_sales"))

# -----------------------------------------------------------------------------
# 3.2 Top products mart
# -----------------------------------------------------------------------------
top_products_mart = (
    df_s.groupBy("ProductName")
        .agg(
            F.countDistinct("SalesOrderNumber").alias("OrderCount"),
            F.sum("Quantity").alias("UnitsSold"),
            F.round(F.sum("LineTotal"), 2).alias("Revenue")
        )
        .orderBy(F.desc("Revenue"))
)

(top_products_mart.write.format("delta").mode("overwrite")
    .option("overwriteSchema","true").saveAsTable("mart_top_products"))

# -----------------------------------------------------------------------------
# 4. Optimize Gold tables (helps Direct Lake performance)
# -----------------------------------------------------------------------------
for t in ["dim_product","dim_customer","dim_date","fact_sales",
          "mart_monthly_sales","mart_top_products"]:
    spark.sql(f"OPTIMIZE {t}")

# -----------------------------------------------------------------------------
# 5. Validate
# -----------------------------------------------------------------------------
print(f"dim_product        : {spark.table('dim_product').count():,} rows")
print(f"dim_customer       : {spark.table('dim_customer').count():,} rows")
print(f"dim_date           : {spark.table('dim_date').count():,} rows")
print(f"fact_sales         : {spark.table('fact_sales').count():,} rows")
print(f"mart_monthly_sales : {spark.table('mart_monthly_sales').count():,} rows")
print(f"mart_top_products  : {spark.table('mart_top_products').count():,} rows")

print("\n--- Top 5 products by revenue ---")
spark.table("mart_top_products").show(5, truncate=False)

print("\n--- Monthly revenue (first 6 months) ---")
spark.table("mart_monthly_sales").show(6, truncate=False)
