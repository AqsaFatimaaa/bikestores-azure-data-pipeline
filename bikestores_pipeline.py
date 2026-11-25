# Databricks notebook source
# MAGIC %md
# MAGIC # BikeStores Pipeline: Bronze to Silver to Gold
# MAGIC End-to-end PySpark code for the Medallion architecture.
# MAGIC
# MAGIC **Flow:** SQL Server -> Azure Data Factory -> ADLS Gen2 (Bronze CSV) -> Databricks (Silver, Gold Delta) -> Power BI
# MAGIC
# MAGIC > Replace `<storage-account>` with your own storage account name.
# MAGIC > Never hard-code keys or tokens. Use a Databricks secret scope.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup: connection and paths

# COMMAND ----------

from pyspark.sql import functions as F

storage_account = "<storage-account>"
container = "bikestores"

# Authenticate with a key stored in a secret scope (never paste the key here)
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="<your-scope>", key="<your-key-name>"),
)

base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
bronze = f"{base}/bronzelayer"
silver = f"{base}/silverlayer"
gold = f"{base}/goldlayer"

# Tables in the two source schemas
tables = {
    "production": ["brands", "categories", "products", "stocks"],
    "sales": ["customers", "orders", "order_items", "staffs", "stores"],
}

# Check that Bronze looks as expected
display(dbutils.fs.ls(bronze))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze: raw CSV files (written by Azure Data Factory)
# MAGIC Bronze is never modified. Layout: `bronzelayer/<schema>/<table>/`

# COMMAND ----------

for schema, names in tables.items():
    for t in names:
        print(f"{schema}/{t}")
        display(dbutils.fs.ls(f"{bronze}/{schema}/{t}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver: cleaned Delta tables
# MAGIC Cleaning rules applied to every table:
# MAGIC 1. snake_case column names
# MAGIC 2. trim text and turn empty strings into null
# MAGIC 3. convert `*_date` columns to dates
# MAGIC 4. remove duplicate rows

# COMMAND ----------

def clean(df):
    # 1. snake_case column names
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip().lower().replace(" ", "_"))
    # 2. trim text and turn empty strings into nulls
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(
                c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c)))
            )
    # 3. convert date columns
    for c in df.columns:
        if c.endswith("_date"):
            df = df.withColumn(c, F.to_date(F.col(c)))
    # 4. remove duplicates
    return df.dropDuplicates()


for schema, names in tables.items():
    for t in names:
        raw = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(f"{bronze}/{schema}/{t}")
        )
        cleaned = clean(raw)
        (
            cleaned.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", True)
            .save(f"{silver}/{schema}_{t}")
        )
        print(f"Silver written: {schema}_{t}  ({cleaned.count()} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gold: business-ready tables
# MAGIC Reads Silver, joins and aggregates, then saves Delta tables for Power BI.

# COMMAND ----------

def s(name):
    """Read one Silver table."""
    return spark.read.format("delta").load(f"{silver}/{name}")


items = s("sales_order_items")
orders = s("sales_orders")
customers = s("sales_customers")
stores = s("sales_stores")
products = s("production_products")
categories = s("production_categories")
brands = s("production_brands")

# One wide fact table: every order line with its product, category, brand, customer and store
sales_fact = (
    items.alias("oi")
    .join(orders.alias("o"), "order_id")
    .join(products.alias("p"), "product_id")
    .join(categories.alias("c"), "category_id")
    .join(brands.alias("b"), "brand_id")
    .join(customers.alias("cu"), "customer_id")
    .join(stores.alias("s"), "store_id")
    .select(
        F.col("order_id"),
        F.col("o.order_date").alias("order_date"),
        F.col("o.order_status").alias("order_status"),
        F.col("s.store_name").alias("store_name"),
        F.col("p.product_name").alias("product_name"),
        F.col("p.model_year").alias("model_year"),
        F.col("c.category_name").alias("category_name"),
        F.col("b.brand_name").alias("brand_name"),
        F.concat_ws(" ", F.col("cu.first_name"), F.col("cu.last_name")).alias("customer_name"),
        F.col("oi.quantity").alias("quantity"),
        F.col("oi.list_price").alias("list_price"),
        F.col("oi.discount").alias("discount"),
    )
    # revenue = quantity x price x (1 - discount)
    .withColumn("revenue", F.col("quantity") * F.col("list_price") * (1 - F.col("discount")))
)

# Revenue and orders per store
sales_by_store = sales_fact.groupBy("store_name").agg(
    F.round(F.sum("revenue"), 2).alias("revenue"),
    F.countDistinct("order_id").alias("orders"),
    F.sum("quantity").alias("units_sold"),
)

# Best-selling products by revenue
top_products = (
    sales_fact.groupBy("product_name", "model_year")
    .agg(F.round(F.sum("revenue"), 2).alias("revenue"), F.sum("quantity").alias("units_sold"))
    .orderBy(F.col("revenue").desc())
)

# Revenue trend by month
monthly_revenue = (
    sales_fact.withColumn("year", F.year("order_date"))
    .withColumn("month", F.month("order_date"))
    .groupBy("year", "month")
    .agg(F.round(F.sum("revenue"), 2).alias("revenue"), F.countDistinct("order_id").alias("orders"))
    .orderBy("year", "month")
)

gold_tables = {
    "sales_fact": sales_fact,
    "sales_by_store": sales_by_store,
    "top_products": top_products,
    "monthly_revenue": monthly_revenue,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Save Gold (Delta files and tables for Power BI)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS bikestores_gold")

for name, df in gold_tables.items():
    # Delta files in the data lake
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{gold}/{name}")
    # Table that Power BI reads through the Databricks SQL warehouse
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(
        f"bikestores_gold.{name}"
    )
    print(f"Gold saved: {name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Quick checks

# COMMAND ----------

display(spark.table("bikestores_gold.sales_by_store"))
display(spark.table("bikestores_gold.monthly_revenue"))
display(spark.sql("SELECT ROUND(SUM(revenue), 2) AS total_revenue, COUNT(DISTINCT order_id) AS total_orders FROM bikestores_gold.sales_fact"))
