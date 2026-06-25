# Databricks notebook source
# DBTITLE 1,Imports + Config
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window

## Configuring raw data path
raw_data_path = spark.conf.get("raw_data_path")

## Configuring catalog and schema
catalog = spark.conf.get("catalog")
schema = spark.conf.get("schema")

# COMMAND ----------

# DBTITLE 1,Raw ingestion via Autoloader (bronze_customers)
@dp.table(
    name = "bronze_customers",
    comment = "Raw data ingestion via autoloader"
)
def bronze_customers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{raw_data_path}/customers")
        .select("*",
                F.col("_metadata.file_name").alias("source_file_name"),
                F.col("_metadata.file_modification_time").alias("source_file_modification_timestamp"),
                F.current_timestamp().alias("ingestion_timestamp")
                )
    )

# COMMAND ----------

# DBTITLE 1,Raw ingestion via Autoloader (bronze_products)
@dp.table(
    name = "bronze_products",
    comment = "Raw data ingestion via autoloader"
)
def bronze_products():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{raw_data_path}/products")
        .select("*",
                F.col("_metadata.file_name").alias("source_file_name"),
                F.col("_metadata.file_modification_time").alias("source_file_modification_timestamp"),
                F.current_timestamp().alias("ingestion_timestamp")
                )
    )

# COMMAND ----------

# DBTITLE 1,Raw ingestion via Autoloader (bronze_orders)
@dp.table(
    name = "bronze_orders",
    comment = "Raw data ingestion via autoloader"
)
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{raw_data_path}/orders")
        .select("*",
                F.col("_metadata.file_name").alias("source_file_name"),
                F.col("_metadata.file_modification_time").alias("source_file_modification_timestamp"),
                F.current_timestamp().alias("ingestion_timestamp")
                )
    )


# COMMAND ----------

# DBTITLE 1,Raw ingestion via Autoloader (bronze_order_items)
@dp.table(
    name = "bronze_order_items",
    comment = "Raw data ingestion via autoloader"
)
def bronze_order_items():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{raw_data_path}/order_items")
        .select("*",
                F.col("_metadata.file_name").alias("source_file_name"),
                F.col("_metadata.file_modification_time").alias("source_file_modification_timestamp"),
                F.current_timestamp().alias("ingestion_timestamp")
                )
    )


# COMMAND ----------

# DBTITLE 1,Customer rules + quarantine expression
customers_rules = {
    "valid_city": "city IS NOT NULL AND TRIM(city) != ''"
}

customers_quarantine_rules = "NOT({0})".format(" AND ".join(customers_rules.values()))

# COMMAND ----------

@dp.table(
    name = "customers_quarantine",
    comment = "Data quality quarantine for customers table",
    temporary = True,
    partition_cols = ["is_quarantined"]
    )
@dp.expect_all(customers_rules)
def customers_quarantine():
    return (
       spark.readStream.table("bronze_customers")
       .withColumn("is_quarantined", F.expr(customers_quarantine_rules))
    )

# COMMAND ----------

# DBTITLE 1,silver_customers_good + silver_customers_bad
@dp.table(
    name = "silver_customers_good",
    comment = "Valid customers — city filter applied, all emails normalised",
)
def silver_customers_good():
    return (
        spark.readStream.table("customers_quarantine")
        .filter("is_quarantined = false")
        .select(
            "customer_id",
            F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("full_name"),
            F.lower(F.col("email")).alias("email"),
            "city",
            "country",
            F.col("signup_date").cast("date").alias("signup_date"),
            F.col("changed_at").cast("timestamp").alias("changed_at"),
        )
    )

@dp.table(
    name = "silver_customers_bad",
    comment = "Invalid customers that failed quality check",
    )
def silver_customers_bad():
    return (
        spark.readStream.table("customers_quarantine")
        .filter("is_quarantined = true")
    )

# COMMAND ----------

# DBTITLE 1,Product rules + quarantine expression
products_rules = {
    "valid_product_id": "product_id IS NOT NULL",
    "valid_unit_price": "unit_price IS NOT NULL AND unit_price >= 0",
}

products_quarantine_rules = "NOT({0})".format(" AND ".join(products_rules.values()))


@dp.table(
    name = "products_quarantine",
    comment = "Data quality quarantine for products table",
    temporary = True,
    partition_cols = ["is_quarantined"]
    )
@dp.expect_all(products_rules)
def products_quarantine():
    return (
       spark.readStream.table("bronze_products")
       .withColumn("is_quarantined", F.expr(products_quarantine_rules))
    )


@dp.table(
    name = "silver_products_bad",
    comment = "Invalid products that failed quality checks",
    )
def silver_products_bad():
    return (
        spark.readStream.table("products_quarantine")
        .filter("is_quarantined = true")
    )


@dp.table(
    name = "silver_products",
    comment = "Valid products — quality-gated"
)
def silver_products():
    return (
        spark.readStream.table("products_quarantine")
        .filter("is_quarantined = false")
        .select(
            "product_id",
            "product_name",
            "category",
            "subcategory",
            "brand",
            F.col("unit_price").cast("decimal(10,2)").alias("unit_price"),
            F.col("changed_at").cast("timestamp").alias("changed_at")
        )
    )

# COMMAND ----------

# DBTITLE 1,Order rules + quarantine expression
orders_rules = {
    "valid_order_id": "order_id IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_total_amount": "total_amount IS NOT NULL AND total_amount >= 0",
}

orders_quarantine_rules = "NOT({0})".format(" AND ".join(orders_rules.values()))


@dp.table(
    name = "orders_quarantine",
    comment = "Data quality quarantine for orders table",
    temporary = True,
    partition_cols = ["is_quarantined"]
    )
@dp.expect_all(orders_rules)
def orders_quarantine():
    return (
       spark.readStream.table("bronze_orders")
       .withColumn("is_quarantined", F.expr(orders_quarantine_rules))
    )


@dp.table(
    name = "silver_orders_bad",
    comment = "Invalid orders that failed quality checks",
    )
def silver_orders_bad():
    return (
        spark.readStream.table("orders_quarantine")
        .filter("is_quarantined = true")
    )


@dp.table(
    name = "_scd1_orders",
    comment = "Valid orders (quality-gated) — SCD1 source",
    temporary = True
)
def silver_orders():
    return (
        spark.readStream.table("orders_quarantine")
        .filter("is_quarantined = false")
        .select(
            "order_id",
            "customer_id",
            F.col("order_date").cast("timestamp").alias("order_date"),
            "order_status",
            "shipping_method",
            F.col("total_amount").cast("decimal(12,2)").alias("total_amount"),
            F.col("created_at").cast("timestamp").alias("created_at")
        )
    )

dp.create_streaming_table(
    name="silver_orders",
    comment="Deduplicated silver orders — keeps latest version per order_id"
)

dp.create_auto_cdc_flow(
    target="silver_orders",
    source="_scd1_orders",
    keys=["order_id"],
    sequence_by="created_at",
    stored_as_scd_type=1
)

# COMMAND ----------

# DBTITLE 1,silver_order_items
# DBTITLE 1,Order item rules + quarantine expression
order_items_rules = {
    "valid_order_item_id": "order_item_id IS NOT NULL",
    "valid_order_id": "order_id IS NOT NULL",
    "valid_product_id": "product_id IS NOT NULL",
    "valid_quantity": "quantity IS NOT NULL AND quantity > 0",
    "valid_unit_price": "unit_price IS NOT NULL AND unit_price >= 0",
    "valid_discount_pct": "discount_pct IS NOT NULL AND discount_pct BETWEEN 0 AND 100",
}

order_items_quarantine_rules = "NOT({0})".format(" AND ".join(order_items_rules.values()))


@dp.table(
    name = "order_items_quarantine",
    comment = "Data quality quarantine for order_items table",
    temporary = True,
    partition_cols = ["is_quarantined"]
    )
@dp.expect_all(order_items_rules)
def order_items_quarantine():
    return (
       spark.readStream.table("bronze_order_items")
       .withColumn("is_quarantined", F.expr(order_items_quarantine_rules))
    )


@dp.table(
    name = "silver_order_items_bad",
    comment = "Invalid order items that failed quality checks",
    )
def silver_order_items_bad():
    return (
        spark.readStream.table("order_items_quarantine")
        .filter("is_quarantined = true")
    )


@dp.materialized_view(
    name="silver_order_items",
    comment="Cleansed order items — quality-gated and filtered to valid orders"
)
def silver_order_items():
    return spark.sql("""
        SELECT
            order_item_id,
            order_id,
            product_id,
            CAST(quantity AS INT) AS quantity,
            CAST(unit_price AS DECIMAL(10,2)) AS unit_price,
            CAST(discount_pct AS INT) AS discount_pct,
            CAST(created_at AS TIMESTAMP) AS created_at
        FROM order_items_quarantine
        WHERE is_quarantined = false
          AND order_id IN (SELECT order_id FROM silver_orders)
    """)

# COMMAND ----------

# DBTITLE 1,Cell 12

dp.create_streaming_table(
    name = "_scd2_customers",
    comment = "Implementing SCD2 for silver_customers",
    private = True,
)
dp.create_auto_cdc_flow(
    target = "_scd2_customers",
    source = "silver_customers_good",
    keys = ["customer_id"],
    sequence_by = "changed_at",
    stored_as_scd_type = 2
)
@dp.materialized_view(
    name = "dim_customers",
    comment = "SCD2 dim_customers table"
)
def dim_customers():
    w = Window.partitionBy("customer_id").orderBy("__START_AT")
    return (
        spark.read.table("_scd2_customers")
        .withColumn("row_num", F.row_number().over(w))
        .withColumn("customer_sk", F.xxhash64(F.col("customer_id"), F.col("__START_AT").cast("string")))
        .withColumn("valid_from",
            F.when(F.col("row_num") == 1, F.col("signup_date").cast("timestamp"))
             .otherwise(F.col("__START_AT"))
        )
        .withColumn("valid_to", F.col("__END_AT"))
        .withColumn("is_current", F.col("__END_AT").isNull())
        .select(
            "customer_sk",
            "customer_id",
            "full_name",
            "email",
            "city",
            "country",
            "signup_date",
            "valid_from",
            "valid_to",
            "is_current"
        )
    )

# COMMAND ----------

dp.create_streaming_table(
    name = "_scd2_products",
    comment = "Implementing SCD2 for silver_products",
    private = True,
)
dp.create_auto_cdc_flow(
    target = "_scd2_products",
    source = "silver_products",
    keys = ["product_id"],
    sequence_by = "changed_at",
    stored_as_scd_type = 2
)
@dp.materialized_view(
    name = "dim_products",
    comment = "SCD2 dim_products table"
)
def dim_products():
    return (
        spark.read.table("_scd2_products")
        .withColumn("product_sk", F.xxhash64(F.col("product_id"), F.col("__START_AT").cast("string")))
        .withColumn("valid_from", F.col("__START_AT"))
        .withColumn("valid_to", F.col("__END_AT"))
        .withColumn("is_current", F.col("__END_AT").isNull())
        .withColumn("price_tier", 
                    F.when(F.col("unit_price") < 100, "Budget")
                    .when(F.col("unit_price") < 500, "Mid")
                    .when(F.col("unit_price") < 1000, "Premium")
                    .otherwise("Luxury")
                    )
        .select(
            "product_sk",
            "product_id",
            "product_name",
            "category",
            "subcategory",
            "brand",
            "unit_price",
            "price_tier",
            "valid_from",
            "valid_to",
            "is_current"
        )
    )
        

# COMMAND ----------

# DBTITLE 1,fact_order_items
@dp.materialized_view(
    name = "fact_order_items",
    comment = "Fact table — star schema with surrogate keys"
)
def fact_order_items():
    order_items = spark.read.table("silver_order_items").alias("oi")
    orders = spark.read.table("silver_orders").select(
        "order_id", "customer_id", "order_date", "order_status", "shipping_method"
    ).alias("o")
    customers = spark.read.table("dim_customers").alias("dc")
    products = spark.read.table("dim_products").alias("dp")

    return (
        order_items
        .join(orders, "order_id", "inner")
        .join(
            customers,
            (F.col("o.customer_id") == F.col("dc.customer_id"))
            & (F.col("o.order_date") >= F.col("dc.valid_from"))
            & ((F.col("o.order_date") < F.col("dc.valid_to")) | F.col("dc.valid_to").isNull()),
            "left"
        )
        .join(
            products,
            (F.col("oi.product_id") == F.col("dp.product_id"))
            & (F.col("o.order_date") >= F.col("dp.valid_from"))
            & ((F.col("o.order_date") < F.col("dp.valid_to")) | F.col("dp.valid_to").isNull()),
            "left"
        )
        .withColumn("fact_order_item_sk", F.xxhash64(F.col("oi.order_item_id").cast("string")))
        .select(
            "fact_order_item_sk",
            F.coalesce(F.col("dc.customer_sk"), F.lit(-1)).alias("customer_sk"),
            F.coalesce(F.col("dp.product_sk"), F.lit(-1)).alias("product_sk"),
            F.col("oi.order_item_id"),
            F.col("oi.order_id"),
            F.col("o.customer_id"),
            F.col("oi.product_id"),
            F.col("o.order_date"),
            F.col("o.order_status"),
            F.col("o.shipping_method"),
            F.col("oi.quantity"),
            F.col("oi.unit_price"),
            F.col("oi.discount_pct"),
            F.round(F.col("oi.quantity") * F.col("oi.unit_price") * (1 - F.col("oi.discount_pct") / 100), 2).alias("line_total")
        )
    )
