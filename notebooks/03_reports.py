# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Config
catalog       = "telerik_u57_de_workspace"
schema        = "kostadin_kotev"
raw_data_path = "/Volumes/telerik_u57_de_workspace/techmart/raw_data"
notebook_path = "/Users/kostadin.kotev.u57@learn.telerikacademy.com/TechMart_SDP_Pipeline"

# COMMAND ----------

# DBTITLE 1,Create pipeline (Databricks SDK)
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary
import time

w = WorkspaceClient()

pipeline_name = "TechMart_SDP_Pipeline"

# Check if pipeline already exists
existing = [p for p in w.pipelines.list_pipelines() if p.name == pipeline_name]

if existing:
    pipeline_id = existing[0].pipeline_id
    print(f"✓ Pipeline already exists: {pipeline_id}")
else:
    created = w.pipelines.create(
        name=pipeline_name,
        catalog=catalog,
        target=schema,
        configuration={
            "catalog": catalog,
            "schema": schema,
            "raw_data_path": raw_data_path
        },
        libraries=[
            PipelineLibrary(notebook=NotebookLibrary(path=notebook_path))
        ],
        serverless=True,
        continuous=False,
    )
    pipeline_id = created.pipeline_id
    print(f"✓ Pipeline created: {pipeline_id}")
    print(f"  Monitor: {w.config.host}#joblist/pipelines/{pipeline_id}")

# COMMAND ----------

# DBTITLE 1,Create mv_sales_metrics
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW telerik_u57_de_workspace.kostadin_kotev.mv_sales_metrics
# MAGIC WITH METRICS LANGUAGE YAML AS $$
# MAGIC version: 1.1
# MAGIC comment: Sales KPIs over the medallion gold layer (as-of SCD2 attribution)
# MAGIC
# MAGIC source: >
# MAGIC   SELECT
# MAGIC     f.order_date,
# MAGIC     f.order_status,
# MAGIC     f.shipping_method,
# MAGIC     f.customer_id,
# MAGIC     f.product_id,
# MAGIC     f.order_id,
# MAGIC     f.quantity,
# MAGIC     f.line_total,
# MAGIC     f.discount_pct,
# MAGIC     dp.category,
# MAGIC     dp.subcategory,
# MAGIC     dp.brand,
# MAGIC     dp.price_tier,
# MAGIC     dc.country,
# MAGIC     dc.city
# MAGIC   FROM telerik_u57_de_workspace.kostadin_kotev.fact_order_items f
# MAGIC   LEFT JOIN telerik_u57_de_workspace.kostadin_kotev.dim_customers dc
# MAGIC     ON f.customer_id = dc.customer_id
# MAGIC     AND f.order_date >= dc.valid_from
# MAGIC     AND (f.order_date < dc.valid_to OR dc.valid_to IS NULL)
# MAGIC   LEFT JOIN telerik_u57_de_workspace.kostadin_kotev.dim_products dp
# MAGIC     ON f.product_id = dp.product_id
# MAGIC     AND f.order_date >= dp.valid_from
# MAGIC     AND (f.order_date < dp.valid_to OR dp.valid_to IS NULL)
# MAGIC
# MAGIC dimensions:
# MAGIC   - name: order_date
# MAGIC     expr: order_date
# MAGIC   - name: order_status
# MAGIC     expr: order_status
# MAGIC   - name: shipping_method
# MAGIC     expr: shipping_method
# MAGIC   - name: category
# MAGIC     expr: category
# MAGIC   - name: subcategory
# MAGIC     expr: subcategory
# MAGIC   - name: brand
# MAGIC     expr: brand
# MAGIC   - name: price_tier
# MAGIC     expr: price_tier
# MAGIC   - name: country
# MAGIC     expr: country
# MAGIC   - name: city
# MAGIC     expr: city
# MAGIC   - name: customer_id
# MAGIC     expr: customer_id
# MAGIC   - name: product_id
# MAGIC     expr: product_id
# MAGIC
# MAGIC measures:
# MAGIC   - name: total_revenue
# MAGIC     expr: SUM(line_total)
# MAGIC   - name: total_orders
# MAGIC     expr: COUNT(DISTINCT order_id)
# MAGIC   - name: total_items_sold
# MAGIC     expr: SUM(quantity)
# MAGIC   - name: avg_order_value
# MAGIC     expr: "SUM(line_total) / NULLIF(COUNT(DISTINCT order_id), 0)"
# MAGIC   - name: avg_discount
# MAGIC     expr: AVG(discount_pct)
# MAGIC $$;

# COMMAND ----------

# DBTITLE 1,Q1 - Monthly YoY Revenue
# MAGIC %sql
# MAGIC WITH monthly_revenue AS (
# MAGIC     SELECT
# MAGIC         YEAR(order_date) AS yr,
# MAGIC         MONTH(order_date) AS mo,
# MAGIC         MEASURE(total_revenue) AS revenue
# MAGIC     FROM telerik_u57_de_workspace.kostadin_kotev.mv_sales_metrics
# MAGIC     WHERE YEAR(order_date) IN (2024, 2025)
# MAGIC     GROUP BY yr, mo
# MAGIC )
# MAGIC SELECT
# MAGIC m25.mo AS month,
# MAGIC m24.revenue AS total_revenue_2024,
# MAGIC m25.revenue AS total_revenue_2025,
# MAGIC ROUND((m25.revenue - m24.revenue) / m24.revenue * 100, 2) AS yoy_pct
# MAGIC FROM monthly_revenue m25
# MAGIC JOIN monthly_revenue m24
# MAGIC ON m25.mo = m24.mo AND m25.yr = 2025 AND m24.yr = 2024
# MAGIC ORDER BY m25.mo;

# COMMAND ----------

# DBTITLE 1,Q2 - Category YoY Growth
# MAGIC %sql
# MAGIC WITH category_revenue AS (
# MAGIC     SELECT
# MAGIC     YEAR(order_date) AS yr,
# MAGIC     MEASURE(total_items_sold) as items_sold,
# MAGIC     category
# MAGIC     FROM telerik_u57_de_workspace.kostadin_kotev.mv_sales_metrics
# MAGIC     WHERE YEAR(order_date) IN (2024, 2025)
# MAGIC     GROUP BY yr, category
# MAGIC )
# MAGIC SELECT
# MAGIC c25.category AS category,
# MAGIC c24.items_sold AS items_sold_2024,
# MAGIC c25.items_sold AS items_sold_2025,
# MAGIC ROUND((c25.items_sold - c24.items_sold) / c24.items_sold * 100, 2) AS yoy_pct
# MAGIC FROM category_revenue c25
# MAGIC JOIN category_revenue c24
# MAGIC ON c25.category = c24.category AND c25.yr = 2025 AND c24.yr = 2024
# MAGIC ORDER BY c25.category; 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Q3 - Country Performance YoY
# MAGIC %sql
# MAGIC WITH country_revenue AS (
# MAGIC     SELECT
# MAGIC     YEAR(order_date) AS yr,
# MAGIC     MEASURE(total_revenue) as revenue,
# MAGIC     country
# MAGIC     FROM telerik_u57_de_workspace.kostadin_kotev.mv_sales_metrics
# MAGIC     WHERE YEAR(order_date) IN (2024, 2025)
# MAGIC     GROUP BY yr, country
# MAGIC )
# MAGIC SELECT
# MAGIC c25.country,
# MAGIC c24.revenue AS total_revenue_2024,
# MAGIC c25.revenue AS total_revenue_2025,
# MAGIC ROUND((c25.revenue - c24.revenue) / c24.revenue * 100, 2) AS yoy_pct
# MAGIC FROM country_revenue c25
# MAGIC JOIN country_revenue c24
# MAGIC ON c25.country = c24.country AND c25.yr = 2025 AND c24.yr = 2024
# MAGIC ORDER BY c25.country;
# MAGIC
# MAGIC

# COMMAND ----------


