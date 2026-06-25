# Databricks notebook source
# DBTITLE 1,Config
base_path = "/Volumes/telerik_u57_de_workspace/techmart/raw_data"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM read_files('/Volumes/telerik_u57_de_workspace/techmart/raw_data/customers', format => 'csv') LIMIT 1000;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM intro_kostadin_kotev_u57_learn_telerikacademy_com.silver_members LIMIT 1000;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM read_files('/Volumes/telerik_u57_de_workspace/techmart/raw_data/orders', format => 'csv') LIMIT 1000;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM read_files('/Volumes/telerik_u57_de_workspace/techmart/raw_data/products', format => 'csv') LIMIT 1000;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) as total_rows,
# MAGIC        count(distinct customer_id) as unique_customers
# MAGIC FROM read_files('/Volumes/telerik_u57_de_workspace/techmart/raw_data/customers', format => 'csv') LIMIT 1000;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM read_files('/Volumes/telerik_u57_de_workspace/techmart/raw_data/customers', format => 'csv')
# MAGIC WHERE city IS NULL;

# COMMAND ----------

# DBTITLE 1,Casing issues across columns
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN first_name != LOWER(first_name) AND first_name = UPPER(first_name) THEN 1 ELSE 0 END) AS first_name_all_upper,
# MAGIC   SUM(CASE WHEN last_name != LOWER(last_name) AND last_name = UPPER(last_name) THEN 1 ELSE 0 END) AS last_name_all_upper,
# MAGIC   SUM(CASE WHEN city != LOWER(city) AND city = UPPER(city) THEN 1 ELSE 0 END) AS city_all_upper,
# MAGIC   SUM(CASE WHEN country != LOWER(country) AND country = UPPER(country) THEN 1 ELSE 0 END) AS country_all_upper,
# MAGIC   SUM(CASE WHEN email != LOWER(email) THEN 1 ELSE 0 END) AS email_has_upper,
# MAGIC   COUNT(*) AS total_rows
# MAGIC FROM read_files('/Volumes/telerik_u57_de_workspace/techmart/raw_data/customers', format => 'csv');

# COMMAND ----------

# DBTITLE 1,Uppercase emails check
# MAGIC %sql
# MAGIC
# MAGIC SELECT customer_id, email
# MAGIC FROM read_files('/Volumes/telerik_u57_de_workspace/techmart/raw_data/customers', format => 'csv')
# MAGIC WHERE email != LOWER(email);

# COMMAND ----------

# DBTITLE 1,Fully uppercase countries
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM read_files('/Volumes/telerik_u57_de_workspace/techmart/raw_data/order_items', format => 'csv')
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM read_files('/Volumes/telerik_u57_de_workspace/techmart/raw_data/orders', format => 'csv') LIMIT 5;

# COMMAND ----------

# DBTITLE 1,Sample duplicate orders
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM read_files('/Volumes/telerik_u57_de_workspace/techmart/raw_data/orders', format => 'csv')
# MAGIC WHERE order_id IN (
# MAGIC   SELECT order_id
# MAGIC   FROM read_files('/Volumes/telerik_u57_de_workspace/techmart/raw_data/orders', format => 'csv')
# MAGIC   GROUP BY order_id
# MAGIC   HAVING COUNT(*) > 1
# MAGIC   LIMIT 5
# MAGIC )
# MAGIC ORDER BY order_id;
