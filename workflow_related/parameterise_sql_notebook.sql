-- Databricks notebook source
-- MAGIC %md
-- MAGIC <img src="https://databricks.com/wp-content/themes/databricks/assets/images/header_logo_2x.png" alt="logo" width="150"/> 
-- MAGIC 
-- MAGIC This tutorial shows how to parameterise sql statement in Databricks notebook.
-- MAGIC 
-- MAGIC We can create notebook widgets and use getArgument to fetch arguments for use across all languages.
-- MAGIC 
-- MAGIC Here we show a few examples:
-- MAGIC 
-- MAGIC 1. Create/Get/Remove widgets, here we create a drop down widget "X" and retrieve it in python or in SQL statement.
-- MAGIC 2. Create widget in SQL and to consume in SQL.

-- COMMAND ----------

-- DBTITLE 1,Create widgets through dbutils
-- MAGIC %python
-- MAGIC # create various types of widgets
-- MAGIC dbutils.widgets.dropdown("X", "1", [str(x) for x in range(1, 10)])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # get widget value
-- MAGIC dbutils.widgets.get("X")
-- MAGIC 
-- MAGIC # remove widgets (skipped in execution), you can also use removeAll()
-- MAGIC # dbutils.widgets.remove("X")

-- COMMAND ----------

-- DBTITLE 1,Create and consume parameters in SQL
CREATE WIDGET TEXT id DEFAULT 'Apple'

-- COMMAND ----------

CREATE TABLE if not exists test_table (
    id string,
    number string
);

SELECT * FROM test_table;

-- COMMAND ----------

INSERT INTO test_table
VALUES
('Jesse', 'A'),
('Nick', 'B'),
('Apple', 'C'),
('Orange', 'D')
;

-- COMMAND ----------

SELECT * FROM test_table WHERE id=getArgument("id")
--key=$y -- The old way of creating widgets in SQL queries with the $<parameter> syntax still works as before
