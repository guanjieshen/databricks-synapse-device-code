# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC 
# MAGIC The following steps need to be configured on Azure before the users can login using Azure AD Device Code Flow:
# MAGIC </br>
# MAGIC ##### Azure Active Directory Configuration 
# MAGIC 1. Create a new App Resigration on Azure Active Directory.
# MAGIC 2. Set `Allow public client flows` to `Yes` in `Advanced Settings` under the `Authenication Blade`.
# MAGIC 3. Add the `Azure SQL Database` API Permissions in the `API permissions` blade. This can be found under the `APIs my organization uses` subsection.
# MAGIC 4. Select `Delegated permissions` and then check off `user_impersonation`.
# MAGIC 
# MAGIC ##### Azure Synapse (Dedicated Pools) Configuration 
# MAGIC 1. Add users as External Providers `CREATE USER [user@company_domain.com] FROM EXTERNAL PROVIDER;` 
# MAGIC 2. Grant them the appropriate permissions `EXEC sp_addrolemember 'db_datareader', 'user@company_domain.com'; `
# MAGIC 
# MAGIC 
# MAGIC ##### Databricks Cluster Configuration 
# MAGIC Ensure the following libraries are installed on the cluster which is running this notebook:
# MAGIC 
# MAGIC </br>
# MAGIC 
# MAGIC - [Microsoft Authentication Library (MSAL) for Python](https://pypi.org/project/msal/)
# MAGIC   - PyPI Package: [`https://pypi.org/project/msal/`](https://pypi.org/project/msal/)
# MAGIC - [Apache Spark Connector for SQL Server and Azure SQL](https://github.com/microsoft/sql-spark-connector)
# MAGIC   - Maven Package Coordinate: `com.microsoft.azure:spark-mssql-connector_2.12:1.2.0`

# COMMAND ----------

# MAGIC %md ### [Do Not Modify] Notebook Setup
# MAGIC 
# MAGIC 
# MAGIC Please do not make any changes to the following section.

# COMMAND ----------

dbutils.widgets.text("client_id", "00000-00000-00000-00000", "AAD Client ID")
dbutils.widgets.text("tenant_id", "00000-00000-00000-00000", "AAD Tenant ID")
dbutils.widgets.text("scope", "https://windows.database.net/.default", "AAD Azure SQL Scope")
dbutils.widgets.text("server_url", "example-synapse.sql.azuresynapse.net","Synapse Server URL")

# COMMAND ----------

# MAGIC %md The following builds the MSAL client for auth.

# COMMAND ----------

import json
import msal

client_id = dbutils.widgets.get("client_id")
tenant_id = dbutils.widgets.get("tenant_id")
scope = dbutils.widgets.get("scope")
server_url = dbutils.widgets.get("server_url")

authority_url = f"https://login.microsoftonline.com/{tenant_id}"
synapse_url = f"jdbc:sqlserver://{server_url}:1433;"

print(f"""
Client ID : {client_id}
Tenant ID : {tenant_id}
Azure SQL Scope : {scope}
Synapse Server  : {server_url}

Authority URL : {authority_url}
Synapse JDBC URL  : {synapse_url}
""")

app = msal.PublicClientApplication(client_id, authority=authority_url)

# COMMAND ----------

# MAGIC %md The following function automatically gets the access token for the user, and caches it.

# COMMAND ----------

def get_access_token():
  result = None
  accounts = app.get_accounts()

  if accounts:
      chosen = accounts[0]
      result = app.acquire_token_silent(scopes=[scope], account=chosen)

  if not result:

      flow = app.initiate_device_flow(scopes=[scope])
      if "user_code" not in flow:
          raise ValueError(
              "Fail to create device flow. Err: %s" % json.dumps(flow, indent=4))

      print(flow["message"])
      result = app.acquire_token_by_device_flow(flow) 

  if "access_token" in result:
    print("User already is authenticated!")
    return result["access_token"]
  else:
      print(result.get("error"))
      print(result.get("error_description"))
      print(result.get("correlation_id"))  # You may need this when reporting a bug

# COMMAND ----------

# MAGIC %md ### [User Input Required] Connect to Synapse
# MAGIC 
# MAGIC Please run all the cells above, before running code below.

# COMMAND ----------

# MAGIC %md Use the following variables to adjust the database name or table name that should be extracted. 

# COMMAND ----------

database_name = "demo"
table_name = "dbo.TableName"

# COMMAND ----------

# MAGIC %md The following code returns the data from Synapse in DataFrame called `synapse_table`.

# COMMAND ----------

# This first line of code authenicates the user with Synapse using Device Code Auth
access_token = get_access_token()

synapse_table = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", f"{synapse_url}database={database_name}") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("dbtable", table_name) \
        .option("accessToken", access_token) \
        .option("encrypt", "true") \
        .option("hostNameInCertificate", "*.sql.azuresynapse.net") \
        .load()

display(synapse_table)

# COMMAND ----------

# MAGIC %md We can also push down a SQL query, if we only need part of a table.

# COMMAND ----------

database_name = "demo"
sql_query = """
  SELECT * FROM dbo.TableName where Id = 1
"""

# COMMAND ----------

# This first line of code authenicates the user with Synapse using Device Code Auth
access_token = get_access_token()

synapse_query = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", f"{synapse_url}database={database_name}") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("query", sql_query) \
        .option("accessToken", access_token) \
        .option("encrypt", "true") \
        .option("hostNameInCertificate", "*.sql.azuresynapse.net") \
        .load()

display(synapse_query)

# COMMAND ----------

# MAGIC %md ### [User Input Required] Persist Data within Databricks.
# MAGIC 
# MAGIC Once the data has been loaded into the Notebook, we can then store this data within Databricks.

# COMMAND ----------

synapse_table.write.format("delta").saveAsTable("guanjie_shen_db.table_name")
