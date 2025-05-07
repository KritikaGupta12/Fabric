# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8afaea64-4d77-49bc-a1c7-82e719ddd896",
# META       "default_lakehouse_name": "oilprices",
# META       "default_lakehouse_workspace_id": "a2fdbf7b-89ff-4006-9e9d-102ccb49ae4a",
# META       "known_lakehouses": [
# META         {
# META           "id": "8afaea64-4d77-49bc-a1c7-82e719ddd896"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from bs4 import BeautifulSoup
import requests
import smtplib
import time
import datetime
from datetime import date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

url = 'https://economictimes.indiatimes.com/commoditysummary/symbol-CRUDEOIL.cms'
headers = ({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"})


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

page = requests.get(url, headers=headers)
page

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# page.content

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

soup = BeautifulSoup(page.content, 'html.parser')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

type(page.content)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# print(soup)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

price = soup.find("span", class_="commodityPrice").get_text()
print(price)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# price_num = "".join([ele for ele in price if ele.isdigit()])
# print(price_num)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

today = date.today()
print("Today's date : ", today," ", type(today))


# IST timezone
todaytime = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)
print("Current timestamp : ", todaytime," ", type(todaytime))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row
df = spark.createDataFrame([
    Row(Date = today, DateTime = todaytime, Price = price)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# display(df)
# df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import IntegerType, DoubleType
df2 = df.withColumn("Price", df.Price.cast(DoubleType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df2.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

delta_table_path = 'Tables/OilPrices'
df2.write.format("delta").mode("append").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
