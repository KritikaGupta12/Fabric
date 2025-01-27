# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d28da01a-33ce-4af0-953c-27bf7fcc495e",
# META       "default_lakehouse_name": "lakehouse_energy_kg",
# META       "default_lakehouse_workspace_id": "a2fdbf7b-89ff-4006-9e9d-102ccb49ae4a",
# META       "known_lakehouses": [
# META         {
# META           "id": "d28da01a-33ce-4af0-953c-27bf7fcc495e"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# # **Parameter**

# PARAMETERS CELL ********************

market_area = "BE"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Library imports**

# CELL ********************

from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup
from delta.tables import *
from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Scrapping functions**

# CELL ********************

def _to_float(v: str) -> float:
    return float(v.replace(",", ""))


def _as_date_str(v: date) -> str:
    return v.strftime("%Y-%m-%d")


def extract_invokes(data: dict[str, Any]) -> dict[str, Any]:
    invokes = {}
    for entry in data:
        if entry["command"] == "invoke":
            invokes[entry["selector"]] = entry
    return invokes


def fetch_data(delivery_date: date, market_area: str) -> dict[str, Any]:
    trading_date = delivery_date - timedelta(days=1)
    params = {
        "market_area": market_area,
        "trading_date": _as_date_str(trading_date),
        "delivery_date": _as_date_str(delivery_date),
        "modality": "Auction",
        "sub_modality": "DayAhead",
        "product": "60",
        "data_mode": "table",
        "ajax_form": 1,
    }
    data = {
        "form_id": "market_data_filters_form",
        "_triggering_element_name": "submit_js",
    }
    r = requests.post("https://www.epexspot.com/en/market-data", params=params, data=data)
    r.raise_for_status()
    return r.json()


def extract_table_data(delivery_date: datetime, data: dict[str, Any], market_area: str) -> pd.DataFrame:
    soup = BeautifulSoup(data["args"][0], features="html.parser")

    try:
        table = soup.find("table", class_="table-01 table-length-1")
        body = table.tbody
        rows = body.find_all_next("tr")
    except AttributeError:
        return []  # no data available

    start_time = delivery_date.replace(hour=0, minute=0, second=0, microsecond=0)

    # convert timezone to UTC (and adjust timestamp)
    start_time = start_time.astimezone(timezone.utc)

    records = []
    for row in rows:
        end_time = start_time + timedelta(hours=1)
        buy_volume_col = row.td
        sell_volume_col = buy_volume_col.find_next_sibling("td")
        volume_col = sell_volume_col.find_next_sibling("td")
        price_col = volume_col.find_next_sibling("td")
        records.append(
            (
                market_area,
                start_time,
                end_time,
                _to_float(buy_volume_col.string),
                _to_float(sell_volume_col.string),
                _to_float(volume_col.string),
                _to_float(price_col.string),
            )
        )
        start_time = end_time

    return pd.DataFrame.from_records(records, columns=["market", "start_time", "end_time", "buy_volume", "sell_volume", "volume", "price"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_day(delivery_date: datetime, market_area) -> pd.DataFrame:
    data = fetch_data(delivery_date.date(), market_area)
    invokes = extract_invokes(data)

    # check if there is an invoke command with selector ".js-md-widget"
    # because this contains the table with the results
    table_data = invokes.get(".js-md-widget")
    if table_data is None:
        # no data for this day
        return []
    return extract_table_data(delivery_date, table_data, market_area)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **First Run and partitioning strategies**

# CELL ********************

prices = fetch_day(datetime.now() + timedelta(days=1), market_area)
if len(prices) == 0:
    mssparkutils.notebook.exit("No prices available yet")

spark_df = spark.createDataFrame(prices)

spark_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").partitionBy("market").saveAsTable("epex_spot_prices")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Subsequent runs**

# CELL ********************

current = DeltaTable.forName(spark, "epex_spot_prices")
current_df = current.toDF()
current_df = current_df.withColumn("date", to_date(col("start_time")))
current_market_count = current_df.filter((current_df.market == market_area) & (current_df.date == _as_date_str(datetime.now() + timedelta(days=1)))).count()
if current_market_count == 24:
    mssparkutils.notebook.exit("Already ingested")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prices = fetch_day(datetime.now() + timedelta(days=1), market_area)
if len(prices) == 0:
    mssparkutils.notebook.exit("No prices available yet")

spark_df = spark.createDataFrame(prices)

current.alias("current").merge(spark_df.alias("new"), "current.market = new.market AND current.start_time = new.start_time AND current.end_time = new.end_time").whenNotMatchedInsertAll().execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
