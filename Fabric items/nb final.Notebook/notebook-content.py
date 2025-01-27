# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "49633d52-5992-4ab2-abdb-6fa12095c04c",
# META       "default_lakehouse_name": "LH_Harvester",
# META       "default_lakehouse_workspace_id": "a2fdbf7b-89ff-4006-9e9d-102ccb49ae4a"
# META     },
# META     "environment": {
# META       "environmentId": "d8d16304-82a4-a21a-47de-9d0e018d0943",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from lxml import etree as ET
import pandas as pd
from datetime import datetime
import time
from collections import Counter, defaultdict

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# file1 = "Files/20211001_gpx103-sdcgpx1852-47b994e1_2296_4d13_8b9e_e5ef5ed3449e-X1.12-9510015292_20211001_Rödvattenmyran.xml" 
# file2 = "Files/20211004_gpx103-sdcgpx1852-6c631266_d606_4bf2_b4b0_01071544d00a-X1.12-9510015292_20211004_Rödvattenmyran.xml"

apif1 = "/lakehouse/default/Files/20211001_gpx103-sdcgpx1852-47b994e1_2296_4d13_8b9e_e5ef5ed3449e-X1.12-9510015292_20211001_Rödvattenmyran.xml"
apif2 = "/lakehouse/default/Files/20211004_gpx103-sdcgpx1852-6c631266_d606_4bf2_b4b0_01071544d00a-X1.12-9510015292_20211004_Rödvattenmyran.xml"

element_list = ['Stem', 'ObjectDefinition', 'ProductDefinition', 'SpeciesGroupDefinition']

data = []
schema = 'bronze'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# removing namespace

def remove_namespace() :
    # Iterate through all XML elements
    for elem in root.getiterator():
        # Skip comments and processing instructions, because they do not have names
        if not (
            isinstance(elem, ET._Comment)
            or isinstance(elem, ET._ProcessingInstruction)
        ):
            # Remove a namespace URI in the element's name
            elem.tag = ET.QName(elem).localname

    # Remove unused namespace declarations
    ET.cleanup_namespaces(root)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_machine_key() :
    for i in range(len(root[1])) :
        if root[1][i].tag == 'MachineKey' :
            machine_key = root[1][i].text
    return machine_key

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# recursive function to parse the xml tree

def parse_element(element, item) :
    if len(list(element)) == 0:
        item[element.tag] = element.text    # attributes

    else:
        for child in list(element):
            parse_element (child, item)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# creating data list

def data_list(x) :
    for child in root[1][x]:
        item = {}
        # item = dd_list()
        parse_element(child, item)
        data.append(item)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# data list to dataframe

def list_to_dataframe(data):
    structured_data = []
    current_row = {}

    for entry in data:
        for key, value in entry.items():
            if key in current_row:
                # If the key already exists, save the current row and start a new one
                structured_data.append(current_row)
                current_row = {}
            current_row[key] = value

    # Append the last row
    if current_row:
        structured_data.append(current_row)

    # Convert the structured data into a DataFrame
    df = spark.createDataFrame(pd.DataFrame(structured_data).ffill().bfill())
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# final table creation with MachineKey

def create_table(index, table_name, schema_name = schema) :
    for i in index :
        data_list(i)
        df = list_to_dataframe(data)
        df_mk = df.withColumn("MachineKey", lit(fetch_machine_key()))
        df_mk = df_mk.select([col(c).cast(StringType()) for c in df_mk.columns])
        location = schema_name + '.' + table_name
        df_mk.write.format("delta").mode("append").option("mergeSchema", True).saveAsTable(location)
        data.clear()
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# reading file
tree = ET.parse(apif2)
root = tree.getroot()

# removing namespace
remove_namespace()

fetch_machine_key()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# creating index

Stem_index = []
ObjectDefinition_index = []
SpeciesGroupDefinition_index = []
ProductDefinition_index = []

for i in range(len(root[1])):
    if root[1][i].tag == 'Stem':
        Stem_index.append(i)
    elif root[1][i].tag == 'ObjectDefinition':
        ObjectDefinition_index.append(i)
    elif root[1][i].tag == 'SpeciesGroupDefinition':
        SpeciesGroupDefinition_index.append(i)
    elif root[1][i].tag == 'ProductDefinition':
        ProductDefinition_index.append(i)

print(len(Stem_index), len(ObjectDefinition_index), len(SpeciesGroupDefinition_index), len(ProductDefinition_index))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# root[1][24][0].text
data_list(24)
print(data)
df = list_to_dataframe(data)
display(df)
# df_mk = df.withColumn("MachineKey", lit(fetch_machine_key()))
data.clear()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# if spark.catalog.tableExists(f"dbo.{element_list[0]}"):
#   print(f"Table Exist {element_list[0]}")
# else:
#     create_table(stem_index, f'{element_list[0]}')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start = time.time()

if spark.catalog.tableExists("dbo.Stem"):
  print("Table Exist Stem")
else:
    create_table(Stem_index, 'Stem')
    print("Table Stem created")


if spark.catalog.tableExists("dbo.ObjectDefinition"):
  print("Table Exist ObjectDefinition")
else:
    create_table(ObjectDefinition_index, 'ObjectDefinition')
    print("Table ObjectDefinition created")


if spark.catalog.tableExists("dbo.SpeciesGroupDefinition"):
  print("Table Exist SpeciesGroupDefinition")
else:
    create_table(SpeciesGroupDefinition_index, 'SpeciesGroupDefinition')
    print("Table SpeciesGroupDefinition created")


if spark.catalog.tableExists("dbo.ProductDefinition"):
  print("Table Exist ProductDefinition")
else:
    create_table(ProductDefinition_index, 'ProductDefinition')
    print("Table ProductDefinition created")

end = time.time()

timediff = (end - start)
print(timediff)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(timediff/60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC USE SCHEMA bronze;
# MAGIC -- DROP TABLE IF EXISTS Stem
# MAGIC -- DROP TABLE IF EXISTS ObjectDefinition
# MAGIC -- DROP TABLE IF EXISTS SpeciesGroupDefinition
# MAGIC -- DROP TABLE IF EXISTS ProductDefinition
# MAGIC 
# MAGIC SELECT * FROM Stem;
# MAGIC SELECT * FROM ObjectDefinition;
# MAGIC SELECT * FROM SpeciesGroupDefinition;
# MAGIC SELECT * FROM ProductDefinition;
# MAGIC 
# MAGIC -- CREATE SCHEMA bronze

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM bronze.Stem where StemKey = '285808'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT MachineKey,
# MAGIC ObjectKey ,
# MAGIC ObjectUserID,
# MAGIC ObjectName,
# MAGIC ObjectModificationDate,
# MAGIC ForestCertification,
# MAGIC LoggingFormCode,
# MAGIC LoggingFormDescription,
# MAGIC -- ObjectArea,
# MAGIC -- LoggingOrganisationBusinessName,
# MAGIC -- LoggingOrganisationBusinessID,
# MAGIC -- LoggingOrganisationDistrict,
# MAGIC -- LoggingOrganisationTeam,
# MAGIC -- ForestOwnerBusinessName,
# MAGIC -- ForestOwnerBusinessID,
# MAGIC ContractNumber,
# MAGIC -- ContractCategory,
# MAGIC RealEstateIDObject,
# MAGIC -- TextFromMachine,
# MAGIC StartDate
# MAGIC -- EndDate
# MAGIC  FROM bronze.ObjectDefinition;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
