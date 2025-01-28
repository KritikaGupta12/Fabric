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

print('PySpark Version :'+spark.version)
print('PySpark Version :'+spark.sparkContext.version)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("rowTag", "book").format("xml").load("Files/books.xml")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Libraries and files

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
# import xml.etree.ElementTree as ET
from lxml import etree as ET
import pandas as pd
from pyspark.sql.functions import *
# from_xml, schema_of_xml, lit, col

file1 = "Files/20211001_gpx103-sdcgpx1852-47b994e1_2296_4d13_8b9e_e5ef5ed3449e-X1.12-9510015292_20211001_Rödvattenmyran.xml" 
file2 = "Files/20211004_gpx103-sdcgpx1852-6c631266_d606_4bf2_b4b0_01071544d00a-X1.12-9510015292_20211004_Rödvattenmyran.xml"

apif1 = "/lakehouse/default/Files/20211001_gpx103-sdcgpx1852-47b994e1_2296_4d13_8b9e_e5ef5ed3449e-X1.12-9510015292_20211001_Rödvattenmyran.xml"
apif2 = "/lakehouse/default/Files/20211004_gpx103-sdcgpx1852-6c631266_d606_4bf2_b4b0_01071544d00a-X1.12-9510015292_20211004_Rödvattenmyran.xml"


# spark = SparkSession.builder \
#     .appName("harvester") \
#     .config("spark.jars.packages", "com.databricks:spark-xml_2.12-0.15.0") \
#     .getOrCreate()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # spark-xml

# CELL ********************

df = spark.read.option("rootTag", "Machine").option("rowTag", "Stem").format("xml").load(file2)
# df = spark.read.option("rootTag", "Stem").format("xml").load(file2)
display(df.select("StemBunchKey", "DBH"))
# list(df.columns)
# df.printSchema(1)


# df = spark.createDataFrame([(8, tree)], ["number", "payload"])
# schema = F.schema_of_xml(df.select("payload").limit(1).collect()[0][0])
# parsed = df.withColumn("parsed", F.from_xml(col("payload"), schema))
# parsed.printSchema()
# parsed.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Machine
ObjectDefinition
ProductDefinition
SpeciesGroupDefinition


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # XML ETree

# MARKDOWN ********************

# ## XML ET functions

# CELL ********************

xml_data = """
<data date="2022-10-10">
<stock>
<name>Microsoft</name>
<ticker>MSFT</ticker>
<price>234.24</price>
<currency>USD</currency>
</stock>
<stock>
<name>Apple</name>
<ticker>APPL</ticker>
<price>140.09</price>
<currency>USD</currency>
</stock>
</data>
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parse XML into Element Tree
# tree = ET.parse("xml_data.xml")
# root = tree.getroot()
root = ET.fromstring(xml_data)

# Root
# print(root.tag)
# print(len(root))  # gives the no. of child elements

# Access Root Child
# print(root[0].tag)    # accessing the first child of root
# print(len(root[0]))   # gives the no. of direct elements of the first child of root

# Loop over Root Children
for child in root:  # for the first child under root
    print(child[0].tag)
    print(child[0].text)

for child in root:  # for the second child under root
    print(child[2].tag)
    print(child[2].text)

# Root Grandchildren
# print(root[0][0].tag) # name
# print(root[0][0].text)
# print(root[0][1].tag) # ticker
# print(root[0][1].text)
# print(root[1][2].tag) # price
# print(root[1][2].text)

# Get an Attribute
print(root.attrib)

# Loop over 'Ticker' Tag
# for ticker in root.iter('ticker'):
#   print(ticker.text)

# Find Children with 'price' Tag - it works only for the direct child below it
# print(len(root[0].findall('price')))  # accessing the first child after root
# print(root[0].findall('price')[0].text)

# Change Price & Remove Currency
# root[0][2].text ="233"
# root[1].remove(root[1][3])
# tree.write('xml_file.xml')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## XML namespace

# CELL ********************

# xml with namespace
ET.register_namespace("","urn:skogforsk:stanford2010")
ns = {"": "urn:skogforsk:stanford2010"}
# ET.dump(tree)


# need to use the API address
tree = ET.parse(apif2)
root = tree.getroot()
ET.dump(tree)


for x in root.findall('.//StemKey',ns):
    print(x.text)

root.findall(".", ns)

# ET.register_namespace("topo","http://www.topografix.com/GPX/1/1")
# ET.dump(tree)
# ns = {"topo": "http://www.topografix.com/GPX/1/1"}
# for x in root.findall('.//topo:time',ns):
# print(x.text)

# for x in root.findall('.//{*}time'):
# print(x.text)

print("root.tag :", root.tag)
print("root.attrib : ")
root.attrib

for child in root:
    print(child.tag, child.attrib)

root[0][1].text

for neighbor in root.iter('OperatorDefinition'):
    print(neighbor.attrib)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Removing namespace

# CELL ********************

# reading file
tree = ET.parse(apif2)
root = tree.getroot()

# Iterate through all XML elements
for elem in root.getiterator():
    # Skip comments and processing instructions,
    # because they do not have names
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

print("root tag - ", root.tag)
print("no. of root elements - ", len(root))
print("1st child of root - ", root[0])
print(root[0].tag, "- no. of elements - ", len(root[0]))
print("2nd child of root - ", root[1])
print(root[1].tag, "- no. of elements - ", len(root[1]))
list(root)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

element_list = ['ObjectDefinition', 'ProductDefinition', 'SpeciesGroupDefinition']

machine_key = 'MachineKey'

# column list

# root[1][14]
stem_list = ['DBH', 'ReferenceDiameter', 'StemKey', 'ObjectKey', 'SpeciesGroupKey', 'OperatorKey', 'HarvestDate', 'StemNumber', 'ProcessingCategory',
 'StumpTreatment']

# for child in root[1]:
#     element_list.append(child.tag)
# print(set(element_list)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

element_count = []
index = []
for i in range(len(root[1])):
    if len(root[1][i]) != 0 :
        element_count.append(len(root[1][i]))
        index.append(i)

print(index)
print(element_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Function

# CELL ********************

# initializing empty list
data = []

# recursive function to parse the xml tree
def parse_element(element, item) :
    if len(list(element)) == 0:
        item[element.tag] = element.text

    else:
        for child in list(element):
            parse_element (child, item)


# function to parse till first level
# def parse_element1(element, item):
#     item[element.tag] = element.text
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# iterate through the xml tree and parse the elements
index = []
for i in range(len(root[1])):
    if len(root[1][i]) != 0 :
        index.append(i)

n = 12
for i in index:
    for child in root[1][i]:
        item = {}
        parse_element(child, item)
        data.append(item)

    df_pd = pd.DataFrame(data)
    df = spark.createDataFrame(df_pd)
    display(df)
    # print(data)
    
    data.clear()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for child in root[1][24]:
    item = {}
    parse_element(child, item)
    data.append(item)

df_pd = pd.DataFrame(data)
df1 = spark.createDataFrame(df_pd)

# display(df1.select([c for c in stem_list]))
display(df1.select([first(x, ignorenulls=True).alias(x) for x in df1.columns]))

data.clear()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_pd = pd.DataFrame(data)
df = spark.createDataFrame(df_pd)
display(df)

# Spark to Pandas
# df_pd = df.toPandas()

# Pandas to Spark
# df_sp = spark_session.createDataFrame(df_pd)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ls = list(df.columns)
'StemNumber' in ls

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select(
# 'StemBunchKey',
'DBH',
# 'ReferenceDiameter',
# 'ReferenceDiameterHeight',
'MachineKey',
'StemKey', 
'ObjectKey', 
'SpeciesGroupKey', 
'OperatorKey',
'HarvestDate',
# 'BioEnergyAdaption',
'StemNumber',
'ProcessingCategory',
# 'StemCode',
'StumpTreatment'
).where(col('DBH').isNotNull()).limit(10).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
