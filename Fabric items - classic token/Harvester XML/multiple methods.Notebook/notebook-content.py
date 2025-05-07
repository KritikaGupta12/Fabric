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
from lxml import etree as ET
import pandas as pd
from collections import Counter
from pyspark.sql.functions import *

file1 = "Files/20211001_gpx103-sdcgpx1852-47b994e1_2296_4d13_8b9e_e5ef5ed3449e-X1.12-9510015292_20211001_Rödvattenmyran.xml" 
file2 = "Files/20211004_gpx103-sdcgpx1852-6c631266_d606_4bf2_b4b0_01071544d00a-X1.12-9510015292_20211004_Rödvattenmyran.xml"

apif1 = "/lakehouse/default/Files/20211001_gpx103-sdcgpx1852-47b994e1_2296_4d13_8b9e_e5ef5ed3449e-X1.12-9510015292_20211001_Rödvattenmyran.xml"
apif2 = "/lakehouse/default/Files/20211004_gpx103-sdcgpx1852-6c631266_d606_4bf2_b4b0_01071544d00a-X1.12-9510015292_20211004_Rödvattenmyran.xml"



element_list = ['Stem', 'ObjectDefinition', 'ProductDefinition', 'SpeciesGroupDefinition']
machine_key = 'MachineKey'

data = []
element_index = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

print(root[1][24].tag)
print(root[1][25].tag)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

elements = []
for i in range(len(root[1])):
    if root[1][i].tag in element_list :
        element_index.append(i)
        elements.append(root[1][i].tag)
        
print(len(element_index))


# print(list(set(elements)))
print(Counter(elements).keys()) # equals to list(set(words)) unique elements
print(Counter(elements).values()) # counts the elements' frequency

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Method 1

# CELL ********************

# list of dictionary

# initializing empty list
data = []

# recursive function to parse the xml tree
def parse_element(element, item) :
    if len(list(element)) == 0:
        item[element.tag] = element.text

    else:
        for child in list(element):
            parse_element (child, item)


# calling function  
for child in root[1]:
    item = {}
    parse_element(child, item)
    data.append(item)


# solutions

# df = pd.DataFrame.from_dict({0: {k: v for d in data
#                                  for k,v in d.items()}},
#                             orient='index')

# from collections import ChainMap
# df = pd.DataFrame.from_dict({0: dict(ChainMap(*data))},
#                             orient='index')

# df = pd.json_normalize(dict(ChainMap(*data)))
# df = pd.concat(map(pd.Series, data)).to_frame().T
# df = pd.DataFrame.from_dict({k: v for d in data for k, v in d.items()}, orient='index').T

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
    df = pd.DataFrame(structured_data).ffill().bfill()
    return df



df1 = spark.createDataFrame(list_to_dataframe(data))

# print(data)
display(df1)
# display(df1.select([c for c in stem_list]))
# display(df1.select([first(x, ignorenulls=True).alias(x) for x in df1.columns]))

data.clear()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Method 2

# CELL ********************

# recursive function to parse the xml tree
item = {}

def parse_element(element, item) :
    if len(list(element)) == 0:
        item[element.tag] = element.text
        # item.update({element.tag : element.text})

    else:
        for child in list(element):
            parse_element (child, item)



for child in root[1][24]:
    parse_element(child, item)
    # data.append(item)


item


# df = pd.DataFrame(item, index=[0])
df2 = spark.createDataFrame(pd.DataFrame(item, index=[0]))
display(df2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Method 3

# CELL ********************

from xml.etree import cElementTree as ElementTree

class XmlListConfig(list):
    def __init__(self, aList):
        for element in aList:
            if element:
                # treat like dict
                if len(element) == 1 or element[0].tag != element[1].tag:
                    self.append(XmlDictConfig(element))
                # treat like list
                elif element[0].tag == element[1].tag:
                    self.append(XmlListConfig(element))
            elif element.text:
                text = element.text.strip()
                if text:
                    self.append(text)


class XmlDictConfig(dict):
    def __init__(self, parent_element):
        if parent_element.items():
            self.update(dict(parent_element.items()))
        for element in parent_element:
            if element:
                # treat like dict - we assume that if the first two tags
                # in a series are different, then they are all different.
                if len(element) == 1 or element[0].tag != element[1].tag:
                    aDict = XmlDictConfig(element)
                # treat like list - we assume that if the first two tags
                # in a series are the same, then the rest are the same.
                else:
                    # here, we put the list in dictionary; the key is the
                    # tag name the list elements all share in common, and
                    # the value is the list itself 
                    aDict = {element[0].tag: XmlListConfig(element)}
                # if the tag has attributes, add those to the dict
                # if element.items():
                #     aDict.update(dict(element.items()))
                self.update({element.tag: aDict})
            # this assumes that if you've got an attribute in a tag,
            # you won't be having any text. This may or may not be a 
            # good idea -- time will tell. It works for the way we are
            # currently doing XML configuration files...
            # elif element.items():
            #     self.update({element.tag: dict(element.items())})
            # finally, if there are no child tags and no attributes, extract
            # the text
            else:
                self.update({element.tag: element.text})


xmldict = XmlDictConfig(root[1][24])
# xmldict
df = pd.DataFrame(xmldict)
# df2 = spark.createDataFrame(pd.DataFrame(xmldict))
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Method 4

# CELL ********************

# ET.tostring(root[1][24])
import xmltodict
def xml_to_dict(xml_data):
    #converting xml to dictionary
    dict_data = xmltodict.parse(xml_data)
    return dict_data

user_dict = xml_to_dict(ET.tostring(root[1][24]))

df = pd.DataFrame.from_dict(result_dict, orient='index')

# df = spark.createDataFrame(df)
display(df)
# df.printSchema()

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
# display(df1)
display(df1.select([first(x, ignorenulls=True).alias(x) for x in df1.columns]))

data.clear()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
