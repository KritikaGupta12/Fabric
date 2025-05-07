# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5ff59f22-5d9c-4992-8b7d-5a9c927967ff",
# META       "default_lakehouse_name": "AI_PoC",
# META       "default_lakehouse_workspace_id": "a2fdbf7b-89ff-4006-9e9d-102ccb49ae4a",
# META       "known_lakehouses": [
# META         {
# META           "id": "5ff59f22-5d9c-4992-8b7d-5a9c927967ff"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from itertools import chain

product_categories = ["tables", "chairs", "sofas-daybeds", "shelves", "beds"]
# product_categories = ["tables", "chairs"]

session = requests.Session()
# Headers to mimic a browser visit
headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36'
    }

retry_strategy = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)

# Base URL of the sofas category
def fetchBaseUrl(product_category):
    base_url = f'https://www.finnishdesignshop.com/en-us/furniture/{product_category}' 

    # Fetch the main category page
    response = session.get(base_url, headers=headers)
    # print(response.status_code)
    
    soup = BeautifulSoup(response.content, 'html.parser')
    # print(soup.prettify())
    return base_url

# Extract product links with Page loop
def fetchProductLinks(base_url, max_pages):
    product_links = []
    count = 0
    while base_url and count < max_pages:
        response = session.get(base_url, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract product links
        for a_tag in soup.find_all('a', class_='campaign product-card__link'):
            product_links.append(a_tag['href'])

        # Find "Next Page" button
        next_page = soup.find('a', {'aria-current': 'page'})

        if next_page:
            base_url = 'https://www.finnishdesignshop.com' + next_page['href']
        else:
            base_url = None  # Stop when no "Next Page" button is found
        count = count + 1
    return product_links

# Extracting final products
def finalProducts(link, product_category):
    time.sleep(0.5)
    product_url = link if link.startswith('http') else 'https://www.finnishdesignshop.com' + link
    product_response = session.get(product_url, headers=headers)
    product_soup = BeautifulSoup(product_response.content, 'html.parser')

    # product ID
    product_id_tag = product_soup.find('dt', string="Product ID")
    if product_id_tag: 
        product_id = product_id_tag.find_next('dd').get_text(strip=True) 
    else:
        product_id_tag = 'NA'
    # print(product_id)

    # company
    company_tag = product_soup.find('div', class_='product-name-container')
    if company_tag:
        product_company = company_tag.find('h2').get_text(strip=True)
    else:
        product_company = 'NA'
    # print(product_company)

    # title
    name_tag = product_soup.find('div', class_='product-name-container')
    if name_tag:
        product_name = name_tag.find('h1').get_text(strip=True)
    else:
        product_name = 'NA'
    # print(product_name)

    # price
    price_tag = product_soup.find('span', {'data-localized-price': True})
    # product_price = price_tag.get_text(strip=True) if price_tag else 'Price not available'
    if price_tag:
        product_price = price_tag['data-localized-price']
    else:
        product_price = 'NA'
    # print(product_price)

    # details
    details = {}
    details_section = product_soup.find('dl', class_='product-features-list')
    if details_section:
        dt_tags = details_section.find_all('dt')
        dd_tags = details_section.find_all('dd')

        for dt, dd in zip(dt_tags, dd_tags):
            key = dt.get_text(strip=True)
            value = dd.get_text(strip=True)
            details[key] = value
    details_str = '; '.join([f'{k}: {v}' for k, v in details.items()])  # Convert details dictionary to a string
    # print(details)

    return {
            'Product_Category': product_category,
            'Product_ID': product_id,
            'Product_Company': product_company,
            'Product_Name': product_name,
            'Product_Price': product_price,
            'Product_Details': details_str,
            'CreatedTS' : datetime.now()
        }

# Create csv file
def createRawFile(details, name):
    df = pd.DataFrame(details)
    date = datetime.today().strftime("%Y-%m-%d")
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")  # Example: 2025-02-09_14-30-15
    filename = f"abfss://a2fdbf7b-89ff-4006-9e9d-102ccb49ae4a@onelake.dfs.fabric.microsoft.com/5ff59f22-5d9c-4992-8b7d-5a9c927967ff/Files/data/{date}/{name}_{timestamp}.csv"
    df.to_csv(filename, index=False)

# main Function call 
all_details = []
max_pages = 100
for category in product_categories:
    base_url = fetchBaseUrl(category)
    print(f"Base URL for {category}:{base_url}")
    
    product_links = fetchProductLinks(base_url, max_pages)
    print(f"Total products found for {category}: {len(product_links)}")

    with ThreadPoolExecutor(max_workers = 500) as executor:
        product = list(executor.map(finalProducts, product_links, [category] * len(product_links)))
    print(f"Details collected for {category}")
     
    createRawFile(product, category)
    print(f"File created for {category}")
    all_details.append(product)

flatten = list(chain.from_iterable(all_details))
createRawFile(flatten, name='final')
print("Final file created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
