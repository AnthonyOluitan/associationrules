import numpy as np
import pandas as pd
from mlxtend.frequent_patterns import apriori, association_rules


from adls_api import download_all_data


# pull data from datalake
download_all_data()

store_data = pd.read_excel("data/OnlineRetail.xlsx")

print(store_data.head())
