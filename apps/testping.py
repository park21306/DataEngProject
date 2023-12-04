from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pandas as pd
from elasticsearch import Elasticsearch,helpers
spark = SparkSession.builder.appName("faker").getOrCreate()
es = Elasticsearch('http://172.18.0.2:9200') 
df = (
    pd.read_csv("/opt/spark/apps/data.csv")
    .dropna()
)
# mappings = {
#         "properties": {
#             "name": {"type": "text"},
#             "email": {"type": "text"},
#             "address": {"type": "text"},
#             "date of birth": {"type": "date"}
#     }
# }

# es.indices.create(index="name",mappings=mappings)
# print(es.ping())

for i, row in df.iterrows():
    doc = {
        "name": row["name"],
        "email": row["email"],
        "address": row["address"],
        "date of birth": row["date_of_birth"]
    }    
    es.index(index="kit", id=i, document=doc)
print(es.ping())