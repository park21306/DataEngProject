from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pandas as pd
from elasticsearch import Elasticsearch,helpers
spark = SparkSession.builder.appName("faker").getOrCreate()
es = Elasticsearch('http://172.18.0.2:9200') 

mappings = {
        "properties": {
            "name": {"type": "text"},
            "email": {"type": "text"},
            "address": {"type": "text"},
            "date of birth": {"type": "date"}
    }
}

es.indices.create(index="kit",mappings=mappings)
print(es.ping())