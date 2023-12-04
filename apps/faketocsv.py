from faker import Faker
import csv
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pandas as pd
from pathlib import Path 

spark = SparkSession.builder.appName("faker").getOrCreate()
fake=Faker()
header=['name','email','address','date of birth']
data=[]

for r in range(1000):
    data.append((fake.name(),fake.email(),fake.address(),fake.date_of_birth()))
    
df = pd.DataFrame(data)
df.columns = ['name', 'email' ,'address' , 'date_of_birth']
filepath = Path('/opt/spark/apps/data.csv')  
filepath.parent.mkdir(parents=True, exist_ok=True)  
df.to_csv(filepath,index=False) 