#!/usr/bin/env python
# coding: utf-8
import sys
import findspark
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import argparse
import calendar
import os
from datetime import date, timedelta, datetime, time
from dateutil.relativedelta import relativedelta

# Inicializa o Spark
findspark.init()

if __name__ == '__main__':
    DATADIR = '/projects/F202500001HPCVLABEPICURE/mca57510/adgd/PRJ-Cleaned-NoArray/'
    OUTPUT_DIR = '/projects/F202500001HPCVLABEPICURE/mca57510/adgd/DATA/'
    

    overall_start_time = datetime.now()

    print("FIND SPARK")
    print(findspark.find())
    
    sc = SparkSession.builder \
        .master("local[*]")\
        .config("spark.executor.memory", "20g")\
        .config("spark.driver.memory", "20g")\
        .config("spark.memory.offHeap.enabled", "true")\
        .config("spark.memory.offHeap.size", "16g")\
        .config("spark.executor.memoryOverhead", "8g")\
        .config("spark.driver.memoryOverhead", "8g")\
        .getOrCreate()

    nd = None

    nd = sc.read.parquet(f"{OUTPUT_DIR}/log-total.parquet")
    nd.printSchema()
    nd.show()

    print(f"Time to process all files:",  (datetime.now() - overall_start_time).total_seconds(), 'seconds')

    # Encerra a sessão do Spark
    sc.stop()
