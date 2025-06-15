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
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml.feature import StringIndexer
import re

# Inicializa o Spark
findspark.init()

if __name__ == '__main__':
    DATADIR = '/projects/F202500001HPCVLABEPICURE/mca57510/adgd/PRJ-Cleaned-NoArray/'
    OUTPUT_DIR = '/projects/F202500001HPCVLABEPICURE/mca57510/adgd/DATA/'
    


    print("FIND SPARK")
    print(findspark.find())
    overall_start_time = datetime.now()
    
    sc = SparkSession.builder \
        .config("spark.executor.memory", "20g")\
        .config("spark.driver.memory", "20g")\
        .config("spark.sql.shuffle.partitions", "500")\
        .getOrCreate()


    nd = None
    
    for root, dirs, files in os.walk(DATADIR):
        files_sorted = sorted(files, key=lambda x: (x != "slurm.json", x))  
    slurm_path = os.path.join(DATADIR, "slurm.json")
    slurm_json = sc.read.json(slurm_path).select("_source.*", "_id")
    
    def expand_nodes(nodes_str):
        result = []
        # Pattern for multiple ranges -> cna[0003-0007,0010-0013]
        range_pattern = r"([a-zA-Z]+)\[([0-9,\\-]+)\]"
        
        # Match ranges and comma-separated values
        match = re.match(range_pattern, nodes_str)
        if match:
            prefix = match.group(1)  # cna
            ranges = match.group(2)  # 0003-0007,0010-0013
            
            # Split the ranges by commas and process each range
            for r in ranges.split(','):
                if '-' in r:  # It's a range -> 0003-0007
                    start, end = map(int, r.split('-'))
                    result.extend([f"{prefix}{i:04d}" for i in range(start, end + 1)])
                else:  # It's a single number, e.g., '0005'
                    result.append(f"{prefix}{int(r):04d}")
        else:
            # If it's a single node -> gnx502
            result.append(nodes_str)
        
        return result
    
    expand_nodes_udf = F.udf(expand_nodes, ArrayType(StringType()))

    slurm_json = slurm_json.withColumn("expanded_nodes", expand_nodes_udf(F.col("nodes")))
    
    slurm_json = slurm_json.withColumn("timestamp_start", F.to_timestamp("@start")).withColumn("timestamp_end", F.to_timestamp("@end"))
    
    file_path_pattern = os.path.join(DATADIR, "logstash-*.json")
    
    nd = sc.read.json(file_path_pattern).select("_source.*")

    nd = nd.withColumn("timestamp_log", F.to_timestamp("@timestamp")) \
       .withColumn("year", F.year("timestamp_log")) \
       .withColumn("month", F.month("timestamp_log")) \
       .withColumn("day", F.dayofmonth("timestamp_log")) \
       .withColumn("day_of_week", F.dayofweek("timestamp_log")) \
       .withColumn("hour", F.hour("timestamp_log")) \
       .withColumn("minute", F.minute("timestamp_log")) \
       .withColumn("second", F.second("timestamp_log"))

    nd.repartition(1000).cache()
    
    df_exploded_nodes = slurm_json.withColumn("expanded_node", F.explode(F.col("expanded_nodes")))

    df_exploded_nodes = df_exploded_nodes.repartition(1000).cache()

    df_joined = nd.join(F.broadcast(df_exploded_nodes), nd["host"] == df_exploded_nodes["expanded_node"], "inner")
    
    df_joined = df_joined.filter((df_joined["timestamp_log"] >= df_joined["timestamp_start"]) &
                                      (df_joined["timestamp_log"] <= df_joined["timestamp_end"]))
    
    df_final = df_joined.select(
    # "jobid", 
    "host", 
    "severity", 
    # "timestamp_log", 
    # "timestamp_start", 
    # "timestamp_end", 
    "year", 
    "month", 
    "day", 
    "day_of_week", 
    "hour", 
    "minute", 
    "second", 
    "state", 
    "cpu_hours", 
    "elapsed", 
    # "expanded_nodes", 
    "@queue_wait",    
    # "derived_ec",     # Isto é igual ao exit_code
    "exit_code",      
    "ntasks",        
    "partition",      
    "qos",            
    # "std_in",         
    # "std_out",        
    "time_limit",     
    "total_cpus",     
    "total_nodes"     
    )


    string_cols = [ 'severity', 'state', 'exit_code', 'qos']
    
    df_encoded = df_final.repartition(1000).cache()

    sample_df = df_encoded.sample(False, 0.01, seed=42)

    for col in string_cols:
        
        ## FULL CHECK
        # indexer = StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep")
        # df_encoded = indexer.fit(df_encoded).transform(df_encoded)
            
        ## SAMPLE CHECK
        indexer = StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep")
        indexer_model = indexer.fit(sample_df)
        df_encoded = indexer_model.transform(df_encoded)
        
        
    df_encoded = df_encoded.drop(*string_cols)
    
    # Show the result
    df_encoded.show(40,truncate=False)
    df_encoded.printSchema()
    
    ## CLASSES NUMBER CHECK
    # unique_states = df_encoded.select("state_index").distinct()

    # print("UNIQUE VALUES:")
    # unique_states.show(truncate=False)
    # num_unique_states = unique_states.count()
    
    # print(f"NUM CLASSES: {num_unique_states}")
    
    ## CLASSES DISTRIBUTION
    # df_encoded.groupBy("state_index").count().show(truncate=False)


    df_encoded.repartition(128).write.mode("overwrite").parquet(f"{OUTPUT_DIR}/speedupcheck_128.parquet")
    
    print(f"Time to process all files:",  (datetime.now() - overall_start_time).total_seconds(), 'seconds')

    # Encerra a sessão do Spark
    sc.stop()
