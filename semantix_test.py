"""
Created on Dec 10, 2018

@author: makquel
Script de análise de um Web Server log para teste da Semantix

Foram utilizadas as seguintes referências para o desenvolvimento:
http://adataanalyst.com/spark/web-server-log-analysis-spark/
https://spark.apache.org/docs/latest/quick-start.html#where-to-go-from-here
http://spark.apache.org/downloads.html
https://blog.sicara.com/get-started-pyspark-jupyter-guide-tutorial-ae2fe84f594f
"""
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import split, regexp_extract
from pyspark.sql.functions import col, sum
from pyspark.sql.functions import udf
from pyspark.sql.functions import desc
from pyspark.sql.functions import dayofmonth

import sys
import os
import re
import datetime
import argparse



month_map = {
  'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
  'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12
}

def count_null(col_name):
  return sum(col(col_name).isNull().cast('integer')).alias(col_name)

def parse_clf_time(s):
    """ Convert Common Log time format into a Python datetime object
    Args:
        s (str): date and time in Apache time format [dd/mmm/yyyy:hh:mm:ss (+/-)zzzz]
    Returns:
        a string suitable for passing to CAST('timestamp')
    """
    # NOTE: We're ignoring time zone here. In a production application, you'd want to handle that.
    return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
      int(s[7:11]),
      month_map[s[3:6]],
      int(s[0:2]),
      int(s[12:14]),
      int(s[15:17]),
      int(s[18:20])
    )

conf = SparkConf().setMaster("local").setAppName("semantix_task")
sc = SparkContext(conf = conf)
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

base_df = sqlContext.read.text('access_log_Jul95')
#base_df = sqlContext.read.text('access_log_Aug95')


split_df = base_df.select(regexp_extract('value', r'^([^\s]+\s)', 1).alias('host'),
                          regexp_extract('value', r'^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]', 1).alias('timestamp'),
                          regexp_extract('value', r'^.*"\w+\s+([^\s]+)\s+HTTP.*"', 1).alias('path'),
                          regexp_extract('value', r'^.*"\s+([^\s]+)', 1).cast('integer').alias('status'),
                          regexp_extract('value', r'^.*\s+(\d+)$', 1).cast('integer').alias('content_size'))
#split_df.show(truncate=False)


##https://ragrawal.wordpress.com/2015/10/02/spark-custom-udf-example/
u_parse_time = udf(parse_clf_time)

logs_df = split_df.select('*', u_parse_time(split_df['timestamp']).cast('timestamp').alias('time')).drop('timestamp')
total_log_entries = logs_df.count()

#logs_df.printSchema()


# Number of unique hosts
unique_host_count = logs_df.select('host').distinct().count()
print ('Numero de host unicos: {0}'.format(unique_host_count))
#print "********** Unique hosts: %d" % unique_host_count

# Number of 404 error
not_found_df = logs_df.filter(logs_df['status'] == 404).cache()
#E404 = not_found_df.count()
print('Total de erros E404 {0}'.format(not_found_df.count()))

# DataFrame containing all accesses that returned 404 status
E404DF = logs_df.filter(logs_df['status'] == 404)
# Sorted DataFrame containing all paths and the number of times they were accessed with non-200 return code
logs_sum_df = (E404DF.groupBy('path').count().sort('count', ascending=False))
print ('Top 5 das URLs com E404:')
logs_sum_df.take(5)
logs_sum_df.show(5)


# 404 error per day
errors_by_date_sorted_df = not_found_df.withColumn('day',dayofmonth(not_found_df['time']))                                       .groupBy('day').count().sort('day')

print ('DF com a quatidade de E404 por dia:\n')
errors_by_date_sorted_df.show(31)	
