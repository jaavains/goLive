import os
import sys
import logging

from pysparkling.sql.functions import col

from utl import getSpark,listHDFSDir
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as F
from py4j.protocol import Py4JJavaError
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType



#initialization
spark,conf,env,logger  = getSpark()
spark.sparkContext.setLogLevel("WARN")

if env=="itversity":
    print("No of files =", len(listHDFSDir(spark,"/user/itv001656/warehouse/pt.db/orders_kaggle")))

#Extraction
logger.info("Started reading the files.")

try:
    products = spark.read.csv(conf.data.products,
                              schema=StructType.fromJson(conf.schema.productsJ),
                              header=True)

    orders = spark.read.csv(conf.data.orders,header=True)

    allOrders = spark.read.csv(conf.data.allOrders,header=True)

    logger.info("Files read completed")
except Exception as e:
    logger.exception("Error in reading the dataset due to exception ",exc_info=e)
    exit(1)

# Join the datasets .
# Top ordered Product ?

try:
    output = orders.join(allOrders, "order_id", "inner") \
        .select("product_id") \
        .groupby("product_id") \
        .agg(F.count("product_id").alias("prod_count")) \
        .join(products, "product_id", "inner").select("product_name", "prod_count") \
        .orderBy(F.col("prod_count").desc()) \

    if env=="itversity":
        output\
        .coalesce(1)\
        .write.mode("overwrite").saveAsTable("pt.orders_kaggle")
        print("No of files =", len(listHDFSDir(spark,"/user/itv001656/warehouse/pt.db/orders_kaggle")))
    else:
        output.selectExpr("udf_nameLower(product_name)").show()
except AnalysisException as e :
    logger.exception("error while joining",e)
except Py4JJavaError as e:
    logger.exception("error in pyjava",e)

logger.info("Job exection completed")