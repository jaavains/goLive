import os
import socket
import json

from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType



class dicdot(dict):
    '''
    This class is used to convert dict keys to dict attributes ,so we can call them using a dot.
    As of now only getattr is enabled.
    '''
    def __getattr__(self, key):
        item = self[key]
        if isinstance(item, dict):
            if key.endswith('D'):
                return dict(item)
            elif key.endswith('J'):
                return item              ## outputs json
                #return json.dumps(item) ## outputs as a string
            else:
                return dicdot(item)
        else:
            return item


def configjson(fs="config.json"):
    '''
    Takes a json config file & returns a dicdot object .
    :param fs:  config.json file
    :return:  dicdot object
    '''
    with open(fs) as f:
        return dicdot(json.loads(f.read()))


env = "pyspark" if "AKARYAMP" in socket.gethostname() else "itversity"
conf = configjson().__getattr__(env)


def sparksubmitconf(m=conf.submitConfD):
    '''
    Spark Context needs a lidt of tuples
    :param m:
    :return:
    '''
    return list(zip(m.keys(), m.values()))


def listhdfsdir(spark,hdfsPath):
    '''
    A method to check if a directory exists in HDFS
    :param spark: Spark session
    :param hdfsPath: A Valid path in HDFS
    :return:  List of files.
    '''
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    list_status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(hdfsPath))
    return [str(file.getPath()) for file in list_status]  # note string.



def namelower(s):
    return s.lower()


def getspark():
    '''
    Creates and returns a spark session object.
    Also registers any UDFs mentioned in config.json
    :return:  Spark session.
    '''
    sconf = SparkConf()
    sconf.setAll(pairs=sparksubmitconf())
    spark = SparkSession \
        .builder \
        .config(conf=sconf) \
        .enableHiveSupport() \
        .getOrCreate()

    customUDFs = conf.udf

    if len(customUDFs)!=0:
        for f in customUDFs:
            spark.udf.register("udf_"+f,globals()[f])
    return spark, conf , env, jvmlogging(spark)



def jvmlogging(spark):
    '''
    Logging at JVM
    :param spark:  spark session object
    :return:  logger
    '''
    logManager = spark._jvm.org.apache.log4j.LogManager
    return logManager.getLogger(conf.job.name)


# Dataframe related functions:
