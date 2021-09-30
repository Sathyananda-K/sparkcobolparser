from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import boto3
from botocore.exceptions import NoCredentialsError
from boto3.s3.transfer import TransferConfig

# Spark session setup
spark=SparkSession.\
    builder.\
    appName("ebcdic2csv").\
    config("spark.jars",
           "C:\\cobrixJars\\spark-cobol_2.11-2.4.0.jar,"
           "C:\\cobrixJars\\scodec-core_2.11-1.10.3.jar,"
           "C:\\cobrixJars\\scodec-bits_2.11-1.1.4.jar,"
           "C:\\cobrixJars\\cobol-parser_2.11-2.4.0.jar,"
           "C:\\cobrixJars\\antlr4-runtime-4.7.2.jar").\
    master("local[1]").\
    getOrCreate()

# AWS secret constants
Access_Key = 'ak'
Secret_Key = 'sk'
Session_Key = ''sk'

# Set S3 bucket
sc = SparkContext.getOrCreate()
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('spark.hadoop.fs.s3a.access.key', Access_Key)
hadoopConf.set('spark.hadoop.fs.s3a.secret.key', Secret_Key)
hadoopConf.set('spark.hadoop.fs.s3a.session.token', Session_Key)
hadoopConf.set('spark.hadoop.fs.s3a.endpoint', 's3-us-east-2.amazonaws.com')
hadoopConf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

# Prepare S3 instance
s3 = boto3.resource('s3',
                    aws_access_key_id = Access_Key,
                    aws_secret_access_key = Secret_Key,
                    aws_session_token = Session_Key)

# Object to refer Ebcdic Data
EData = s3.Object('s3 bucket',
                  'key')

# Object to refer Cobol Copybook
CobCpy = s3.Object('s3 bucket',
                   'key')

# Variable file processing
df = spark.read.format('cobol'). \
    option("record_format", "V"). \
    option("rdw_adjustment", 4). \
    options(copybook=CobCpy.get()['Body'].read().decode('utf-8')). \
    load(EData.get()['Body'].read())

# write CSV to S3
df.coalesce(1).\
    write.csv("s3 bucket with file name link complete",mode='overwrite',header=True)








