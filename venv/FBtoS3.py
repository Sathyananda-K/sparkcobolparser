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
Access_Key = 'ASIAYEZDQG37V6CB77EX'
Secret_Key = 'WKdeufPMlv1+vnvU9zHtJozzNZizkF6sy0HhoDi4'
Session_Key = 'IQoJb3JpZ2luX2VjEGgaCXVzLWVhc3QtMSJGMEQCHzqJzLREP+7xHRS/bi3lgUXf7bJupMGN5oZwh6Av0n8CIQDwYE9GWiem+kH6AlE+/aWJH3CpBU5qIGp3ph8TBgN4YyqCAwjR//////////8BEAAaDDU2MDAzMDgyNDE5MSIMpPS62xL56ErqgOtrKtYCjHLF36GuYsVxC69X7tQWWLIGBwjIPEGm68f3OuwtB/rYQ25pvfvY1LJ5xtGGFUVFQOMVK37wQzqutr6A1FdqnZuGJgcQoGejCzufo32XVxVroefdZ6sNZSZI+N6NWinN0vnwb6l1kbhaAtnO+RIVLtRFK1pl3FJsQXeHmwNRzk+KNtBIuC0TnuP7In9dqGtlyZ7M4Z+TlpNjNJGIkNBLOOC26w3cO8bLj0dwhitlUmhUeESaTMlrak2B5wlI/G8JKeJMYsLbWidnn3uRSkB3t5iyCVm0O6IBidEUPQLvItPIJck9WyNjOzdiZjRsOkBjl/C/Mkw/6xk06hIuuD9924tB9B2T305Q5cpJSlIPrJ9qg61JXNreuhO5ioW2MW3Pu+fY5ABYTgM2eIF4bU+3ty/+qsX+CyXUVn5o+oHL1NuSjGmZxoIR4ViL++k6sx7npqr6d/2sMMTX1YoGOqcB4v20D/i4TYl/ftNoNb10C7qv034xHW09fyztkqKGG6oTKMAPNaLtKRbV9S/MDHlEcpD5F1tViyamtEsirR94pmkxFfBRaa+kT6C2/c+K8K0r6G96uCOtK7xYT4FN/Mr9WF82q6jz2Fsugu0QsDsJnS6NlmuXsySBXeIJXyZqse84xyC3i6XPoYAhrMVg5PXz0vpqEQr71Si1XYLsvBlySyfq5w8/g8s='

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
EData = s3.Object('oedsd-acc-s3-dl-ingestion-sftpscore-con-ue1-all',
                  'score/A_T1_RC_ZZ_DLY_CUSTPRF_PR4000CC')

# Object to refer Cobol Copybook
CobCpy = s3.Object('oedsd-dev-s3-dl-infra-appcode-ltd-ue1-all',
                   'appcode/score/CUSTPRF1.cob')

# Fixed file processing
df = spark.read.format('cobol'). \
    option("record_format", "F"). \
    option("record_length", "5500"). \
    options(copybook=CobCpy.get()['Body'].read().decode('utf-8')). \
    load(EData.get()['Body'].read())

# write CSV to S3
df.coalesce(1).\
    write.csv("s3://oedsd-dev-s3-dl-score-raw-con-ue1-all/score/2021/09/30/CUSTPRF",mode='overwrite',header=True)








