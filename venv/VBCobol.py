from pyspark.sql import SparkSession

spark = SparkSession.\
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

# Variable file processing
#=========================
df = spark.read.format('cobol'). \
    option("record_format", "V"). \
    option("rdw_adjustment", 4). \
    options(copybook='RTADAIL1.cob'). \
    load('A_T1_DL_ZZ_OF1012CC_CTAMPC_RTADAILY')

df.count
df.show(1)

# df.coalesce(1).write.format("csv").\   #If needed in single file
df.write.csv("RTADAILY",mode='overwrite',header=True)









