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

# Fixed file processing
df = spark.read.format('cobol'). \
    option("record_format", "F"). \
    option("record_length", "5500"). \
    options(copybook='CUSTPRF1.cob'). \
    load('A_T1_RC_ZZ_DLY_CUSTPRF_PR4000CC')

df.count
df.show(1)

# df.coalesce(1).write.format("csv").\   #If needed in single file
df.write.csv("CUSTPRF",mode='overwrite',header=True)









