# Databricks notebook source
####Library import##########
import sys
import traceback
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from subprocess import Popen, PIPE

####### Main Function ######
if __name__ == "__main__":
#######Spark Session ######
    spark=SparkSession\
            .builder\
            .appName("FileIngestion")\
            .enableHiveSupport()\
            .config("hive.exec.dynamic.partition","true")\
            .config("hive.exec.dynamic.partition.mode","nonstrict")\
            .config("spark.sql.caseSensitive","false")\
            .getOrCreate()

#####HDFS Path initialisation######
hdfs_path = '/tmp/'

########Fetching the list of files from hdfs ######
process = Popen(f'hdfs dfs -ls -h {hdfs_path}', shell=True, stdout=PIPE, stderr=PIPE)
std_out, std_err = process.communicate()
list_of_file_names = [fn.split(' ')[-1].split('/')[-1] for fn in std_out.decode().split('\n')[1:]][:-1]
list_of_file_names_with_full_address = [fn.split(' ')[-1] for fn in std_out.decode().split('\n')[1:]][:-1]

########Database Creation #########
spark.sql('create database dbName')

#######Read individual files and load in seperate tables###########
for i in list_of_file_names_with_full_address:
    tableName=i.replace(".","").split("/")[-1]
    readFile=spark.read.format("csv").option("header","true").option("sep",",").load(i)
    readFile.write.format("orc").mode("overwrite").saveAsTable("dbName.tableName")
########Fetching the lsit of tables ########################
tableList=spark.sql('show tables from dbName').select("tableName")
ListtableList=tableList.rdd.flatMap(lambda x: x).collect()
#print(ListtableList)
tableColumn=[]
dataList=[]
columnValues=[]
for i in ListtableList:
    tableColum=spark.read.table("dbName.{}".format(i)).columns
    #print(tableColum)
    dataList.extend(tableColum)
    for j in tableColum:
        columnValues.extend(spark.read.table("dbName.{}".format(i)).select("{}".format(j)).rdd.flatMap(lambda x:x).collect())
print("#What's the average number of fields across all the tables you loaded?#")
print("Question|Answer")
print("1|{}".format(len(dataList)))
dataListClean=[]
str2=[]
#####Getting the list of word and count df occurenrce ######
print("#What is the word count of every value of every table#")
print ("Value | Count")
for values, count  in dict((x,columnValues.count(x)) for x in set(columnValues)).items():
    print('{}|{}'.format(values,count))
########Total number of records loaded in all tables ######
print("###########What's the total number or rows for the all the tables?########")
rowCount=0
for i in ListtableList:
    rowCount+=spark.read.table("dbName.{}".format(i)).count()
print("Question|Answer")
print("3|",rowCount)



