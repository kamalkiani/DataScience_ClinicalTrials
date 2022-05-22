# Databricks notebook source
# MAGIC %md
# MAGIC # BDTT Assignment
# MAGIC **Kamal Kiani**
# MAGIC 
# MAGIC This solution uses **Dataframe** in spark to solve the assignment tasks. 
# MAGIC 
# MAGIC To run the code for other years as input datasets, simply change the values for `yearOfStudy` and `clinicaltiral_filename` variables.

# COMMAND ----------

yearOfStudy = '2021'
clinicaltiral_filename = 'clinicaltrial_2021_csv.gz'

# COMMAND ----------

# importing useful packages :
from pyspark.sql.functions import desc
from pyspark.sql.functions import split
from pyspark.sql.functions import explode
from pyspark.sql.functions import lit
from pyspark.sql.functions import when

# static values definition :
bold = '\033[1m'
bold_end = '\033[0m'

# COMMAND ----------

# reading the data into dataframes :

clinicaltiral = spark.read.option("delimiter", '|').csv('dbfs:/FileStore/tables/' + clinicaltiral_filename , header=True)
mesh    = spark.read.option("delimiter", ',').csv('dbfs:/FileStore/tables/mesh.csv', header=True)
pharma  = spark.read.option("delimiter", ',').csv('dbfs:/FileStore/tables/pharma.csv', header=True)

# COMMAND ----------

# exploring the data :
clinicaltiral.limit(3).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 1 
# MAGIC **The number of distinct studies**

# COMMAND ----------

def numberOfStudies(myDF) : 
    return myDF.distinct().count();

# COMMAND ----------

print('Number of studies in ' + bold + yearOfStudy + bold_end + ' is: ' + bold + str(numberOfStudies(clinicaltiral)) + bold_end )

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 2 
# MAGIC **Types of studies and their frequencies**

# COMMAND ----------

def typesOfStudies(myDF) :
    temp = myDF.groupBy('Type').count().orderBy(desc('count'))
    return temp.withColumnRenamed('count','count ' + yearOfStudy)

# COMMAND ----------

print(bold + '<< Types of studies and their frequencies in ' + yearOfStudy + ' >>' + bold_end)
typesOfStudies(clinicaltiral).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 3 
# MAGIC **Top 5 conditions and their frequencies**

# COMMAND ----------

def topConditions(myDF):
    splitDf = myDF.filter("Conditions is Not NULL").withColumn('spilted_conditions' , split(myDF['Conditions'],','))
    explodedDF = splitDf.withColumn('exploded_conditions', explode( splitDf['spilted_conditions'] ))
    group_condition = explodedDF.groupBy('exploded_conditions').count().orderBy(desc('count')).limit(5)
    return group_condition.withColumnRenamed('count','count ' + yearOfStudy)

# COMMAND ----------

print(bold + '<< Top 5 conditions and their frequencies in ' + yearOfStudy + ' >>' + bold_end)
topConditions(clinicaltiral).show(truncate=False)    

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 4 
# MAGIC **The 5 most frequent roots**

# COMMAND ----------

def frequentRoots(myDF):    
    mesh_code = mesh.withColumn('tree_id',split(mesh['tree'],'\.')[0])
    splitDf = myDF.filter("Conditions is Not NULL").withColumn('spilted_conditions' , split(myDF['Conditions'],','))
    explodedDF = splitDf.withColumn('exploded_conditions', explode( splitDf['spilted_conditions'] ))   
    temp = explodedDF.join(mesh_code , mesh_code.term ==  explodedDF.exploded_conditions,'inner')
    return temp.groupBy('tree_id').count().orderBy(desc('count')).limit(5).withColumnRenamed('count','count ' + yearOfStudy)    

# COMMAND ----------

print(bold + '<< The 5 most frequent roots in ' + yearOfStudy + ' >>' + bold_end)
frequentRoots(clinicaltiral).show(truncate=False)  

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 5 
# MAGIC **10 most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored**

# COMMAND ----------

def getSponsors(myDF):  
    pharmaceutical_companies = pharma.distinct().rdd.map(lambda item: item.Parent_Company).collect()
    non_pharmaceutical_companies = myDF.filter(myDF.Sponsor.isin(pharmaceutical_companies) == False)
    temp = non_pharmaceutical_companies.groupBy('Sponsor').count().orderBy(desc('count')).limit(10)
    return temp.withColumnRenamed('count','count ' + yearOfStudy)

# COMMAND ----------

print(bold + '<< non pharmaceutical sponsors, and sponsored clinical trials in ' + yearOfStudy + ' >>' + bold_end)
getSponsors(clinicaltiral).show(truncate=False) 

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 6 
# MAGIC **Number of completed studies each month in a given year**

# COMMAND ----------

def compeletedStudies(myDF):
    temp = myDF.where("status ='Completed'") \
        .withColumn('month', split(myDF['Completion'],' ')[0])\
        .withColumn('year', split(myDF['Completion'],' ')[1])
    t2 = temp.where('year= ' + yearOfStudy).groupBy('month' ).count() 
    result = t2.withColumn('month_order', \
      when((t2.month == 'Jan'), lit(1)) \
     .when((t2.month == 'Feb'), lit(2)) \
     .when((t2.month == 'Mar'), lit(3)) \
     .when((t2.month == 'Apr'), lit(4)) \
     .when((t2.month == 'May'), lit(5)) \
     .when((t2.month == 'Jun'), lit(6)) \
     .when((t2.month == 'Jul'), lit(7)) \
     .when((t2.month == 'Aug'), lit(8)) \
     .when((t2.month == 'Sep'), lit(9)) \
     .when((t2.month == 'Oct'), lit(10)) \
     .when((t2.month == 'Nov'), lit(11)) \
     .when((t2.month == 'Dec'), lit(12)) \
     .otherwise(lit(0)) ) 
    return result.orderBy('month_order')

# COMMAND ----------

print(bold + '<< Number of completed studies per each month in ' + yearOfStudy + ' >>' + bold_end)
return_df = compeletedStudies(clinicaltiral)
return_df.show(truncate=False) 
myDG = return_df.toPandas()
myDG.plot.bar(x='month', y='count' , title='completed studies in ' + yearOfStudy , rot=0)

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 7 - Extra Question
# MAGIC **The number of different companies which they had penalty, and plotting the trend in a 10 years period from 2011 to 2020**

# COMMAND ----------

import pyspark.sql.functions as func
temp = pharma.filter(pharma.Penalty_Year>='2011').filter(pharma.Penalty_Year<='2020')
result = temp.groupBy('Penalty_Year').agg(func.countDistinct('Company')).orderBy('Penalty_Year') \
         .withColumnRenamed('count(''Company'')','Companies_Had_Penalty')
result.display()
