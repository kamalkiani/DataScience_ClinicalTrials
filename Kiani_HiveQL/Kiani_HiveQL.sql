-- Databricks notebook source
-- MAGIC %md
-- MAGIC # BDTT Assignment
-- MAGIC **Kamal Kiani**
-- MAGIC 
-- MAGIC This solution uses **HiveQL** to solve the assignment tasks.
-- MAGIC 
-- MAGIC To run the code for other years as input datasets, simply change the values for `yearOfStudy` and `clinicaltiral_filename` variables.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW yearOfStudy AS
select 2021 as year;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltiral_filename = 'clinicaltrial_2021_csv.gz'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def create_table(file_location , my_table_name, delimiter, file_type) :
-- MAGIC     myDF=spark.read.format(file_type)\
-- MAGIC       .option('inferSchema',True)\
-- MAGIC       .option('header',True)\
-- MAGIC       .option('sep',delimiter)\
-- MAGIC       .load(file_location)
-- MAGIC     if my_table_name not in [table.name for table in spark.catalog.listTables()]:   
-- MAGIC         myDF.write.mode("overwrite").saveAsTable(my_table_name)
-- MAGIC     else:
-- MAGIC         return

-- COMMAND ----------

-- MAGIC %python
-- MAGIC create_table('/FileStore/tables/'+ clinicaltiral_filename ,'clinicaltiral','|','csv')
-- MAGIC create_table('/FileStore/tables/pharma.csv','pharma',',','csv')
-- MAGIC create_table('/FileStore/tables/mesh.csv','mesh',',','csv')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Task 1
-- MAGIC **The number of distinct studies**

-- COMMAND ----------

select count(distinct Id) as Number_of_studies 
from clinicaltiral;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Task 2 
-- MAGIC **Types of studies and their frequencies**

-- COMMAND ----------

select Type, count(*) as frequency 
from clinicaltiral  
group by Type 
order by frequency desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Task 3
-- MAGIC **Top 5 conditions and their frequencies**

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW splited_tbl AS 
  SELECT * , split( Conditions , ',') as splited_conditions  FROM clinicaltiral where Conditions is Not NULL;

CREATE OR REPLACE TEMP VIEW exploded_tbl AS 
  SELECT * , explode( splited_conditions) as exploded_conditions  FROM splited_tbl ;

select exploded_conditions as conditions, count(*) as number_of_conditions
from exploded_tbl 
group by exploded_conditions
order by number_of_conditions desc 
limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Task 4
-- MAGIC **The 5 most frequent roots**

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_mesh AS 
  select * , split(tree , '\\.')[0] as tree_id from mesh ;
  
select tree_id, count(*) as frequency
from exploded_tbl inner join v_mesh on v_mesh.term = exploded_tbl.exploded_conditions
group by tree_id
order by frequency desc
limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Task 5
-- MAGIC **10 most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored**

-- COMMAND ----------

select Sponsor, count(*) as number_of_trials
from clinicaltiral
where Sponsor not in 
  ( select distinct Parent_Company from pharma )
group by Sponsor
order by number_of_trials desc
limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Task 6
-- MAGIC **Number of completed studies each month in a given year**

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW tempView AS 
  select * , split(Completion , ' ')[0] as Month
           , split(Completion , ' ')[1] as Year 
  from clinicaltiral 
  where status ='Completed';

select 
  CASE
    WHEN Month = 'Jan' THEN '01'
    WHEN Month = 'Feb' THEN '02'
    WHEN Month = 'Mar' THEN '03'
    WHEN Month = 'Apr' THEN '04'
    WHEN Month = 'May' THEN '05'
    WHEN Month = 'Jun' THEN '06'
    WHEN Month = 'Jul' THEN '07'
    WHEN Month = 'Aug' THEN '08'
    WHEN Month = 'Sep' THEN '09'
    WHEN Month = 'Oct' THEN '10'
    WHEN Month = 'Nov' THEN '11'
    WHEN Month = 'Dec' THEN '12'
    ELSE '00'
  END AS Month_Order 
  , Month , count(*) as Number_Of_Compeleted
from tempView 
where year = (select year from yearOfStudy)
group by Month
order by Month_Order

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Task 7 - Extra Question
-- MAGIC **The number of different companies which they had penalty, and plotting the trend in a 10 years period from 2011 to 2020**

-- COMMAND ----------

select Penalty_Year , count(distinct Company) as Companies_Had_Penalty
from pharma
where Penalty_Year between 2011 and 2020
group by Penalty_Year
order by Penalty_Year
