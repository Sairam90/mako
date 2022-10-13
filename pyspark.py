
import os
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType,TimestampType,DateType
from pyspark.sql.functions import col, asc,desc
from pyspark.sql.functions import date_trunc
from pyspark.sql import SparkSession
# sc = spark.sparkContext
spark = SparkSession.builder.getOrCreate()
print(spark)
# A JSON dataset is pointed to by path.
# The path can be either a single text file or a directory storing text files
path = "/local-files/"



###########################################################################################################################################################################
###INGEST JSON FILES FROM DATALAKE FOR EX: S3 bucket in ideal scenario -- in this case from location in path variable

tradeschema = StructType([
      StructField("event_timestamp",TimestampType(),True),
      StructField("instrument_id",StringType(),True),
      StructField("tradedate",StringType(),True),

  ])

tradesDF = spark.read.json(os.path.join(path,"trades.json"))
##Round off to event_timestamp to seconds from microseconds
tradesDF=tradesDF.withColumn("event_timestamp",date_trunc('second',"event_timestamp")).orderBy(col("instrument_id").asc(),col("event_timestamp").asc())

tradesDF.printSchema()
tradesDF.show(20,False)
###########################################################################################################################################################################


valueschema = StructType([
      StructField("gamma",StringType(),True),
      StructField("instrument_id",StringType(),True),
      StructField("theta",StringType(),True),
      StructField("tradedate",StringType(),True),
      StructField("vega",StringType(),True),
      StructField("when_timestamp",TimestampType(),True)                               
  ])



valuesDF = spark.read.json(os.path.join(path,"valuedata.json")).orderBy(col("instrument_id").asc(),col("when_timestamp").asc())        

valuesDF.printSchema()
valuesDF.show(20,False)

###########################################################################################################################################################################
###Create temporary views

tradesDF.createOrReplaceTempView("v_trades")
valuesDF.createOrReplaceTempView("v_values")


###########################################################################################################################################################################

##SQL to get future Greeks for various instrument_ids

result  = spark.sql("""

with q0_5s as (
select * 
,(event_timestamp+interval 5 seconds) as t_5s
-- ,(event_timestamp+interval 60 seconds) as t_1m
-- ,(event_timestamp+interval 1800 seconds) as t_30m
-- ,(event_timestamp+interval 3600 seconds) as t_60m
from v_trades )

,

q1_5s as (
select q0_5s.*
,v.when_timestamp
,v.gamma
,v.vega
,v.theta 
from q0_5s
inner join
v_values v
on q0_5s.instrument_id=v.instrument_id and q0_5s.tradedate=v.tradedate
where (
(v.when_timestamp <= q0_5s.t_5s and v.when_timestamp > q0_5s.t_5s - interval 5 seconds)

)
)
,
q2_5s as 
(
select 
q1_5s.*
,(unix_timestamp(q1_5s.t_5s)-unix_timestamp(q1_5s.when_timestamp)) as diff_5s

from q1_5s
)

,
q3_5s as (
select *
,rank() over (partition by instrument_id order by diff_5s,when_timestamp desc) as rank_5s
,row_number() over (partition by instrument_id order by when_timestamp) as rownum
,count(*) over(partition by instrument_id) as count_instruments
from
q2_5s where diff_5s >=0)
,
q_5s as (select 
instrument_id
,when_timestamp
,gamma as gamma_5s
,vega as vega_5s
,theta as theta_5s
from q3_5s where rank_5s =1 and rownum=count_instruments)
,

---------------

q0_1m as (
select * 

 ,(event_timestamp+interval 60 seconds) as t_1m

from v_trades )

,

q1_1m as (
select q0_1m.*
,v.when_timestamp
,v.gamma
,v.vega
,v.theta 
from q0_1m
inner join
v_values v
on q0_1m.instrument_id=v.instrument_id and q0_1m.tradedate=v.tradedate
where (

(v.when_timestamp < q0_1m.t_1m and v.when_timestamp > q0_1m.t_1m - interval 5 seconds)

)
)
,
q2_1m as 
(
select 
q1_1m.*
,(unix_timestamp(q1_1m.t_1m)-unix_timestamp(q1_1m.when_timestamp)) as diff_1m

from q1_1m
)

,
q3_1m as (
select *
,rank() over (partition by instrument_id order by diff_1m,when_timestamp desc) as rank_1m
,row_number() over (partition by instrument_id order by when_timestamp) as rownum
,count(*) over(partition by instrument_id) as count_instruments
from q2_1m where diff_1m >=0)
,
q_1m as (select 
instrument_id
,when_timestamp
,gamma as gamma_1m
,vega as vega_1m
,theta as theta_1m 
from q3_1m 
where rank_1m =1 and rownum=count_instruments)
,
-----------------------

q0_30m as (
select * 
 ,(event_timestamp+interval 1800 seconds) as t_30m
from v_trades )
,

q1_30m as (
select q0_30m.*
,v.when_timestamp
,v.gamma
,v.vega
,v.theta 
from q0_30m
inner join
v_values v
on q0_30m.instrument_id=v.instrument_id and q0_30m.tradedate=v.tradedate
where (
(v.when_timestamp < q0_30m.t_30m and v.when_timestamp > q0_30m.t_30m - interval 5 seconds)

)
)
,
q2_30m as 
(
select 
q1_30m.*
,(unix_timestamp(q1_30m.t_30m)-unix_timestamp(q1_30m.when_timestamp)) as diff_30m

from q1_30m
)
,
q3_30m as (
select *
,rank() over (partition by instrument_id order by diff_30m,when_timestamp desc) as rank_30m
,row_number() over (partition by instrument_id order by when_timestamp) as rownum
,count(*) over(partition by instrument_id) as count_instruments
from q2_30m where diff_30m >=0)
,
q_30m as (select 
instrument_id
,when_timestamp
,gamma as gamma_30m
,vega as vega_30m
,theta as theta_30m
from q3_30m where rank_30m =1 and rownum=count_instruments)
,
-------------------------------------------


q0_60m as (
select * 
 ,(event_timestamp+interval 3600 seconds) as t_60m
from v_trades )
,

q1_60m as (
select q0_60m.*
,v.when_timestamp
,v.gamma
,v.vega
,v.theta 
from q0_60m
inner join
v_values v
on q0_60m.instrument_id=v.instrument_id and q0_60m.tradedate=v.tradedate
where (
(v.when_timestamp < q0_60m.t_60m and v.when_timestamp > q0_60m.t_60m - interval 5 seconds)

)
)
,
q2_60m as 
(
select 
q1_60m.*
,(unix_timestamp(q1_60m.t_60m)-unix_timestamp(q1_60m.when_timestamp)) as diff_60m

from q1_60m
)

,
q3_60m as (
select *
,rank() over (partition by instrument_id order by diff_60m,when_timestamp desc) as rank_60m
,row_number() over (partition by instrument_id order by when_timestamp) as rownum
,count(*) over(partition by instrument_id) as count_instruments
from q2_60m where diff_60m >=0)
,
q_60m as (select 
instrument_id
,when_timestamp
,gamma as gamma_60m
,vega as vega_60m
,theta as theta_60m
from q3_60m where rank_60m =1 and rownum=count_instruments)

select q_5s.instrument_id
,q_5s.when_timestamp 

,q_5s.gamma_5s
,q_1m.gamma_1m
,q_30m.gamma_30m
,q_60m.gamma_60m

,q_5s.vega_5s
,q_1m.vega_1m
,q_30m.vega_30m
,q_60m.vega_60m

,q_5s.theta_5s
,q_1m.theta_1m
,q_30m.theta_30m
,q_60m.theta_60m


from q_5s 
left join q_1m
on q_5s.instrument_id = q_1m.instrument_id 
--and q_5s.tradedate = q_1m.tradedate

left join q_30m
on q_5s.instrument_id = q_30m.instrument_id 
--and q_5s.tradedate = q_30m.tradedate

left join q_60m
on q_5s.instrument_id = q_60m.instrument_id

--and q_5s.tradedate = q_60m.tradedate
""")



result.write.csv(path)

result.show(29,False)

