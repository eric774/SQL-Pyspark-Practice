-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## First

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SQL 1
-- MAGIC

-- COMMAND ----------


create table icc_world_cup
(
Team_1 Varchar(20),
Team_2 Varchar(20),
Winner Varchar(20)
);
INSERT INTO icc_world_cup values('India','SL','India');
INSERT INTO icc_world_cup values('SL','Aus','Aus');
INSERT INTO icc_world_cup values('SA','Eng','Eng');
INSERT INTO icc_world_cup values('Eng','NZ','NZ');
INSERT INTO icc_world_cup values('Aus','India','India');

select * from icc_world_cup;

-- COMMAND ----------

with cte as (
select team_1, winner from icc_world_cup
union all
select team_2 as team_1, winner from icc_world_cup
)


select team_name, matches_played, no_of_wins,
a.matches_played - a.no_of_wins as no_of_losses
from
(select team_1 as team_name, count(team_1) as matches_played, 
sum(case when winner = team_1 then 1 else 0 end) as no_of_wins
from cte
group by team_1)a

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Pyspark 1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql('''select * from icc_world_cup''')
-- MAGIC df.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC
-- MAGIC df1 = df.select("team_1", "winner").union(df.select("team_2", "winner"))
-- MAGIC df1 = df1.withColumn("winner", when(col("team_1")==col("winner"), 1).otherwise(0))
-- MAGIC df2 = df1.groupBy("team_1").agg(count("team_1").alias("no_of_matches"), sum("winner").alias("no_of_wins"))\
-- MAGIC     .select(col("team_1").alias("team_name"), "no_of_matches", "no_of_wins", (col("no_of_matches") - col("no_of_wins")).alias("No_of_losses"))
-- MAGIC df2.display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Second

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SQL 2

-- COMMAND ----------

create or replace table customer_orders (
order_id integer,
customer_id integer,
order_date date,
order_amount integer
);
select * from customer_orders;
insert into customer_orders values(1,100,cast('2022-01-01' as date),2000),(2,200,cast('2022-01-01' as date),2500),(3,300,cast('2022-01-01' as date),2100)
,(4,100,cast('2022-01-02' as date),2000),(5,400,cast('2022-01-02' as date),2200),(6,500,cast('2022-01-02' as date),2700)
,(7,100,cast('2022-01-03' as date),3000),(8,400,cast('2022-01-03' as date),1000),(9,600,cast('2022-01-03' as date),3000)
;

-- COMMAND ----------

select * from customer_orders;
-- order_date, no_of_new_customers, no_of_repeat_customers

-- COMMAND ----------

with cte as (select *, row_number() over(partition by customer_id order by order_date) as rn from customer_orders)
select order_date, count(case when rn = 1 then 1 else null end) as no_of_new_customers,
count(case when rn > 1 then 1 else null end) as no_of_repeat_customers
from cte
group by order_date
order by order_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Pyspark 2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql('''select * from customer_orders''')
-- MAGIC df.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.window import Window
-- MAGIC
-- MAGIC df = spark.sql('''select * from customer_orders''')
-- MAGIC df = df.withColumn("if_first", row_number().over(Window.partitionBy('customer_id').orderBy('order_date')))
-- MAGIC # df.orderBy('order_id').display()
-- MAGIC df1 = df.groupBy('order_date').agg(count(when(col('if_first') == 1, 1).otherwise(None)).alias('no_of_first_visitors'),
-- MAGIC                                     count(when(col('if_first') != 1, 1).otherwise(None)).alias('no_of_repeat_visitors')
-- MAGIC                                    ).orderBy('order_date')
-- MAGIC df1.display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Third

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SQL 3

-- COMMAND ----------

create table entries ( 
name varchar(20),
address varchar(20),
email varchar(20),
floor int,
resources varchar(10));

insert into entries 
values ('A','Bangalore','A@gmail.com',1,'CPU'),('A','Bangalore','A1@gmail.com',1,'CPU'),('A','Bangalore','A2@gmail.com',2,'DESKTOP')
,('B','Bangalore','B@gmail.com',2,'DESKTOP'),('B','Bangalore','B1@gmail.com',2,'DESKTOP'),('B','Bangalore','B2@gmail.com',1,'MONITOR')

-- COMMAND ----------

select * from entries;
-- name, total_visits, most_visited_floors, resources_used

-- COMMAND ----------

select name, count(name) as total_visits, mode(floor) as most_visited_floor, array_join(collect_set(resources), ',') as resources_used 
from entries group by name
order by name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Pyspark 3

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql('''select * from entries''')
-- MAGIC df.display()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from pyspark.sql.functions import *
-- MAGIC
-- MAGIC df1 = df.groupBy('name').agg(count('name').alias('total_visits'), mode(col('floor')).alias('most_visited_floor'), array_join(collect_set('resources'), ',').alias('resources_used')).orderBy('name')
-- MAGIC df1.display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Fourth

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SQL 4
-- MAGIC
-- MAGIC Write a query to provide date for the nth occurence of sunday in future from given date

-- COMMAND ----------

declare today_date date;
set variable today_date = '2022-01-01';
declare n int;
set variable n = 3;

-- COMMAND ----------

select date_add(date_add(today_date, 8-dayofweek(today_date)), (n-1)*7) as nth_sunday

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Fifth

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SQL 5
-- MAGIC how we can find out top 20 % products which gives 80% of the sales. This is also known as pareto principle.

-- COMMAND ----------

select * from superstore_orders_csv 
order by sales desc
limit 20

-- COMMAND ----------

select round(sum(sales)*0.8, 2)  as 80_percent_sale from superstore_orders_csv
-- 1837760.6882399642

-- COMMAND ----------

with cte as (
select product_id, sales_sum, sum(sales_sum) over (order by a.sales_sum desc) as percentile_sales
-- ,round(0.8* sum(a.sales_sum) over(), 2) as 80_percent_sale
from
(select product_id, sum(sales) as sales_sum from superstore_orders_csv 
group by Product_ID
order by sum(sales) desc)a
-- qualify sum(sales_sum) over (order by a.sales_sum desc) <= 1837760.69
)

select 
round((select count(product_id) from cte
where percentile_sales <= (select 0.8* sum(sales_sum) from cte))/(select count(product_id) from cte)*100, 2) 
as top_sales_product

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Pyspark 5

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.csv('/FileStore/tables/Superstore_orders.csv', header = True)
-- MAGIC df.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.window import *
-- MAGIC
-- MAGIC df1 = df.groupBy('Product_ID').agg(sum('sales').alias('sales_sum'))\
-- MAGIC .orderBy('sales_sum', ascending = False)
-- MAGIC # df1.display()
-- MAGIC eighty_percent = df1.select(sum('sales_sum')).collect()[0][0]*(0.8)
-- MAGIC # print(eighty_percent)
-- MAGIC df1 = df1.withColumn("percentile_sales", sum('sales_sum').over(Window.orderBy(col('sales_sum').desc())))
-- MAGIC df_percentile_count = df1.where(col("percentile_sales") <= eighty_percent).count()
-- MAGIC df_total_count = df1.count()
-- MAGIC percentage = 100*df_percentile_count/df_total_count
-- MAGIC percentage
-- MAGIC
-- MAGIC # not sure why but eighty_percent getting different value from SQL above

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Sixth

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SQL 6

-- COMMAND ----------


