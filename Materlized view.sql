Alter secssion set use_cached_result=false;   --disable 
Alter warehouse compute_wh suspend;
Alter warehouse compute_wh resume;


create or replace TRANSIENT database orders_db;

create or replace schema test;
create or replace table test.orders as
select *from snowflake_sample_data.TPCH_SF100.orders;

create or replace materialized view orders_mv
as
select 
year(O_ORDERDATE) AS YEAR,
MAX(O_COMMENT) AS MAX_COMMENT,
MIN(O_COMMENT) AS MIN_COMMENT,
MAX(O_CLERK) AS MAX_CLERK,
MIN(O_CLERK) AS MIN_CLERK
FROM ORDERS_DB.TEST.ORDERS
GROUP BY YEAR(O_ORDERDATE);

SELECT *FROM ORDERS;
SELECT *FROM ORDERS_MV order by year;

update orders 
set O_clerk='clerk#99990000'
where O_Orderdate='1994-06-13';


