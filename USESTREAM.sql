create or replace database streams_db;

create or replace table sales_raw_stageing (
id varchar,
product varchar,
price varchar,
amount varchar,
store_id varchar
);

-- INSERT VALUES
INSERT INTO sales_raw_stageing values
(1,'banana',1.99,1,1),
(2,'lemon',0.99,0,1),
(3,'Apple',1.79,1,1),
(4,'orange juice',1.89,1,2),
(5,'cereals',5.98,2,1);

create or replace table store_table (
store_id number,
location varchar,
employees number);

insert into store_table values (1,'chicgo',33);
insert into store_table values (2,'london',12);

create or replace table sales_final_table (
id int,
product varchar,
price varchar,
amount varchar,
store_id int,
location varchar,
employees int);

insert into sales_final_table
select 
SA.id,
SA.product,
SA.price,
SA.amount,
ST.store_id,
ST.location,
ST.employees
from sales_raw_stageing SA
join store_table ST on ST.store_id=SA.store_id;

select *from sales_final_table;

create or replace stream sales_stream on table sales_raw_stageing;

select *from sales_stream;
show streams;

desc stream sales_stream;

 -- inser values to check see the changes
INSERT INTO sales_raw_stageing values
(6,'banana',1.99,2,1),
(7,'mengo',8.99,1,2);
select *from sales_raw_stageing;
select *from sales_stream;

select *from sales_final_table;

insert into sales_final_table
select 
SA.id,
SA.product,
SA.price,
SA.amount,
ST.store_id,
ST.location,
ST.employees
from sales_stream SA
join store_table ST on ST.store_id=SA.store_id;

select* from sales_final_table;

select *from sales_stream;

select *from sales_raw_stageing;

update sales_raw_stageing
set product='potato' where id=2;

select*from sales_raw_stageing;

select *from sales_final_table;

select*from sales_stream;

merge into sales_final_table  f
using sales_stream s
on f.id=s.id
when matched 
and s.METADATA$ACTION='INSERT'
and s.METADATA$ISUPDATE='TRUE'
then update
set f.product=s.product,
f.price=s.price,
f.amount=s.amount,
f.store_id=s.store_id;

select *from sales_final_table










