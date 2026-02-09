create or replace database clone_db;

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
select* from sales_raw_stageing;

CREATE TABLE sales_raw_clone
CLONE sales_raw_stageing;

UPDATE sales_raw_clone
SET product='Regex'
WHERE id = 2;

select*from sales_raw_clone;

select*from sales_raw_stageing;
update sales_raw_stageing set price=10;

select *from sales_raw_stageing at(offset=>-300);

create or replace table sale_clone2_time
clone sales_raw_stageing at(offset =>-300);

select* from sales_clone2_time;



select * from clone_db.sales_raw_staging;
drop table clone_db.sales_raw_staging;

update sales_raw_clone set product='regex' where id in (1,2);
select * from sales_raw_clone; -- change 2 new are insert into new partition



