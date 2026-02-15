create database if not exists sales_db;
USE sales_db; -- database
USE ROLE ACCOUNTADMIN; -- masking / polciy accointadmin ke through


-- Prepare table --
create or replace table customers(
  id number,
  full_name varchar,
  email varchar,
  phone varchar,
  spent number,
  create_date DATE DEFAULT CURRENT_DATE);

-- insert values in table --
insert into customers (id, full_name, email,phone,spent)
values
  (1,'abc','asakjsdfjkasf@un.org','583-665-9168',333),
  (2,'Tylor','iuyhshtgall1@mayoclinic.com','412-987-7120',643),
  (3,'Marieee','mspadllkjwelkrjtazzi2@txnews.com','412-946-3659',1356),
  (4,'Neena Sharma','sadfasdf@patch.com','123-853-8192',9795),
  (5,'Odilia','fsadf@globo.com','095-451-8637',2958),
  (6,'LrMeggie123','yhasdf@rediff.com','866-896-6138',800);

  select *from customers;

select current_user();

-- set up roles
CREATE OR REPLACE ROLE ANALYST_MASKED;
CREATE OR REPLACE ROLE ANALYST_FULL;


-- grant select on table to roles
-- grant all PRIVILEGES on db.* to role_name

GRANT SELECT ON TABLE sales_db.PUBLIC.CUSTOMERS TO ROLE ANALYST_MASKED;
GRANT SELECT ON TABLE sales_db.PUBLIC.CUSTOMERS TO ROLE ANALYST_FULL;

show Grants to role analyst_masked;

GRANT USAGE ON SCHEMA sales_db.PUBLIC TO ROLE ANALYST_MASKED;
GRANT USAGE ON SCHEMA sales_db.PUBLIC TO ROLE ANALYST_FULL;

show grants to user vasu;
show grants to role  accountadmin;

-- grant warehouse access to roles
-- GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYST_MASKED;
-- GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYST_FULL;


select current_user();
-- assign roles to a user
GRANT ROLE ANALYST_MASKED TO USER vasu;
GRANT ROLE ANALYST_FULL TO USER vasu;

show grants to user vasu;
select current_role();

-- set up masking policy 
-- val is column of VARCHAR datatype
--- case  => condtion if my role is analyst_full then i will access the orginal column itself
-- otherwise we see the #####

create or replace masking policy phone_p
as (val varchar) returns varchar->
    case
    when current_role() in ('ANALYST_FULL','ACCOUNTADMIN') then val
    else '##-##-##'
    end;

ALter  table IF exists customers modify column phone 
set masking policy phone;

select current_role ();
select *from customers;

use role analyst_masked;
select *from customers;


desc masking policy phone_p;
show masking policies;

select *from table (information_schema.policy_references(Policy_name=>'phone_p'));

alter table customers modify COLUMN PHONE UNSET MASKING POLICY;

DROP MASKING POLICY phone_p;  -- now we can drop masking policy



create or replace masking policy phone 
as (val varchar) returns varchar ->
    case
    when current_role() in ('ANALYST_FULL','ACCOUNTADMIN') then val
    else concat(left(val,2),'*****')
    end;
    
    
select current_role ();
use role ACCOUNTADMIN;




