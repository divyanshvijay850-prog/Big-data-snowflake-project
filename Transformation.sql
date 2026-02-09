use divyansh_db;
create schema new_schema;

 create or replace table divyansh_db.new_schema.product(product_id int, product_name varchar(50),product_category varchar(52));


describe stage divyansh_db.new_schema.my_s3_stage;

list @divyansh_db.new_schema.my_s3_stage;

COPY INTO divyansh_db.new_schema.product
FROM (select substring($1,2) as product_id,$2 as product_name,$3 as product_category from @divyansh_db.new_schema.my_s3_stage/products.csv)
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

select *from product;

COPY INTO divyansh_db.new_schema.product
FROM @divyansh_db.new_schema.my_s3_stage
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

COPY INTO divyansh_db.new_schema.product
FROM (
    SELECT
        $2 AS customer_id,
        $3 AS first_name,
        $8 AS Email
    FROM @divyansh_db.new_schema.my_s3_stage/customer.csv
)
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

LIST @divyansh_db.new_schema.my_s3_stage;

create or replace table divyansh_db.new_schema.customers(customer_id varchar(60) , first_name varchar(50), email varchar(100));

COPY INTO divyansh_db.new_schema.customers
FROM (select $2 as cutomer_id,$3 as first_name,$8 as email from @divyansh_db.new_schema.my_s3_stage/customers.csv)
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
FORCE = TRUE;

select*from customers;






