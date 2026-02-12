

use divyansh_db;

create or replace schema sample_external_schema;


CREATE or replace STAGE s3_sample_stage
  URL = 's3://vasu-bucket-regex'
  CREDENTIALS = (
    AWS_KEY_ID = 'Aceess key'
    AWS_SECRET_KEY = 'secret key'
  );
  
list @s3_sample_stage;


create or replace table product(ProductID string(50),ProductName varchar(50),Category string(50),SubCategory string
);
select * from product;

-- copy command => load data from the external location to snowflake table
copy into prodcut
from @tushar_bucket_stage
files=('samplefile - Sheet1.csv')
file_format=( field_delimiter='-', skip_header=1);


// Define pipe
CREATE OR REPLACE pipe product_pipe
auto_ingest = TRUE
AS
copy into product
from @s3_sample_stage
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

drop pipe employee_pipe;
desc pipe product_pipe;
show pipes;


select*from product;

