--The following SQL file uploads keyword data to snowflake
--Note that this is inside a snowsql editor. Prior to installation follow the instructions below:
--Downloaded snowsql rpm: 
--wget https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/1.2/linux_x86_64/snowflake-snowsql-1.2.21-1.x86_64.rpm
--Install: 
--rpm -i snowflake-snowsql-1.2.21-1.x86_64.rpm
--Connect:
--snowsql -a <account-name> -u <username>
--E.g. 
--snowsql -a osaarwp-kt92453 -u username

-------------------------------------
create or replace database generalindex;

 use schema generalindex.public;

  create or replace table keywords (
    dkey varchar default null,
    keywords  varchar default null,
    keywords_lc  varchar default null,
    keyword_tokens integer default null,
    keyword_score numeric default null,
    doc_count integer default null,
    insert_date date default null
  );

create or replace warehouse COMPUTE_WH with
  warehouse_size='MEDIUM'
  auto_suspend = 120
  auto_resume = true
  initially_suspended=true;

use warehouse COMPUTE_WH;

--creating a file format
create or replace file format sf_tut_parquet_format
  type = parquet;

--staging
create or replace temporary stage sf_tut_stage
file_format = sf_tut_parquet_format;

--putting from local file system to stage
put file:///home/rccuser/generalindex_workflow/generalindex_workflow/doc_keywords/*.parquet.gz @sf_tut_stage;

--copying to snowflake database
copy into keywords
 from (select $1:dkey::varchar,
              $1:keywords::varchar,
              $1:keywords_lc::varchar,
	   $1:keyword_tokens::integer,
	   $1:keyword_score::numeric,
	   $1:doc_count::integer,
	   $1:doc_insert_date::date
      from @sf_tut_stage);
    
