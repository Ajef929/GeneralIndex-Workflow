-- This script cleans the metadata into a format that is usable.
-- It contains queries that populate a table of recent data for easier query processing.
-- drop the temporary table if it exists
drop table if exists meta_clean;
-- create a temporary table with distinct data
with clean_date as (
    select distinct dkey,title,doi,author,journal,cast(pub_date as varchar) as pub_date -- removing all of the duplicate rows
    from metadata
)
select dkey,title,doi,author,pub_date,journal,
        -- extract the year from the pub_date field
        CASE
            -- when pub_date starts with 4 digits, extract the year
            when pub_date ~ '^\d{4}' then CAST(SUBSTRING(pub_date for 4) as int)
            -- when pub_date starts with whitespace and 4 digits, extract the year after trimming the whitespace
            when pub_date ~ '^/s+\d{4}' then CAST(SUBSTRING(TRIM( both from pub_date) for 4) as int)
            -- when pub_date is not in the correct format, set year to NULL
            else NULL
        END as year
    into TEMP meta_clean
from clean_date;
-- drop the recent metadata table if it exists
drop table if exists metadata_recent;
-- select data from the temporary table and insert it into the recent metadata table
select * into metadata_recent
from meta_clean
-- only select data from the past 20 years
where date_part('year',current_date) - year between 0 and 20;

