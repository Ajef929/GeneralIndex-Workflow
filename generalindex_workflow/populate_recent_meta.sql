--queries to populate a table of recent data for easier query processing 
--\x on

drop table if exists meta_clean;

with clean_date as 
(select  dkey,
	title,
	doi,
	author,
	cast(pub_date as varchar) as pub_date
from metadata
--limit 10000
)
--meta_clean as (
select dkey,
	title,
	doi,
	author,
	pub_date,
	CASE
	--when SPLIT_PART(pub_date, '-',1) ~ '^\d{4}$' then CAST(SPLIT_PART(pub_date,'-',1) as int)
	when pub_date ~ '^\d{4}' then CAST(SUBSTRING(pub_date for 4) as int)
	when pub_date ~ '^/s+\d{4}' then CAST(SUBSTRING(TRIM( both from pub_date) for 4) as int)
	--when pub_date = 'N/A' then NULL
	else NULL
	END as year
into TEMP meta_clean
from clean_date;

--create recent metadata table
drop table if exists metadata_recent;
select *
into metadata_recent 
from  meta_clean as mr
where date_part('year',current_date) - mr.year <= 20; --only looking at the past 20 years of dates
