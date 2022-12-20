--queries to output summary information on all of the dates and their quality
--\x on
--\pset format wrapped
--\pset columns 20

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

--checking how many vlaid dates there
select sum(case when year is NULL then 1 else 0 end) as InvalidDates, sum(case when year != -1 then 1 else 0 end) as ValidDates
from meta_clean;

--querying invalid dates
select year, pub_date, count(title) as Frequency 
from meta_clean
where year is NULL
group by year,pub_date
order by Frequency desc;


