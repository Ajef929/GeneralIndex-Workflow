--queries to output summary information on all of the dates and their quality
--\x on
--\pset format wrapped
--\pset columns 20

drop table meta_clean if exists;

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


--now producing summary queries 
--just extracting the year from the dates
--Aggregating year data
select Year,count(title) as DocumentCounts 
from meta_clean
where year is not NULL
group by Year
order by Year asc;
--creating a date table for ease of use for other summary queries
/*
--creating a new table of only recent data
drop table metadata_recent;
select *
into metadata_recent 
from  meta_clean as mr
where date_part('year',current_date) - mr.year <= 50; --only looking at the past 50 years of dates

--summarising for a given author 

select author,Year,count(title) as DocumentCounts
from metadata_recent
where author like ANY(ARRAY['%Pedro Ros Petrovick%']) -- like ALL for exlusive for multiple authors
AND Year is not NULL
group by author,Year
order by author, Year asc;

--summarising for a particular title
select title,Year,count(title) as DocumentCounts
from metadata_recent
where title like ANY(ARRAY['%rotary micromachines%'])
and Year is not NULL
group by title,Year
order by title, Year asc;

--counting the number of authors per document
select dkey,
	SUBSTRING(author,0,10) as author_part,
	(select count(*) from regexp_matches(author,';','g')) + 1 as AuthorCount
from metadata_recent
order by AuthorCount desc;


--remove meta_clean table
drop table meta_clean;
*/
/*
--publications per author

select author,count(title) as Frequency
from
clean_meta
where Year is not NULL
group by author
order by Frequency desc;
*/

