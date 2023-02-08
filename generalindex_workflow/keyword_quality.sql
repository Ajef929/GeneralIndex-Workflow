--the following file contains queries that assess the data quality of the keywords data. The syntax style is suited to Snowflake.

--Missing Data by Column
select avg(case when dkey in ('N/A','','blank','-1',NULL) then 100.0 else 0 end) as pct_missing_dkey,
       avg(case when keywords in ('N/A','','blank','-1',NULL) then 100.0 else 0 end) as pct_missing_keywords,
       avg(case when keywords_lc in ('N/A','','blank','-1',NULL)then 100.0 else 0 end) as pct_missing_keywords_lc,
       avg(case when keyword_tokens < 0 then 100.0 else 0 end) as pct_missing_keyword_tokens,
       avg(case when keyword_score < 0 then 100.0 else 0 end) as pct_missing_keyword_score,
       avg(case when doc_count < 0 is NULL then 100.0  else 0 end) as pct_missing_doc_count,
       avg(case when insert_date is null then 100 else 0 end) as pct_missing_insert_date
from keywords;

--Duplicate Data
select sum(case when n_row = 1 then 0 else 1 end) as duplicateRowCount,
    sum(case when n_row = 1 then 1 else 0 end) as singleRowCount
from  (select dkey,row_number() OVER(PARTITION BY dkey,
    keywords, keywords_lc,keyword_tokens, keyword_score, doc_count,
    insert_date order by dkey) as n_row from keywords) as duplicated;

--Consistency
with totalCount as (select count(dkey) as total from keywords)
SELECT COUNT(dkey) as count_consistent,COUNT(dkey)/(select total from TOTALCOUNT) * 100 as perc_consistent 
FROM keywords WHERE LENGTH(dkey) != 32 --when the dkey fails to be an Md5 Hash of length 32
                    OR keywords != keywords_lc --where the lowercase keywords do notmatch the regular keywords
                    OR NOT is_double(keyword_score); --where the keyword score is not numeric

--uniqueness of rows
with totalCount as (select count(dkey) as total from keywords),
unique_keys as (select distinct dkey from keywords)
select count(dkey) as unique_row_count,count(dkey) / (select total from TOTALCOUNT) * 100 as perc_unique_rows 
from unique_keys;