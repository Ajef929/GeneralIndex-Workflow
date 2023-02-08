drop table if exists meta_clean_all;
with clean_date as
(select  dkey,
        title,
        doi,
        author,
	journal,
        cast(pub_date as varchar) as pub_date
from metadata
--limit 10000
)
select dkey,
        title,
        doi,
        author,
	journal,
        pub_date,
        CASE
        --when SPLIT_PART(pub_date, '-',1) ~ '^\d{4}$' then CAST(SPLIT_PART(pub_date,'-',1) as int)
        when pub_date ~ '^\d{4}' then CAST(SUBSTRING(pub_date for 4) as int)
        when pub_date ~ '^/s+\d{4}' then CAST(SUBSTRING(TRIM( both from pub_date) for 4) as int)
        --when pub_date = 'N/A' then NULL
        else NULL
        END as year
into meta_clean_all
from clean_date;
