-- The Snowflake SQL file below calculates some descriptive statistics for the General Index keyword data. 
-- Average YAKE score of keywords in a given document
SELECT dkey,AVG(keyword_score) as avg_keyword_score FROM keywords GROUP BY dkey order by avg_keyword_score desc limit 1000;

-- Number of distinct keywords in a given document
select avg(count) as average number of keywords per document
from 
(SELECT COUNT(DISTINCT keywords) as count FROM keywords GROUP BY dkey);

-- Minimum and Maximium YAKE score of keywords in a given document
SELECT MIN(keyword_score),MAX(keyword_score) FROM keywords;

-- Most common keywords in the dataset
SELECT keywords, COUNT(keywords) FROM keywords GROUP BY keywords ORDER BY COUNT(keywords) DESC limit 10;

-- Distribution of keyword_score across the dataset
SELECT keyword_score, COUNT(keyword_score) FROM keywords GROUP BY keyword_score ORDER BY keyword_score;

-- Distribution of keywords_tokens across the dataset
SELECT keyword_tokens, COUNT(keyword_tokens) FROM keywords GROUP BY keyword_tokens ORDER BY keyword_tokens;
