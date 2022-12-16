#!/bin/bash
#This workflow achievs the following tasks:
#1) Obtains metadata from GeneralIndex website
#2) Uncompresses the metadata files
#3) Uploads the metadata files to posgresql datatbase
#4) merges the tables in the postgresql database to form one single table

totalStart=`date +%s` #initialise a timer for the whole script

cd generalindex #change working directory

##Downloading metadata from Generalindex 
wget -nc -i metadata_urls.txt  #metadata_urls is a text file that contains the urls to download

##extract each of the metadatafiles if it does not already exist in the directory
#rm doc_meta_dumps/doc_meta_* ##remove existing files 
for file in `ls *.tar.gz`; do tar -n -xzvf $file; done

##create database if not exists. Create schema docs
psql -d postgres -c "CREATE DATABASE generalindex_metadata"
psql -d generalindex_metadata -c "CREATE SCHEMA docs"

##change working directory again
cd doc_meta_dumps

#'remove lines that assert file owner of database
for filename in doc_meta_*.sql;
do
echo "removing rogers permissions..."
start=`date +%s`
perl -0777 -i -pe 's/ALTER TABLE.* OWNER TO roger/ /' $filename
end=`date +%s`
echo Execution time was `expr $end - $start` seconds.
done

#removing existing tables
echo "removing existing metadata slice tables in database..."
start=`date +%s`
psql -d generalindex_metadata -c "DROP TABLE metadata;"
psql -d generalindex_metadata -f ~/generalindex/drop_tables_query.sql #drop_tables_query.sql contains a SQL statement that drops all tables like doc_meta_*
end=`date +%s`
echo execution time was `expr $end - $start` seconds.

##upload files to database
echo "uploading files to database..."
for filename in doc_meta_0.sql;
do
echo importing `expr $filename` into generalindex_metadata
start=`date +%s`
psql -d generalindex_metadata -f $filename
end=`date +%s`
echo Execution time was `expr $end - $start` seconds.
done

#create a template metadata table
echo "Creating the metadata table with the same structure as the first slice..."
psql -d generalindex_metadata -c "CREATE TABLE metadata (LIKE docs.doc_meta_0);"

#merging all of the databases into one database
echo "combining metadata tables..."
start=`date +%s`
psql -d generalindex_metadata -c "select CONCAT('docs.',tablename) from pg_tables where 
schemaname='docs'" | sed 1,2d | head -n -2 > hello.out
while read line;
do
echo "Inserting table into metadata..."
psql -d generalindex_metadata -c "insert into metadata (select * from  $line);"
done < hello.out
end=`date +%s`
echo Execution time was `expr $end - $start` seconds.

##create index on metadata table
echo "Creating an index for metatable"
psql -d generalindex_metadata -c "CREATE INDEX idx_metadata on metadata(dkey);"

#removing existing metadata tables to clean up 
echo "cleaning database (removing original metadata tables)..."
start=`date +%s`
psql -d generalindex_metadata -f /home/rccuser/drop_tables_query.sql
end=`date +%s`
echo execution time was `expr $end - $start` seconds.

#total script timer
totalEnd=`date +%s`
echo total execution time for script was `expr $totalEnd - $totalStart` seconds.  



