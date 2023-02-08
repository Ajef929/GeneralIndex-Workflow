#!/bin/bash
#This workflow achievs the following tasks:
#1) Obtains metadata from GeneralIndex website
#2) Uncompresses the metadata files
#3) Uploads the metadata files to posgresql datatbase
#4) merges the tables in the postgresql database to form one single table

totalStart=`date +%s` #initialise a timer for the whole script

cd ~/generalindex_workflow/generalindex_workflow/ #change working directory

##Downloading metadata from Generalindex
start0=`date %s` 
wget -nc -i metadata_urls.txt -P ~/generalindex_workflow/generalindex_workflow #metadata_urls is a text file that contains the urls to download
end0=`date %s`

##extract each of the metadatafiles if it does not already exist in the directory
#rm doc_meta_dumps/doc_meta_* ##remove existing files 
start01=`date %s`
for file in `ls *.tar.gz`; do tar -n -xzvf $file; done
end01=`date %s`


##create database if not exists. Create schema docs
psql -d postgres -c "CREATE DATABASE generalindex_metadata"
psql -d generalindex_metadata -c "CREATE SCHEMA docs"

#'remove lines that assert file owner of database
start1=`date +%s`
for filename in doc_meta_dumps/doc_meta_*.sql;
do
echo "\nremoving rogers permissions...\n"
perl -0777 -i -pe 's/ALTER TABLE.* OWNER TO roger/ /' $filename
done
end1=`date +%s`
echo Execution time was `expr $end1 - $start1` seconds.

#removing existing tables
echo "\nremoving existing metadata slice tables in database...\n"
start2=`date +%s`
psql -d generalindex_metadata -c "DROP TABLE metadata;"
psql -d generalindex_metadata -f drop_tables_query.sql #drop_tables_query.sql contains a SQL statement that drops all tables like doc_meta_*
end2=`date +%s`
echo execution time was `expr $end2 - $start2` seconds.

##upload files to database
echo "\nuploading files to database...\n"
start3=`date +%s`
for filename in doc_meta_dumps/doc_meta_*.sql;
do
echo importing `expr $filename` into generalindex_metadata
start=`date +%s`
psql -d generalindex_metadata -f $filename
end=`date +%s`
echo Execution time was `expr $end - $start` seconds.
done
end3=`date +%s`

#create a template metadata table
echo "\nCreating the metadata table with the same structure as the first slice...\n"
psql -d generalindex_metadata -c "CREATE TABLE metadata (LIKE docs.doc_meta_0);"

#merging all of the databases into one database
echo "\ncombining metadata tables...\n"
start4=`date +%s`
psql -d generalindex_metadata -c "select CONCAT('docs.',tablename) from pg_tables where 
schemaname='docs'" | sed 1,2d | head -n -2 > hello.out
while read line;
do
echo "\nInserting table into metadata...\n"
psql -d generalindex_metadata -c "insert into metadata (select * from  $line);"
done < hello.out
end4=`date +%s`
echo Execution time was `expr $end4 - $start4` seconds.

##remove hello.out (table names for original slices)
rm hello.out

##create index on metadata table
echo "\nCreating an index for metatable\n"
psql -d generalindex_metadata -c "CREATE INDEX idx_metadata on metadata(dkey);"

#removing existing metadata tables to clean up 
echo "\ncleaning database (removing original metadata tables)...\n"
start5=`date +%s`
psql -d generalindex_metadata -f drop_tables_query.sql
end5=`date +%s`
echo execution time was `expr $end5 - $start5` seconds.

#creating a copy of the metadata table to make it easier to work with that only uses the last 20 years of information
echo "\ncreating a sub-table of clean,recent data...\n"
start6=`date +%s`
psql -d generalindex_metadata -f populate_recent_meta.sql
end6=`date +%s`
echo execution time was `expr $end6 - $start6` seconds.


#total script timer
totalEnd=`date +%s`

echo Execution time for downloading metadata files was `expr $end0 - $start0` seconds.
echo Execution time for extracting metadata files was `expr $end01 - $start01`seconds.
echo Rogers permissions took `expr $end1 - $start1` seconds to execute.
echo Removing exisiting tables execution time was `expr $end2 - $start2` seconds.
echo Uploading slices to database took `expr $end3 - $start3` seconds.
echo Combing slices into one table execution time was `expr $end4 - $start4` seconds.
echo Execution time for removing original metadata tables was `expr $end5 - $start5` seconds.
echo Execution time for creating recent sub-table was `expr $end6 - $start6` seconds.
echo Total execution time for script was `expr $totalEnd - $totalStart` seconds.  



