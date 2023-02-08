#workflow to import keywords into snowflake.

totalStart=`date +%s` #initialise a timer for the whole script

cd ~/generalindex_workflow/generalindex_workflow/ #change working directory

##Downloading keywords from Generalindex
wget -nc -i keyword_urls.txt -P ~/generalindex_workflow/generalindex_workflow #metadata_urls is a text file that contains the urls to download

##extract each of the keywords files if it does not already exist in the directory
#rm doc_meta_dumps/doc_meta_* ##remove existing files
for file in `ls doc_keywords_*.sql.zip`; 
do unzip $file
; 
done

##converting each keywords file into a .parquet tile
for file in `ls doc_keywords_*.sql` ; 
do 
start = `data +%s`
python3 format_transformer.py $file;
end = `data +%s`
echo file took expr`$end - $start` seconds to convert to parquet format.

done;

#rm doc_keywords 

#importing each of the files into snowflake

