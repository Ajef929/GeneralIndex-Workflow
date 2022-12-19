##python file rwrites gzip file into another format
"""
This file rewrites the a .sql.zip file into a csv equivalent

"""
import sys
import subprocess
import pkg_resources

##install missing packages
required = {'dataclasses','pyarrow','pandas','more_itertools'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed

if missing:
    for package in list(missing):
        try:
            dist = pkg_resources.get_distribution(package)
            print('{} ({}) is installed'.format(dist.key, dist.version))
        except pkg_resources.DistributionNotFound:
            print('{} is NOT installed'.format(package))
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])

##imports
from zipfile import ZipFile
from dataclasses import dataclass,field
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from more_itertools import chunked

#helper function for code readability
def append_to_parquet_table(dataframe, filepath=None, writer=None):
    """Method writes/append dataframes in parquet format.

    This method is used to write pandas DataFrame as pyarrow Table in parquet format. If the methods is invoked
    with writer, it appends dataframe to the already written pyarrow table.

    :param dataframe: pd.DataFrame to be written in parquet format.
    :param filepath: target file location for parquet file.
    :param writer: ParquetWriter object to write pyarrow tables in parquet format.
    :return: ParquetWriter object. This can be passed in the subsequenct method calls to append DataFrame
        in the pyarrow Table
    """
    table = pa.Table.from_pandas(dataframe)
    if writer is None:
        writer = pq.ParquetWriter(filepath, table.schema,compression='GZIP')
    writer.write_table(table=table)
    return writer



@dataclass
class SQLtoParquetTransformer:
    file_name:str
    ##first creating a zip compressed version of the metadata
    schema:dict = field(default_factory=dict)
    datatype_dict:dict = field(default_factory=dict) ##dictionary to map datatypes from postgres to new format
    colnames:list[str] = field(default_factory=list) ##initialise list of strings 
    file_name_only:str = None##storing the file name root
    data_start_index:int = 0

    def __post_init__(self) -> None:
        #create schmema dictionary
        self.data_struct = self.extract_schema()
        #dictionary to map datatypes
        self.datatype_dict = dict(text='str',varchar='str',integer='int',numeric='float',date='date')
        self.colnames = self.obtain_colnames()
        self.file_name_only = self.file_name.split(".")[0] 
        self.data_start_index = self.findDataStart()
        
    def obtain_colnames(self) -> list[str]:
        return list(self.extract_schema().keys())
       
    def extract_schema(self) -> dict:
        """
        extracting both datatypes and column names
        CREATE TABLE docs.doc_keywords_3 (
        dkey text,
        keywords text,
        keywords_lc text,
        keyword_tokens integer,
        keyword_score numeric,
        doc_count integer,
        insert_date date
    );
        """
        with ZipFile(self.file_name, 'r') as zip_ref:
            # get a list of all files in the zip file
            files = zip_ref.namelist()
            data_dict = {} #initialise data dictionary
            # loop through the files in the zip file
            for file in files:
                # read the file
                with zip_ref.open(file) as f:
                    data = f.readlines()
                    parenth_count = 0
                    i = 0
                    create_flag = False
                    parenth_count = 0

                    while parenth_count < 2:
                        line = data[i]
                        line_decoded = line.decode()
                        if create_flag and parenth_count == 1:
                            if line_decoded.find(")") != -1:
                                parenth_count += 1
                                continue 

                            column_name,column_type = line_decoded.strip().split(",")[0].split(' ')
                            data_dict[column_name] = column_type
                            
                        if line_decoded.find("CREATE TABLE") != -1:
                            create_flag = True
                            ##continue
                            assert line_decoded.find("(") > 0, "opening parethesis in create statement not found"
                            parenth_count += 1
        
                        i+=1

        return data_dict

    def findDataStart(self) -> int:
        """returns the starting line number for the data in the file"""
        with ZipFile(self.file_name, 'r') as zip_ref:
            # get a list of all files in the zip file
            files = zip_ref.namelist()
            # loop through the files in the zip file
            for file in files:
                # read the file
                with zip_ref.open(file) as f:
                    data = f.readlines()
                    for idx,line in enumerate(data):
                        line_decoded = line.decode()
                        if 'COPY' in line_decoded:
                            return idx + 1 ##returns the start position of the data

                    return None ##returns 

    def writeToParquet(self,chunk_size=100000,filepath=None) -> None:
        """takes a .sql.zip postgres dump file and converts it into parquet format.chunk size partitions the dataset"""
        with ZipFile(self.file_name, 'r') as zip_ref:
        # get a list of all files in the zip file
            files = zip_ref.namelist()
            # loop through the files in the zip file
            for file in files:
                # read the file
                with zip_ref.open(file) as f:
                    data = f.readlines()
                    data = data[self.data_start_index:]
                    #reading data a chunk at a time from COPY statement downwards
                    writer = None
                    chunks = chunked(data,chunk_size)
                    for idx,chunk in enumerate(chunks):
                        print(f"transforming chunk{idx} of {len(chunks)}")
                        data_to_write = [line.decode().split("\t") for line in chunk]
                        df = pd.DataFrame(data=data_to_write,columns=self.colnames)
                        ##append each parquet to a new parquet table
                        append_to_parquet_table(df,filepath=filepath,writer=writer)
                    
                    if writer: writer.close()

    def getDataStruct(self):
        return self.data_struct

    def getFormats(self): 
        return self.datatype_dict

    def getColnames(self):
        return self.colnames

                            
def main():
    ##parsing command line arguments
    args =list(sys.argv)
    #print(str(args[1]))
    #input_filename = 'keywords_sample.sql'
    #create_zip(input_filename)
    #zip_filename = 'keywords_sample.sql.zip'
    zip_filename = args[1] ##extract the first non-script argument from the list
    filepath = zip_filename.split(".")[0] + '.parquet.gz'
    #'keywords_sample.parquet.gz'
    #schema_dict = extract_schema(zip_filename) 
    #print(schema_dict)
    print("Setting up...")
    ft = SQLtoParquetTransformer(zip_filename)
    print("Transforming...")    
    ft.writeToParquet(filepath=filepath,chunk_size=100000)
    #print("="*20)
    #print(ft.colnames)
    ##print("="*20)
    #print(ft.data_struct)
    #print("="*20)
    #print(ft.datatype_dict)

if __name__ == "__main__":
    main()

