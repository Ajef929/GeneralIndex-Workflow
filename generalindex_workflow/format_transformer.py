##python file rwrites gzip file into another format
"""
This file rewrites the a .sql.zip file into a csv equivalent

"""
import sys
import subprocess
import pkg_resources

##install missing packages
required = {'dataclasses','pyarrow','pandas'}
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
from dataclasses import dataclass,field
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from more_itertools import chunked
from tqdm import tqdm
from enum import Enum
import re

def write_parquet_table(df,filepath=None):
    """Method writes dataframes in parquet format.

    This method is used to write pandas DataFrame as pyarrow Table in parquet format.
    
    :param dataframe: pd.DataFrame to be written in parquet format.
    :param filepath: target file location for parquet file.
    :param writer: ParquetWriter object to write pyarrow tables in parquet format.
    """
    table = pa.Table.from_pandas(df)
    writer = pq.ParquetWriter(filepath, table.schema,compression='GZIP')
    writer.write_table(table=table)
    writer.close()
    
def write_parquet_batch(df,filepath=None):
    
    batch = pa.RecordBatch.from_pandas(df)
    writer = pq.ParquetWriter(filepath, batch.schema)
    writer.write_batch(batch)
    writer.close()

##an ENUM equivalent for the size of the bytes
@dataclass
class Size(Enum):
    MEB: int = 2**20   # mebibyte

@dataclass
class SQLDumpTransformer:
    file_name:str
    data_start_idx = 0
    chunks_written = 0

    def __post_init__(self):
        self.findDataStart()
        
    def findDataStart(self) -> int:
        '''returns the starting line number for the data in the file'''
        """opens the file in byte stream format"""
        data = open(self.file_name, "rb", buffering=4*Size.MEB.value)
        for idx,line in enumerate(data):
            line_decoded = line.decode()
            if re.match(r'COPY',line_decoded):
                self.data_start_idx = idx

        assert idx > 0,"Copy statement not found in file"


    def getSchema(self) -> dict:
        """defines the schema to write the parquet format into """
        d = {
            "dkey":"text",
            "keywords":"text",
            "keywords_lc":"text",
            "keyword_tokens" :"integer",
            "keyword_score":" numeric",
            "doc_count" :"integer",
            "insert_date": "date"
        }
        return d

    def writeToParquetBuffered(self,chunksize:int  = 240_000_000) -> None:
        """writes a file in chunks to format"""
        byteswritten = 0
        writer = None #initializing parquet writer
        chunk = [] ##initial chunk write
        #read the new encoded file with buffering in bytestream
        infile = open(self.file_name, "rb", buffering=Size.MEB.value)
        new_filename = self.file_name.split(".")[0] + ".parquet.gz"
        #outfile = open(f"{filename_root}_{count}.sql", "wb", buffering=4*Size.MEB.value)
        try:
            #begin writing once the data has begun
            idx = 0
            for line in tqdm(infile):
                idx += 1
                #print(f"data start index {self.data_start_idx}")
                #writing the data to parquet
                ##only write if the data is 'data' and not other
                if idx > self.data_start_idx:
                    #print("starting to write...")

                    #when current chunk exceeded
                    if byteswritten > chunksize:
                        ##converting to parquet format
                        if self.chunks_written > 1:
                            df = pd.DataFrame(data=chunk,columns=colnames)
                            write_parquet_batch(df,new_filename)
                        else:
                            colnames = list(self.getSchema().keys()) #obtain column names from schema
                            df = pd.DataFrame(data=chunk,columns=colnames)
                            write_parquet_table(df,new_filename)
                        byteswritten = 0 ##reset bytes written to start writing the next chunk                  >
                        chunk = [] #reinitialise chunk
                        self.chunks_written += 1
                        print(f"{self.chunks_written} chunks written.")


                    chunk.append(line.decode().split("\t")) ##adding list represention of data chunk
                    byteswritten += len(line)
        finally:
            infile.close()
        assert self.chunks_written > 0, "No chunks written. Please reduce chunk size"

def main():
    ##parsing command line arguments
    args = sys.argv
    zip_filename = args[1] ##extract the first non-script argument from the list
    #zip_filename = 'keywords_sample.sql.zip'
    ft = SQLDumpTransformer(zip_filename)
    ft.writeToParquetBuffered(chunksize=Size.MEB.value * 200)

if __name__ == "__main__":
    main()
