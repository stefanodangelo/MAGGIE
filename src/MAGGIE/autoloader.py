import os
import requests
import collections
from concurrent.futures import ThreadPoolExecutor
from .utils import spark
from typing import List
import pyspark.sql.functions as F
from .preprocessing import preprocess

class AutoLoader:
    def __init__(self, catalog: str, schema: str, volume: str, pdfs_folder: str) -> None:
        self.volume = f"/Volumes/{catalog}/{schema}/{volume}"
        self.pdfs_path = self.volume + '/' + pdfs_folder.replace('/', '')
        self.checkpoints_path = f'dbfs:{self.volume}/checkpoints'
        self.raw_checkpoints_path = self.checkpoints_path + '/raw_docs'
        self.clean_checkpoints_path = self.checkpoints_path + '/pdf_chunk'
        spark.sql("CREATE VOLUME IF NOT EXISTS {volume}")
        os.makedirs(self.pdfs_path, exist_ok=True)

    def _download_pdfs(self, urls: List[str]) -> None:
        def download_file(url):
            local_filename = url.split('/')[-1]
            destination = self.pdfs_path
            try:
                with requests.get(url, stream=True) as r:
                    r.raise_for_status()
                    print('saving '+destination+'/'+local_filename)
                    with open(destination+'/'+local_filename, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192): 
                            f.write(chunk)
            except Exception as e:
                print(e)

            # return local_filename

        # def download_to_dest(url):
        #     try:
        #         download_file(url)
        #     except Exception as e:
        #         print(e)

        with ThreadPoolExecutor(max_workers=10) as executor:
            collections.deque(executor.map(download_file, urls))

    def _write_raw_pdfs(self, to_table: str) -> None:
        (
            # Read new files in the volume
            spark.readStream
            .format('cloudFiles')
            .option('cloudFiles.format', 'BINARYFILE')
            .option("pathGlobFilter", "*.pdf")
            .load('dbfs:'+self.pdfs_path)
            # Write the data as a Delta table
            .writeStream
            .trigger(availableNow=True)
            .option("checkpointLocation", self.raw_checkpoints_path)
            .table(to_table)
            .awaitTermination()
        )

    def _write_clean_pdfs(self, raw_table_name: str, clean_table_name: str) -> None:
        (
            preprocess(spark.readStream.table(raw_table_name), save_table_name=clean_table_name)            
            .writeStream
            .trigger(availableNow=True)
            .option("checkpointLocation", self.clean_checkpoints_path)
            .table(clean_table_name)
            .awaitTermination()
        )
        
    def load_pdfs_to_catalog(self, urls: List[str], raw_table_name: str, clean_table_name: str) -> None:
        self._download_pdfs(urls)
        self._write_raw_pdfs(raw_table_name)
        self._write_clean_pdfs(raw_table_name, clean_table_name)

        df = spark.sql("SELECT DISTINCT path FROM .clean_table_name}")
        self.pdfs =  [r.path for r in df.collect()] # save all the paths to pdfs