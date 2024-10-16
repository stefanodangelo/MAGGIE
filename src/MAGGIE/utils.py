import zipfile
import logging
import os
import io
import pandas as pd
import json
import warnings
import re
import time

from datetime import datetime

from pypdf import PdfReader

# from adobe.pdfservices.operation.auth.service_principal_credentials import ServicePrincipalCredentials
# from adobe.pdfservices.operation.exception.exceptions import ServiceApiException, ServiceUsageException, SdkException
# from adobe.pdfservices.operation.io.cloud_asset import CloudAsset
# from adobe.pdfservices.operation.io.stream_asset import StreamAsset
# from adobe.pdfservices.operation.pdf_services import PDFServices
# from adobe.pdfservices.operation.pdf_services_media_type import PDFServicesMediaType
# from adobe.pdfservices.operation.pdfjobs.jobs.extract_pdf_job import ExtractPDFJob
# from adobe.pdfservices.operation.pdfjobs.params.extract_pdf.extract_element_type import ExtractElementType
# from adobe.pdfservices.operation.pdfjobs.params.extract_pdf.extract_pdf_params import ExtractPDFParams
# from adobe.pdfservices.operation.pdfjobs.params.extract_pdf.extract_renditions_element_type import ExtractRenditionsElementType
# from adobe.pdfservices.operation.pdfjobs.result.extract_pdf_result import ExtractPDFResult

from typing import Union

# import pymupdf
# import pymupdf4llm
from PIL import Image

from llama_index.core.node_parser import SentenceSplitter, MarkdownElementNodeParser, MarkdownNodeParser
from databricks.vector_search.client import VectorSearchClient
from databricks.feature_engineering.entities.feature_lookup import FeatureLookup
from databricks.feature_engineering import FeatureEngineeringClient, FeatureFunction
from databricks.feature_engineering.entities.feature_serving_endpoint import (
    EndpointCoreConfig,
    ServedEntity
)

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame

def get_spark() -> SparkSession:
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()
    
spark = get_spark()


# Hyperparameters
CHUNK_SIZE = 500
CHUNK_OVERLAP = 10
SPLIT_TYPE = 'markdown'

PARSERS = {
    'sentence': SentenceSplitter(chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP),
    'markdown': MarkdownNodeParser()
}
PARSER = PARSERS[SPLIT_TYPE]

# Variables
BASE_URL = 'https://www.bpw.de/fileadmin/user_upload/Service/Downloads/'
URLS = [
    BASE_URL+'BPW-Aftermarketnews_75022201e_AM04_Conversion_of_brake_shoes_for_10-12_t_low_loader_trailer_axles.pdf',
    BASE_URL+'Code_number_designations_BRO_BPW_en_2020_39342001.pdf',
    BASE_URL+'Anhaengerachsen-5_5-t-Trailer-axles-5.5-tonnes-Essieux-jus-5_5-35031401def.pdf',
    BASE_URL+'BPW-Reference_times_for_warranty_work_2022-en-_39052201.pdf',
    BASE_URL+'Maintenance_instructions_BPW_Trailer_Axles_and_Suspensions_2024__33112401e.pdf',
    BASE_URL+'Brake_Drum-HKN-Workshop_manual-BPW-2024-_35192401e.pdf',
    BASE_URL+'HKN_trailer_axles_Spare_parts_list_BPW_31022101e_2021.pdf',
    BASE_URL+'Trailer_Disc_brake_TSB-ECODisc-_Workshop_manual_2023_en-BPW-35292301e.pdf',
    BASE_URL+'BPW-2023-service_manual_WH-TS2_35472302e.pdf',
    BASE_URL+'ECO_Disc_Disc_brake_TS2_TSB_Original_spare_parts_BPW_2024_en__31232401e.pdf'
]

PDF_ROOT_PATH = './pdfs/'
PROCESSED_PDF_ROOT_PATH = './processed-pdfs/'

PDF_PATHS = [os.path.join(PDF_ROOT_PATH, u.split('/')[-1]) for u in URLS]
PDF_NAMES = [path.replace('.pdf', '') for path in PDF_PATHS]

RAW_PDF_TABLE = 'hackathon_pdf_raw'
CLEAN_PDF_TABLE = 'hackathon_pdf_chunks'
VOLUME_NAME = 'volume_hackathon' 
PDFS_FOLDER = 'pdfs'

VECTOR_SEARCH_ENDPOINT_NAME = "rag_endpoint"

VSC = VectorSearchClient()

FEC = FeatureEngineeringClient()

CATALOG_NAME = "test-catalog"
SCHEMA_NAME = "bronze"
TABLE_PATH = "{}.{}.".format(CATALOG_NAME, SCHEMA_NAME) + "{table_name}"

VS_INDEX_FULLNAME = TABLE_PATH.format(table_name="hackathon_pdfs_self_managed_vs_index") # Where we want to store our index
PDFS_TABLE_FULLNAME = TABLE_PATH.format(table_name="hackathon_pdf_chunks") # Table containing the PDF's chunks

CHAIN_CONFIG_FILE = "rag_chain_config.yaml"

MODEL_NAME = "hackathon_rag_model"
MODEL_NAME_FQN = TABLE_PATH.format(table_name=MODEL_NAME)

EMBEDDING_MODEL = "databricks-gte-large-en"


# Helper methods

# def render_page_as_image(pdf_path: str, page_number: int) -> Image:
#     doc = pymupdf.open(pdf_path)
#     page = doc[page_number]
#     pix = page.get_pixmap(dpi=150)
#     return Image.frombytes("RGB", [pix.width, pix.height], pix.samples)


# def extract_images(pdf_path: str):
#     import io

#     pil_imgs = []
#     doc = pymupdf.open(pdf_path)
#     for i in range(1, doc.xref_length()):
#         try:
#             img = doc.extract_image(i)
#             pil_imgs.append(Image.open(io.BytesIO(img['image'])))
#         except:
#             pass
#     return pil_imgs

# def extract_tables(text: str):
#     parser = MarkdownElementNodeParser()
#     elems = parser.extract_elements(text)
#     page_text = ''
#     tables = []
#     for elem in elems:
#         if elem.type == 'table':
#             tables.append(elem.element)
#         else:
#             page_text += elem.element
#             ## TODO: elem.table contains a pd.DataFrame version of elem.element when elem.type == 'table'

#     return page_text, tables

# def preprocess_pdf(pdf_path, use_llama=False, isolate_tables=False):
#     # img_root = './images'
#     # img_path = os.path.join(img_root, PDF_PATHS[2].split('/')[-1].replace('.pdf', ''))
#     # os.makedirs(img_path, exist_ok=True)

#     # Process PDF
#     if use_llama:   
#         llama_reader = pymupdf4llm.LlamaMarkdownReader()
#         pages = llama_reader.load_data(pdf_path, margins=0, table_strategy='lines')
#     else:
#         pages = pymupdf4llm.to_markdown(
#             pdf_path, 
#             page_chunks=True, 
#             extract_words=False, 
#             # write_images=True, 
#             # dpi=150, 
#             # image_path=img_path, 
#             # image_format='png', 
#             # image_size_limit=0, 
#             margins=0, 
#             table_strategy='lines'
#         )

#     cols = ['path', 'title', 'page_number', 'text']
#     contents_df = None
#     tables_df = None
#     for page in pages:
#         if use_llama:
#             page = page.dict()
#         text = page['text'].replace('-----', '').strip()
#         text, tables = extract_tables(text) if isolate_tables else (text, None)

#         new_row = spark.createDataFrame([[page['metadata']['file_path'], page['metadata']['title'], page['metadata']['page'], text]], schema=cols)
#         contents_df = contents_df.union(new_row) if contents_df is not None else new_row

#         if tables is not None:
#             for t in tables:
#                 new_row = spark.createDataFrame([[page['metadata']['file_path'], page['metadata']['title'], page['metadata']['page'], t]], schema=cols)
#                 tables_df = contents_df.union(new_row) if tables_df is not None else new_row

#     # Filter content
#     initial_text = r"(?:.*\n+)?"
#     markdown_titles = r"(#+\s.*\n+)"
#     markdown_content = r"(.*)"
#     pattern = initial_text + markdown_titles + markdown_content

#     # pages_to_remove = []
#     # for idx, row in contents.collect():
#     #     # Use re.DOTALL to make '.' match any character including newlines
#     #     matches = re.findall(pattern, row['content'], re.DOTALL)
#     #     if len(matches) == 0:
#     #         contents = contents.filter(contents['page_number'] != idx + 1)

#     # contents_df = contents_df.withColumn('to_remove', (F.regexp_extract('text', pattern, 0) == '')).filter('to_remove == false').drop('to_remove')

#     return contents_df, tables_df
    

def table_exists(table_name):
    try:
        spark.table(table_name).isEmpty()
    except:
        return False
    return True