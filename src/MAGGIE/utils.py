import os
from llama_index.core.node_parser import SentenceSplitter, MarkdownElementNodeParser, MarkdownNodeParser
from databricks.feature_engineering.entities.feature_lookup import FeatureLookup
from databricks.feature_engineering import FeatureEngineeringClient, FeatureFunction
from databricks.feature_engineering.entities.feature_serving_endpoint import (
    EndpointCoreConfig,
    ServedEntity
)

from pyspark.sql import SparkSession, DataFrame
from .prompt import *

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
MAX_TOKENS = 1500
TEMPERATURE = 0.01
TOP_K = 3
SIMILARITY_QUERY_TYPE = "ann"

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

QR_CODES_DIR = './qr_codes'
PARTS_LIST_TABLE = 'part_lists'

VECTOR_SEARCH_ENDPOINT_NAME = "rag_endpoint"
VS_PRIMARY_KEY = "id"
EMBEDDING_COLUMN = "embedding"

FEC = FeatureEngineeringClient()

CATALOG_NAME = "test-catalog"
SCHEMA_NAME = "bronze"
TABLE_PATH = "{}.{}.".format(CATALOG_NAME, SCHEMA_NAME) + "{table_name}"

VS_INDEX_FULLNAME = TABLE_PATH.format(table_name="hackathon_pdfs_self_managed_vs_index") # Where we want to store our index
PDFS_TABLE_FULLNAME = TABLE_PATH.format(table_name=CLEAN_PDF_TABLE) # Table containing the PDF's chunks

CHAIN_CONFIG_FILE = "rag_chain_config.yaml"
MODEL_SCRIPT_PATH = os.path.join(os.getcwd(), "chain.py")

MODEL_NAME = "maggie"

EMBEDDING_MODEL = "databricks-gte-large-en"
CHAT_MODEL = "databricks-meta-llama-3-1-70b-instruct"
# CHAT_MODEL = "dbrx_instruct"

RAG_CONFIG = {
    "databricks_resources": {
        "llm_endpoint_name": CHAT_MODEL,
        "vector_search_endpoint_name": VECTOR_SEARCH_ENDPOINT_NAME,
    },
    "input_example": {
        "messages": [
            # {"role": "user", "content": "What is a drum brake?"},
            # {"role": "assistant", "content": "A drum brake is a brake that uses friction caused by a set of shoes or pads that press outward against a rotating bowl-shaped part called a brake drum."},
            {"role": "user", "content": "How do I install an ecometer?"}
        ]
    },
    "output_example": """
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. 
        Ut erat mi, pretium ac aliquam vitae, fermentum at risus. 
        Ut faucibus, lorem sit amet iaculis auctor, odio lacus cursus mauris, ut ornare erat mauris id nibh. 
        Curabitur nulla magna, posuere quis fermentum sit amet, maximus eu mi.
    """,
    "llm_config": {
        "llm_parameters": {"max_tokens": MAX_TOKENS, "temperature": TEMPERATURE},
        "llm_prompt_template": SYSTEM_MESSAGE_TEMPLATE,
        "llm_system_prompt_rewrite": OUTPUT_REWRITE_WITH_CONTEXT_TEMPLATE,
        "tools_prompt_addition": TOOLS_SYSTEM_MESSAGE_ADDITION,
        "llm_prompt_template_variables": extract_vars_from_format_str(SYSTEM_MESSAGE_TEMPLATE),
        "output_rewrite_template": OUTPUT_REWRITE_WITH_HISTORY_TEMPLATE,
        "output_rewrite_template_variables": extract_vars_from_format_str(OUTPUT_REWRITE_WITH_HISTORY_TEMPLATE),
    },
    "retriever_config": {
        "embedding_model": EMBEDDING_MODEL,
        "chunk_template": CHUNK_TEMPLATE,
        "query_rewrite_template": QUERY_REWRITE_TEMPLATE,
        "query_rewrite_template_variables": extract_vars_from_format_str(QUERY_REWRITE_TEMPLATE),
        "data_pipeline_tag": "poc",
        "parameters": {"k": TOP_K, "query_type": SIMILARITY_QUERY_TYPE},
        "schema": {"chunk_text": "content", "document_uri": "url", "primary_key": "id", "page_nr": "page_number"}, # the keys need to match CHUNK_TEMPLATE's variables
        "vector_search_index": VS_INDEX_FULLNAME,
        "uri_prefix": BASE_URL,
    },
}