from utils import (
    spark,
    CATALOG_NAME,
    SCHEMA_NAME,
    VOLUME_NAME,
    PDFS_FOLDER,
    URLS,
    RAW_PDF_TABLE,
    CLEAN_PDF_TABLE,
    PDFS_TABLE_FULLNAME,
    VECTOR_SEARCH_ENDPOINT_NAME,
    VS_INDEX_FULLNAME,
    TABLE_PATH,
    VS_PRIMARY_KEY,
    EMBEDDING_COLUMN,
    CHAIN_CONFIG_FILE,
    RAG_CONFIG,
    MODEL_NAME,
    MODEL_SCRIPT_PATH,
)
from autoloader import AutoLoader
from vector_search import VectorStore
from deployment import DeploymentManager
import yaml


def initialize():
    catalog = CATALOG_NAME if '-' not in CATALOG_NAME else '`'.join([CATALOG_NAME, ''])
    schema = SCHEMA_NAME if '-' not in SCHEMA_NAME else '`'.join([SCHEMA_NAME, ''])
    volume = VOLUME_NAME if '-' not in VOLUME_NAME else '`'.join([VOLUME_NAME, ''])
    pdfs_folder = PDFS_FOLDER if '-' not in PDFS_FOLDER else '`'.join([PDFS_FOLDER, ''])
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"USE CATALOG {spark}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {spark}.{schema}")
    spark.sql(f"USE SCHEMA {schema}")
    
    # Load data into Unity Catalog
    loader = AutoLoader(catalog, schema, volume, pdfs_folder)
    loader.load_pdfs_to_catalog(URLS, RAW_PDF_TABLE, CLEAN_PDF_TABLE)

    # Create Vector Store for user query retrieval
    vector_store = VectorStore(index_table_name=TABLE_PATH.format(table_name=VS_INDEX_FULLNAME), endpoint_name=VECTOR_SEARCH_ENDPOINT_NAME)
    vector_store.create_index(primary_key=VS_PRIMARY_KEY, embedding_vector_column=EMBEDDING_COLUMN, source_table_name=PDFS_TABLE_FULLNAME)

    # Save RAG configuration file for deployment purposes 
    try:
        with open(CHAIN_CONFIG_FILE, 'w') as f:
            yaml.dump(RAG_CONFIG, f)
    except:
        print('pass to work on build job')

    # Log and deploy model
    deployment_manager = DeploymentManager(model_name_full_path = f"{catalog}.{schema}.{MODEL_NAME}")
    deployment_manager.log_model(MODEL_SCRIPT_PATH, CHAIN_CONFIG_FILE, run_name="hackathon_rag")
    deployment_manager.deploy_model()

def main():
    initialize()

if __name__ == "__main__":
    main()
