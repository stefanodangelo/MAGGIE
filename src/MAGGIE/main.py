from utils import spark, CATALOG_NAME, SCHEMA_NAME, VOLUME_NAME, PDFS_FOLDER, URLS, RAW_PDF_TABLE, CLEAN_PDF_TABLE
from autoloader import Autoloader

def main():
    catalog = CATALOG_NAME if '-' not in CATALOG_NAME else '`'.join([CATALOG_NAME, ''])
    schema = SCHEMA_NAME if '-' not in SCHEMA_NAME else '`'.join([SCHEMA_NAME, ''])
    volume = VOLUME_NAME if '-' not in VOLUME_NAME else '`'.join([VOLUME_NAME, ''])
    pdfs_folder = PDFS_FOLDER if '-' not in PDFS_FOLDER else '`'.join([PDFS_FOLDER, ''])
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"USE CATALOG {spark}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {spark}.{schema}")
    spark.sql(f"USE SCHEMA {schema}")
    
    loader = Autoloader(catalog, schema, volume, pdfs_folder)
    loader.load_pdfs_to_catalog(URLS, RAW_PDF_TABLE, CLEAN_PDF_TABLE)


if __name__ == "__main__":
    main()
