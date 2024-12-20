from .utils import spark, EMBEDDING_MODEL, EMBEDDING_COLUMN, PARSER
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import mlflow.deployments
import pandas as pd
from typing import Iterator, List, Union
import io
from pypdf import PdfReader
import warnings
from llama_index.core import Document
from PIL import Image
import pymupdf
import cv2
import numpy as np
import os
import requests
from bs4 import BeautifulSoup
from delta.tables import DeltaTable

def get_reader(reference: Union[str, bytearray]):
    if isinstance(reference, str):
        pdf = reference
    else:
        pdf = io.BytesIO(reference)
    return PdfReader(pdf)

def read_pdf(reference: Union[str, bytes]):
    try:
        reader = get_reader(reference)
        return [[reader.metadata['/Title'], i, page_content.extract_text()] for i, page_content in enumerate(reader.pages)]
    except Exception as e:
        warnings.warn(f"Exception {e} has been thrown during parsing")
        return None


@F.pandas_udf("array<float>")
def get_embedding(contents: pd.Series) -> pd.Series:
    deploy_client = mlflow.deployments.get_deploy_client("databricks")
    def get_embeddings(batch):
        #Note: this will fail if an exception is thrown during embedding creation (add try/except if needed) 
        response = deploy_client.predict(endpoint=EMBEDDING_MODEL, inputs={"input": batch})
        return [e['embedding'] for e in response.data]

    # Splitting the contents into batches of 150 items each, since the embedding model takes at most 150 inputs per request.
    max_batch_size = 150
    batches = [contents.iloc[i:i + max_batch_size] for i in range(0, len(contents), max_batch_size)]

    # Process each batch and collect the results
    all_embeddings = []
    for batch in batches:
        all_embeddings += get_embeddings(batch.tolist())

    return pd.Series(all_embeddings)

@F.pandas_udf("array<string>")
def split_in_chunks(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
    def extract_and_split_page(txt):
      if txt is None:
        return []
      nodes = PARSER.get_nodes_from_documents([Document(text=txt)])
      return [n.text for n in nodes]
    
    for x in batch_iter:
        yield x.apply(extract_and_split_page)

@F.pandas_udf("array<struct<title:string, page_number:int, text:string>>")
def extract_pages_content(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]: 
    for x in batch_iter:
        yield x.apply(read_pdf)


def preprocess(df: DataFrame, save_table_name: str) -> DataFrame:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {save_table_name} (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY,
            path STRING,
            title STRING,
            url STRING,
            page_number INT,
            content STRING,
            embedding ARRAY<FLOAT>
        ) USING DELTA
        TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)
    
    return (
        df
        .withColumn('url', F.col('path'))
        .withColumn('path', F.regexp_replace(F.col("path"), 'dbfs:', ''))
        .withColumn("extracted_content", F.explode(extract_pages_content("content")))
        .select(
            "path",
            "url",
            "content",
            F.col("extracted_content.title").alias("title"),
            F.col("extracted_content.page_number").alias("page_number"),
            F.col("extracted_content.text").alias("text")
        )
        .withColumn("content", F.explode(split_in_chunks("text")))
        .withColumn("embedding", get_embedding("content"))
        .select('path', 'title', 'url', F.col('page_number').cast('int'), 'content', EMBEDDING_COLUMN)
    )

def render_page_as_image(pdf_path: str, page_number: int, as_opencv: bool = False) -> Union[Image, np.ndarray]:
    doc = pymupdf.open(pdf_path)
    page = doc[page_number]
    pix = page.get_pixmap(dpi=300)
    pil_img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

    if not as_opencv:
        return pil_img
    else:
        return cv2.cvtColor(np.array(pil_img), cv2.COLOR_RGB2BGR)




class QRCodeScraper:
    # def download_image_from_html(self, html_file_content, part_code):
    #     # Read the HTML file
    #     soup = BeautifulSoup(html_file_content, 'html.parser')

    #     # Find the image tag within the class 'image-gallery-image'
    #     image_tag = soup.find('img', class_='image-gallery-image')

    #     # Get the image URL
    #     image_url = image_tag['src']

    #     # Download the image
    #     response = requests.get(image_url)

    #     with open(f"export/images/{part_code}.{image_url.split('.')[-1]}", 'wb') as img_file:
    #         img_file.write(response.content)
    
    def _process_partlist_html(self, url: str, id: int):
        page = requests.get(url)
        
        if page is None or page.content is None:
            return
        
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(page.content, 'html.parser')

        # Find the table with the id 'chakra-table'
        table = soup.find('table', {'class': 'chakra-table'})
        part_code = soup.select('#root > div > div.chakra-stack.css-nigbwa > div.css-1f8o5nu > div > div > div > div > div > div > div.chakra-stack.css-1humjyr > div > div > div.chakra-stack.css-1l5gd6o > div > h2.chakra-heading.css-uaw0gg')[0].text
        part_name = soup.select('#root > div > div.chakra-stack.css-nigbwa > div.css-1f8o5nu > div > div > div > div > div > div > div.chakra-stack.css-1humjyr > div > div > div.chakra-stack.css-1l5gd6o > div > h2.chakra-heading.css-15gvieb > div')[0].text

        # Parse the table using pandas
        df = pd.read_html(str(table))[0]
        df['part_code'] = part_code
        df['part_name'] = part_name
        df['qr_code_url'] = url

        part_lists_df = spark.createDataFrame(df).select('item_number', 'part_number', 'designation', 'part_code', 'part_name', 'qr_code_url')

        # part_lists_df = part_lists_df.withColumnRenamed('Part number', 'part_number')
        # part_lists_df = part_lists_df.withColumnRenamed('Designation', 'designation')
        # part_lists_df = part_lists_df.withColumnRenamed('Item', 'item_number')

        return part_lists_df
    
    def _extract_qr_code_links(self, img: np.ndarray, filename: str = "", write_dir: str = None, border_size: int = 20):
        links = {}

        # Detect QR code
        qcd = cv2.QRCodeDetector()
        is_qr_detected, decoded_info, points, straight_qrcode = qcd.detectAndDecodeMulti(img)

        if points is None:
            return links
        
        for i, p in enumerate(points):
            file = '_'.join([filename, str(i)])
            links[file] = decoded_info[i].strip()

            # Get the coordinates of the bounding rectangle of the QR code
            bl, br, ur, ul = points[0].astype(int)
            y_start = bl[1]
            y_end = ul[1]
            x_start = bl[0]
            x_end = br[0]

            # Crop the image
            crop_img = img[y_start:y_end, x_start:x_end]

            # Resize the image
            resized_img = cv2.resize(crop_img, (224, 224))

            # Convert to grayscale
            gray = cv2.cvtColor(resized_img, cv2.COLOR_BGR2GRAY)
            
            # Add white border
            bordered_image = cv2.copyMakeBorder(
                gray,
                top=border_size,
                bottom=border_size,
                left=border_size,
                right=border_size,
                borderType=cv2.BORDER_CONSTANT,
                value=[255]
            )

            # Save the resized image
            if write_dir is not None:
                os.makedirs(write_dir, exist_ok=True)
                cv2.imwrite(os.path.join(write_dir, file+".png"), bordered_image)

        return links
    
    def scrape(self, pdf_urls_in_dbx_volume: List[str], save_to_table: str = None, **kwargs) -> DataFrame:
        qr_code_urls = []
        for pdf in pdf_urls_in_dbx_volume:
            try:
                nr_pages = pymupdf.open(pdf).page_count
            except:
                print(f"Skipping {pdf} - an error occured while trying to open it")
                
            for page in range(nr_pages):
                print(f"Processing pdf {pdf}, page {page+1}/{nr_pages}")
                
                img = render_page_as_image(pdf, page_number=page, as_opencv=True)
                links = self._extract_qr_code_links(img, filename='_'.join([pdf.split('/')[-1].replace('.pdf', ''), str(page)]), **kwargs)
                qr_code_urls += links

        df = None
        for url in qr_code_urls:
            _df = self._process_partlist_html(url)
            df = _df if df is None else df.union(_df)
            # self.download_image_from_html(webpage_content, part_code)

        if save_to_table is not None:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {save_to_table} (        
                    item_number BIGINT,
                    part_number STRING,
                    designation STRING,
                    part_code STRING,
                    part_name STRING,
                    qr_code_url STRING
                ) USING DELTA
                TBLPROPERTIES (delta.enableChangeDataFeed = true)
            """)
            
            # Save the DataFrame merging it with the existing data
            (
                DeltaTable.forPath(spark, save_to_table)
                .alias('target').merge(
                    df
                    # .select(*cols)
                    .alias('source'), 
                    """
                    source.qr_code_url = target.qr_code_url 
                    AND source.part_code = target.part_code 
                    AND source.item_number == target.item_number
                    """
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            return df






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