from utils import spark, EMBEDDING_MODEL, PARSER
import pyspark.sql.functions as F
import mlflow.deployments
import pandas as pd
from typing import Iterator, Union
import io
from pypdf import PdfReader
import warnings
from llama_index.core import Document

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



def preprocess(df):
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
        .select('path', 'title', 'url', F.col('page_number').cast('int'), 'content', 'embedding')
    )