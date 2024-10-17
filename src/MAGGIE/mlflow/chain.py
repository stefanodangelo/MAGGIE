from langchain_community.embeddings import DatabricksEmbeddings
from operator import itemgetter
import mlflow
import os
import json

from databricks.vector_search.client import VectorSearchClient

from langchain_community.chat_models import ChatDatabricks
from langchain_community.vectorstores import DatabricksVectorSearch

from langchain_core.runnables import RunnableLambda, RunnableParallel, RunnablePassthrough, RunnableBranch
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import (
    PromptTemplate,
    ChatPromptTemplate,
    MessagesPlaceholder,
)
from langchain_core.messages import HumanMessage, AIMessage
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain.agents.react.agent import create_react_agent
from langchain_community.tools.databricks import UCFunctionToolkit


## Enable MLflow Tracing
mlflow.langchain.autolog()



#### VARIABLES

# Load the chain's configuration
model_config = mlflow.models.ModelConfig(development_config="rag_chain_config.yaml")

databricks_resources = model_config.get("databricks_resources")
retriever_config = model_config.get("retriever_config")
llm_config = model_config.get("llm_config")

# Connect to the Vector Search Index
vs_client = VectorSearchClient(disable_notice=True)
vs_index = vs_client.get_index(
    endpoint_name=databricks_resources.get("vector_search_endpoint_name"),
    index_name=retriever_config.get("vector_search_index"),
)
vector_search_schema = retriever_config.get("schema")

embedding_model = DatabricksEmbeddings(endpoint=retriever_config.get("embedding_model"))

# Turn the Vector Search index into a LangChain retriever
vector_search_as_retriever = DatabricksVectorSearch(
    vs_index,
    text_column=vector_search_schema.get("chunk_text"),
    embedding=embedding_model, 
    columns=[
        vector_search_schema.get("primary_key"),
        vector_search_schema.get("chunk_text"),
        vector_search_schema.get("document_uri"),
        vector_search_schema.get("page_nr")
    ],
).as_retriever(search_kwargs=retriever_config.get("parameters"))

CATALOG_NAME = "`test-catalog`"
SCHEMA_NAME = "bronze"
TABLE_PATH = "{}.{}.".format(CATALOG_NAME, SCHEMA_NAME) + "{table_name}"



#### METHODS

# Return the string contents of the most recent message from the user
def extract_question(chat_messages_array):
    return chat_messages_array[-1]["content"]

# Return the chat history, which is is everything before the last question
def extract_chat_history(chat_messages_array):
    last_n = 5 # always use an even number to capture both questions and answers in the chat history
    return chat_messages_array[-last_n:-1] if chat_messages_array[-1]["role"] == "user" else chat_messages_array[-last_n:]

# Format the conversation history to fit into the prompt template below.
def format_chat_history_for_prompt(chat_messages_array):
    ## TODO: summarize older messages and append the last pair of user-assistant messages
    history = extract_chat_history(chat_messages_array)
    formatted_chat_history = []
    if len(history) > 0:
        for chat_message in history:
            if chat_message["role"] == "user":
                formatted_chat_history.append(HumanMessage(content=chat_message["content"]))
            elif chat_message["role"] == "assistant":
                formatted_chat_history.append(AIMessage(content=chat_message["content"]))
    return formatted_chat_history

# Method to update the chat history with new messages
def update_chat_history(inputs):
    chat_messages_array = inputs['chat_history']
    chat_messages_array.append({"role": "user", "content": inputs['question']})
    chat_messages_array.append({"role": "assistant", "content": inputs['answer']})
    return chat_messages_array

# Method to format the docs returned by the retriever into the prompt
# This puts together all the answers retrieved from the vector index into a single string
def format_context(docs):
    chunk_template = retriever_config.get("chunk_template")
    chunk_contents = [
        chunk_template.format(
            chunk_text=d.page_content,
            document_uri=d.metadata[vector_search_schema.get("document_uri")],
            page_nr=d.metadata[vector_search_schema.get("page_nr")],
        )
        for d in docs
    ]
    return "".join(chunk_contents)

def combine_references(outputs):
    refs = set()
    for var, output in outputs.items():
        if 'references' in var:
            for ref in output:
                refs.add((ref.metadata['id'], ref.page_content, ref.metadata['url'], ref.metadata['page_number']))

    references = dict()
    for id, content, url, page_nr in refs:
        references[int(id)] = dict(zip(["passage", "url", "page_number"], [content, url, int(page_nr)]))

    return references

def get_tools(wh_id="36943660786efec7", catalog='`dev-gold`', schema="maggie_hackathon"):
    return (
        UCFunctionToolkit(warehouse_id=wh_id)
        # Include functions as tools using their qualified names.
        # You can use "{catalog_name}.{schema_name}.*" to get all functions in a schema.
        .include(f"{catalog.replace('`', '')}.{schema}.*")
        .get_tools()
    )



#### PROMPTS

# Prompt Template for generation
full_prompt = ChatPromptTemplate.from_messages(
    [
        ("system", llm_config.get("llm_prompt_template")),
        # Note: This chain does not compress the history, so very long conversations can overflow the context window.
        MessagesPlaceholder(variable_name="formatted_chat_history"),
        # User's most current question
        ("user", "{question}"),
    ]
)

# Prompt Template for generation with tools
prompt_with_tools = ChatPromptTemplate.from_messages(
    [
        # ("system", llm_config.get("llm_prompt_template")),
        # ("placeholder", "{chat_history}"),
        # ("human", "{input}"),
        # ("placeholder", "{agent_scratchpad}"),
        ("system", llm_config.get("llm_prompt_template") + llm_config.get("tools_prompt_addition")),
        # Note: This chain does not compress the history, so very long conversations can overflow the context window.
        MessagesPlaceholder(variable_name="formatted_chat_history"),
        # User's most current question
        ("human", "{input}"),
        MessagesPlaceholder(variable_name="agent_scratchpad"),
    ]
)

# Prompt Template for query rewriting to allow conversation history to work - this will translate a query such as "how does it work?" after a question such as "what is spark?" to "how does spark work?".
query_rewrite_prompt = PromptTemplate(
    template=retriever_config.get("query_rewrite_template"),
    input_variables=retriever_config.get("query_rewrite_template_variables")
)

# Prompt Template for output rewriting - this will list tools required by the user to perform an action (example "replace brake pads" -> tools: - Wrenches - ...; steps: 1, 2, ...)
output_rewrite_prompt_sequential = PromptTemplate(
    template=llm_config.get("output_rewrite_template"),
    input_variables=llm_config.get("output_rewrite_template_variables"),
)



#### CHAIN

# Model for generation
model = ChatDatabricks(
    endpoint=databricks_resources.get("llm_endpoint_name"),
    extra_params=llm_config.get("llm_parameters"),
)

model_parser = model | StrOutputParser()

# Tools and Agent
tools = get_tools()
# model_with_tools = create_tool_calling_agent(model, tools, prompt_with_tools)
model_with_tools = create_react_agent(model, tools, prompt_with_tools) 
agent_executor = AgentExecutor(agent=model_with_tools, tools=tools, verbose=True)

def agent_executor_wrapper(input_data):
    # Wrapping the agent_executor invocation
    question = input_data['question']
    chat_history = input_data['formatted_chat_history']
    result = agent_executor.invoke({"input": question, "formatted_chat_history": chat_history})
    return result["output"]


# Chains
input_parser = RunnablePassthrough.assign(
    question=itemgetter("messages") | RunnableLambda(extract_question),
    chat_history=itemgetter("messages") | RunnableLambda(extract_chat_history),
    formatted_chat_history=itemgetter("messages") | RunnableLambda(format_chat_history_for_prompt),
)

vector_store_chain = RunnableBranch(
    ( #if there is a chat history, then re-write the question 
        lambda x: len(x["chat_history"]) > 0,
        query_rewrite_prompt | model_parser, 
    ), # else return the question as-is
    itemgetter("question"),
) | vector_search_as_retriever

prompt_inputs = (
    {
        "question": lambda x: x["question"],
        "chat_history": lambda x: x["chat_history"],
        "formatted_chat_history": lambda x: x["formatted_chat_history"],
        "context": lambda x: format_context(x["references"]),
    }
)

update_chat_history_passthrough = RunnablePassthrough.assign(chat_history=RunnableLambda(lambda x: update_chat_history(x))).assign(formatted_chat_history=itemgetter("chat_history") | RunnableLambda(format_chat_history_for_prompt))


# Additional question to make the LLM list out parts
parts_listing_question = RunnableLambda(lambda x: "What compatible parts or components are needed?")
tools_listing_question = RunnableLambda(lambda x: "What tools must be used?")

# Final outputs selection
final_outputs = (
    {
        "question": itemgetter("original_question"),
        "answer": itemgetter("answer"),
        "references": lambda x: combine_references(x),
    }
)

select_outputs = lambda x: dict((k, x[k]) for k in ["question", "answer", "references"])
dict_to_str = lambda x: json.dumps(x)

chain = (
    input_parser
    | RunnablePassthrough.assign(references=vector_store_chain).assign(answer=prompt_inputs | full_prompt | model_parser)
    | update_chat_history_passthrough.assign(original_question=itemgetter("question"), question=parts_listing_question) # update chat history and add new question for parts to the chain
    | RunnablePassthrough.assign(
        original_references=itemgetter("references"),
        original_answer=itemgetter("answer"),
        references=vector_store_chain
    ).assign(answer=prompt_inputs | full_prompt | model_parser)
    | update_chat_history_passthrough.assign(question=tools_listing_question) # update chat history and add new question for tools to the chain
    | RunnablePassthrough.assign(
        parts_references=itemgetter("references"),
        parts_answer=itemgetter("answer"),
        references=vector_store_chain
    ).assign(answer=prompt_inputs | full_prompt | model_parser)
    | update_chat_history_passthrough # update chat history before rewriting all the answers
    .assign(answer=output_rewrite_prompt_sequential | model_parser)
    .assign(
        question=itemgetter("original_question"),
        references=RunnableLambda(lambda x: combine_references(x)),
    )
    # | final_outputs
    | RunnableLambda(select_outputs)
    | RunnableLambda(dict_to_str)
    # | RunnableLambda(agent_executor_wrapper)  # Pass the query to the agent executor
    | RunnablePassthrough()
)



#### MLFLOW SETTINGS

# Enable the RAG Studio Review App to properly display retrieved chunks and evaluation suite to measure the retriever
mlflow.models.set_retriever_schema(
    primary_key=vector_search_schema.get("primary_key"),
    text_column=vector_search_schema.get("chunk_text"),
    doc_uri=vector_search_schema.get("document_uri")  # Review App uses `doc_uri` to display chunks from the same document in a single view
)

# Tell MLflow logging where to find your chain.
mlflow.models.set_model(model=chain)