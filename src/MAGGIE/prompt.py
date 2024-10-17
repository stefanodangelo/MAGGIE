from string import Formatter

extract_vars_from_format_str = lambda x: [fn for _, fn, _, _ in Formatter().parse(x) if fn is not None]

SYSTEM_MESSAGE_TEMPLATE = """
You are a trusted assistant that helps answer questions based only on the provided information. 
If you do not know the answer to a question, you truthfully say you do not know. 
Questions will be related to maintenance on trailers and trucks.
Any question not related to the topic must be ignored. Here is some context which might or might not help you answer: {context}. 
Answer directly, do not repeat the question, do not start with something like: the answer to the question, do not add AI in front of your answer, do not say: here is the answer, do not mention the context or the question.
"""

QUERY_REWRITE_TEMPLATE = """
Based on the chat history below, we want you to generate a query for an external data source to retrieve relevant documents so that we can better answer the question. 
The query should be in natural language. The external data source uses similarity search to search for relevant documents in a vector space. 
So the query should be similar to the relevant documents semantically. Answer with only the query. Do not add explanation.

Chat history: {chat_history}

Question: {question}
"""

CHUNK_TEMPLATE = "Passage: {chunk_text}\nSource: Page {page_nr} from {document_uri}\n"

REVIEW_APP_INSTRUCTIONS = f"""
### Instructions for Testing the Maintenance assistant

Your inputs are invaluable for the development team. By providing detailed feedback and corrections, you help us fix issues and improve the overall quality of the application. We rely on your expertise to identify any gaps or areas needing enhancement.

1. **Variety of Questions**:
- Please try a wide range of questions that you anticipate the end users of the application will ask. This helps us ensure the application can handle the expected queries effectively.

2. **Feedback on Answers**:
- After asking each question, use the feedback widgets provided to review the answer given by the application.
- If you think the answer is incorrect or could be improved, please use "Edit Answer" to correct it. Your corrections will enable our team to refine the application's accuracy.

3. **Review of Returned Documents**:
- Carefully review each document that the system returns in response to your question.
- Use the thumbs up/down feature to indicate whether the document was relevant to the question asked. A thumbs up signifies relevance, while a thumbs down indicates the document was not useful.

Thank you for your time and effort in testing our assistant. Your contributions are essential to delivering a high-quality product to our end users.
"""
