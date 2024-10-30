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

OUTPUT_REWRITE_WITH_HISTORY_TEMPLATE = """
Based on the chat history below, we want you to restructure the answer into a list of tools, a list of parts and a series of steps needed to accomplish the actions outlined in the answer.
Additionally, list out some safety advice related to the required steps and tools used.
List tools, parts, and steps that are only mentioned in the answer. 
If any step requires sub-steps, list them too. Do not add explanation. 
Include as many details as possible.
Answer directly, do not start with something like: here is the restructured answer.

Example:
    Chat history:
        User: How do I replace brake pads on a trailer?
        Assistant: 
            To replace brake pads on a trailer you need to first remove the wheel and the slider bolt with a torx wrench (T25, BPW no. 02.0130.44.10). 
            Next, pivot the caliper up and slide out the old brake pads. Then, slide in the new brake pads and retract the pistons. 
            To check if you did replaced them correctly, you can monitor the brake fluid level. Finally, reposition the caliper and reinstall the slider bolt.
        User: What parts or components are needed?
            You need new brake pads of the same type as those that you will replace. Optionally, you may need new brake fluid from a compatible manufacturer.
    Restructured Answer:
        Here is what you need to replace brake pads on a trailer. 
        
        Tools required:
            - torx wrench (T25, BPW no. 02.0130.44.10)
        
        Parts required:
            - new brake pads
            - (optional) new brake fluid
            
        Steps required:
            1. Remove the wheel.
                1.1 Use the jack to lift the trailer up.
            2. Remove the slider bolt.
            3. Pivot the caliper up.
            4. Slide out the old brake pads.
            5. Replace the retaining clips.
            6. Slide in the new brake pads.
            7. Retract the pistons.
            8. Monitor the brake fluid level.
            9. Reposition the caliper.
            10. Reinstall the slider bolt.
                10.1 Tighten the bolts a bit while the trailer is up. 
                10.2 Lower the trailer, then lower the jack and finally tighten the bolts completely.

        Safety advice:
            - Ensure the trailer is securely parked: Park the trailer on a flat, stable surface and engage the parking brake to prevent movement.
            - Wear protective gear: Use gloves, safety glasses, and sturdy work boots to protect your hands, eyes, and feet.
            - Use proper lifting equipment: Use a jack rated for the weight of the trailer and jack stands to support it safely before removing the wheel.
            - Avoid contact with hot components: Let the brakes cool down completely before starting the work to avoid burns from hot parts.
            - Work in a well-ventilated area: If replacing brake fluid, work in a well-ventilated area to avoid inhaling fumes.
            - Handle brake fluid carefully: Brake fluid is corrosive; avoid skin contact and clean spills immediately.
            - Check for correct tool usage: Use the correct size Torx wrench (T25) to avoid stripping the bolts.
            - Do not overfill the brake fluid reservoir: If adding brake fluid, ensure the level is correct to avoid pressure issues.
            - Check brake function before use: After reassembly, pump the brake pedal to ensure proper brake function and check for leaks or unusual noises before moving the trailer.

    Chat history: {chat_history}

    Restructured Answer:
"""

OUTPUT_REWRITE_WITH_CONTEXT_TEMPLATE = """
Your answer must contain a list of tools, a list of parts and a series of steps needed to accomplish the actions outlined in the provided context.
Additionally, list out some safety advice related to the required steps and tools used.
List tools, parts, and steps that are only mentioned in the context. 
If any step requires sub-steps, list them too. Do not add explanation. 
Include as many details as possible.

Example:
    Context:
        To replace brake pads on a trailer you need to first remove the wheel and the slider bolt with a torx wrench (T25, BPW no. 02.0130.44.10). 
        Next, pivot the caliper up and slide out the old brake pads. Then, slide in the new brake pads and retract the pistons. 
        To check if you did replaced them correctly, you can monitor the brake fluid level. Finally, reposition the caliper and reinstall the slider bolt.
    Question: How do I replace brake pads on a trailer?
    Restructured Answer:
        Here is what you need to replace brake pads on a trailer. 
        
        Tools required:
            - torx wrench (T25, BPW no. 02.0130.44.10)
        
        Parts required:
            - new brake pads
            - (optional) new brake fluid
            
        Steps required:
            1. Remove the wheel.
                1.1 Use the jack to lift the trailer up.
            2. Remove the slider bolt.
            3. Pivot the caliper up.
            4. Slide out the old brake pads.
            5. Replace the retaining clips.
            6. Slide in the new brake pads.
            7. Retract the pistons.
            8. Monitor the brake fluid level.
            9. Reposition the caliper.
            10. Reinstall the slider bolt.
                10.1 Tighten the bolts a bit while the trailer is up. 
                10.2 Lower the trailer, then lower the jack and finally tighten the bolts completely.

        Safety advice:
            - Ensure the trailer is securely parked: Park the trailer on a flat, stable surface and engage the parking brake to prevent movement.
            - Wear protective gear: Use gloves, safety glasses, and sturdy work boots to protect your hands, eyes, and feet.
            - Use proper lifting equipment: Use a jack rated for the weight of the trailer and jack stands to support it safely before removing the wheel.
            - Avoid contact with hot components: Let the brakes cool down completely before starting the work to avoid burns from hot parts.
            - Work in a well-ventilated area: If replacing brake fluid, work in a well-ventilated area to avoid inhaling fumes.
            - Handle brake fluid carefully: Brake fluid is corrosive; avoid skin contact and clean spills immediately.
            - Check for correct tool usage: Use the correct size Torx wrench (T25) to avoid stripping the bolts.
            - Do not overfill the brake fluid reservoir: If adding brake fluid, ensure the level is correct to avoid pressure issues.
            - Check brake function before use: After reassembly, pump the brake pedal to ensure proper brake function and check for leaks or unusual noises before moving the trailer.

Restructured Answer:
"""


TOOLS_SYSTEM_MESSAGE_ADDITION = """
You have access to the following tools:

{tools}

Use the following format:

Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: the final answer to the original input question

Begin!
"""

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
