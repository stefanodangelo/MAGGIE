## MAGGIE (MAintenance Generative Guide for Industrial Efficiency)

TIP Group is pleased to introduce *MAGGIE*!

### What is MAGGIE?
**MAGGIE** (**MA**intenance **G**enerative **G**uide for **I**ndustrial **E**fficiency) is a cutting-edge digital assistant specifically designed to revolutionize the repair and maintenance workflows in the automotive industry, particularly for mechanics working in complex environments like TIP workshops. 
For companies like TIP, which service *thousands of trailers* across Europe, the inefficiencies in repair workflows lead to increased labor costs, longer repair times, and reduced customer satisfaction. MAGGIE addresses all these issues by providing mechanics with instant access to the exact tools, parts, and step-by-step instructions needed to complete repairs, installations and replacements. 
This allows businesses to: 
1. **Increase** throughput by servicing more vehicles in less time. 
2. **Reduce** the reliance on seasoned mechanics by providing apprentices with precise guidance. 
3. **Minimize** safety risks associated with incorrect repairs, ensuring compliance with industry standards. 

### How does MAGGIE work? 
MAGGIE combines indeed **generative AI** and **computer vision** to dynamically provide context-aware guidance, simplifying and accelerating manual searches through large documents and inventory management. 
To do so, it follows these steps:
1. It pre-processes maintenance manuals of trailer components in PDF format by chunking them into manageable and searchable units.
2. It enables accurate identification of parts needed for workshop activities by identifying QR codes in the documents with *OpenCV* and then scraping the links they reference to extract data for parts related to each component. 
3. It then combines all of the above with Databricks’ foundational model *DBRX Instruct* to deliver relevant content based on mechanics' queries and tailored to specific scenarios, manufacturers, and tasks. 

Furthermore, the custom UI makes the user experience much easier by allowing mechanics to interact with MAGGIE either through natural language queries or via pre-structured prompts, ensuring accessibility for all experience levels. 

MAGGIE’s architecture is built for **scalability** and **modularity**, ensuring that the solution can grow with the business and accommodate new features without requiring extensive rewrites. 
The design leverages: 
- **Databricks Autoloader** for continuous ingestion of new documents, allowing the system to handle increasing data volumes without the need to write new code. 
- **Delta Lake** and **Vector Search** for efficient querying, making it easy to retrieve relevant information even if the data volume scales. 
- **MLflow** and **Mosaic AI Agent Framework** for model scalability, deployment, and governance, allowing MAGGIE to ensure continuous quality. 
- **Databricks Model Serving** to allow the model to be invoked as a low-latency API on a highly available serverless service, guaranteeing high throughput at low costs. 

Each component is encapsulated within a *Databricks Asset Bundle*, making it easy to update and redeploy the system without significant downtime or codebase restructuring. This modular approach ensures that the system can be adapted to new assets, manufacturers, or repair procedures with minimal effort, driving down costs as the system scales.