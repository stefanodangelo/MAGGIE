from utils import spark, TABLE_PATH
from databricks.vector_search.client import VectorSearchClient
import time

class VectorStore:
    def __init__(self, index_full_name: str, endpoint_name: str, endpoint_type: str = "STANDARD"):
        self.vsc = VectorSearchClient()
        self.endpoint_name = endpoint_name
        self.endpoint_type = endpoint_type
        self.index_full_name = index_full_name

        if not self._endpoint_exists():
            self.vsc.create_endpoint(name=endpoint_name, endpoint_type=endpoint_type)
        self._wait_for_endpoint_to_be_ready()

    def _endpoint_exists(self):
        try:
            return self.endpoint_name in [e['name'] for e in self.vsc.list_endpoints().get('endpoints', [])]
        except Exception as e:
            #Temp fix for potential REQUEST_LIMIT_EXCEEDED issue
            if "REQUEST_LIMIT_EXCEEDED" in str(e):
                print("WARN: couldn't get endpoint status due to REQUEST_LIMIT_EXCEEDED error. The demo will consider it exists")
                return True
            else:
                raise e
        
    def _wait_for_endpoint_to_be_ready(self):
        for i in range(180):
            try:
                endpoint = self.vsc.get_endpoint(self.endpoint_name)
            except Exception as e:
                #Temp fix for potential REQUEST_LIMIT_EXCEEDED issue
                if "REQUEST_LIMIT_EXCEEDED" in str(e):
                    print("WARN: couldn't get endpoint status due to REQUEST_LIMIT_EXCEEDED error. Please manually check your endpoint status")
                    return
                else:
                    raise e
            status = endpoint.get("endpoint_status", endpoint.get("status"))["state"].upper()
            if "ONLINE" in status:
                return endpoint
            elif "PROVISIONING" in status or i <6:
                if i % 20 == 0: 
                    print(f"Waiting for endpoint to be ready, this can take a few min... {endpoint}")
                time.sleep(10)
            else:
                raise Exception(f'''Error with the endpoint {self.endpoint_name}. - this shouldn't happen: {endpoint}.\n Please delete it and run: vsc.delete_endpoint("{self.endpoint_name}")''')
        raise Exception(f"Timeout, your endpoint isn't ready yet: {self.vsc.get_endpoint(self.endpoint_name)}")

    def _index_exists(self):
        try:
            self.vsc.get_index(self.endpoint_name, self.index_full_name).describe()
            return True
        except Exception as e:
            if 'RESOURCE_DOES_NOT_EXIST' not in str(e):
                print(f'Unexpected error describing the index. This could be a permission issue.')
                raise e
        return False
        
    def _wait_for_index_to_be_ready(self):
        for i in range(180):
            idx = self.vsc.get_index(self.endpoint_name, self.index_full_name).describe()
            index_status = idx.get('status', idx.get('index_status', {}))
            status = index_status.get('detailed_state', index_status.get('status', 'UNKNOWN')).upper()
            url = index_status.get('index_url', index_status.get('url', 'UNKNOWN'))
            if "ONLINE" in status:
                return
            if "UNKNOWN" in status:
                print(f"Can't get the status - will assume index is ready {idx} - url: {url}")
                return
            elif "PROVISIONING" in status:
                if i % 40 == 0: 
                    print(f"Waiting for index to be ready, this can take a few min... {index_status} - pipeline url:{url}")
                time.sleep(10)
            else:
                raise Exception(f'''Error with the index - this shouldn't happen. DLT pipeline might have been killed.\n Please delete it and run: vsc.delete_index("{self.index_full_name}, {self.endpoint_name}") \nIndex details: {idx}''')
        raise Exception(f"Timeout, your index isn't ready yet: {self.vsc.get_index(self.index_full_name, self.endpoint_name)}")

    def create_index(self, primary_key: str, source_table_name: str, embedding_vector_column: str, embedding_dimension: int = 1024, pipeline_type: str = "TRIGGERED"):
        if not self._index_exists():
            print(f"Creating index {self.index_full_name} on endpoint {self.endpoint_name}...")
            try:
                self.vsc.create_delta_sync_index(
                    endpoint_name=self.endpoint_name,
                    index_full_name=self.index_full_name,
                    source_table_name=source_table_name, #The table we'd like to index
                    pipeline_type=pipeline_type, #Sync needs to be manually triggered
                    primary_key=primary_key,
                    embedding_dimension=embedding_dimension,
                    embedding_vector_column=embedding_vector_column
                )
            except Exception as e:
                if "already exists" in str(e):
                    pass
                else:
                    raise e
            #Let's wait for the index to be ready and all our embeddings to be created and indexed
            self._wait_for_index_to_be_ready()
        else:
            #Trigger a sync to update our vs content with the new data saved in the table
            self._wait_for_index_to_be_ready()
            try:
                self.csv.get_index(self.endpoint_name, self.index_full_name).sync()
            except Exception as e:
                print(e)