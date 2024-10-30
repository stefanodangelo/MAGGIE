import os

from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from databricks.sdk.credentials_provider import oauth_service_principal

class DatabricksService:
    def __init__(self):
        self.client = WorkspaceClient(client_id=os.environ['DATABRICKS_CLIENT_ID'], client_secret=os.environ['DATABRICKS_CLIENT_SECRET'], host=os.environ['DATABRICKS_HOST'])
        self.warehouse = self.get_warehouse('Serverless Starter Warehouse')
        self.config = self.credential_provider()

    def get_warehouse(self, name=None):
        warehouses = self.client.warehouses.list()
        for wh in warehouses:
            if wh.name == name:
                return wh

    def get_qr_code_options(self):

        query = 'SELECT DISTINCT qr_code_id, part_code, part_name FROM dev_bronze.maggie_hackathon.part_lists'

        with sql.connect(server_hostname="adb-4176489020682060.0.azuredatabricks.net",
                         http_path="/sql/1.0/warehouses/36943660786efec7",
                         credentials_provider=self.credential_provider,
                         ) as connection:
            with connection.cursor() as cursor:
                # Execute the SQL statement
                cursor.execute(query)
                results = cursor.fetchall()
        return list(map(lambda x: {'label': x[2], 'value': x[0]}, results))
        # return [{'label': 'test', 'value': 1}]

    def get_partlist_options(self, qr_code_id: int):
        query = str("""SELECT DISTINCT item_number, designation
         FROM dev_bronze.maggie_hackathon.part_lists 
         WHERE qr_code_id = """ + str(qr_code_id))

        with sql.connect(server_hostname="adb-4176489020682060.0.azuredatabricks.net",
                         http_path="/sql/1.0/warehouses/36943660786efec7",
                         credentials_provider=self.credential_provider,
                         ) as connection:
            with connection.cursor() as cursor:
                # Execute the SQL statement
                cursor.execute(query)
                results = cursor.fetchall()
        return list(map(lambda x: {'label': x[1], 'value': x[1]}, results))
        # return [{'label': 'test', 'value': 1}, {'label': 'test2', 'value': 2}]

    def credential_provider(self):
        config = Config(
            host=os.getenv('DATABRICKS_HOST'),
            client_id=os.getenv("DATABRICKS_CLIENT_ID"),
            client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"))
        return oauth_service_principal(config)


